import azure.functions as func
import json
import logging
import os
import io
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd
import pyodbc

# Azure imports
from azure.storage.blob import BlobServiceClient
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError

# Fixed Kusto ingest imports
try:
    from azure.kusto.ingest import QueuedIngestClient, IngestionProperties
    from azure.kusto.ingest import DataFormat
except ImportError:
    try:
        # Try alternative import path for newer versions
        from azure.kusto.ingest import QueuedIngestClient, IngestionProperties
        from azure.kusto.ingest.ingestion_properties import DataFormat
    except ImportError:
        # Fallback for different versions
        from azure.kusto.ingest import QueuedIngestClient, IngestionProperties
        # Define DataFormat locally if not available
        class DataFormat:
            JSON = "json"
            CSV = "csv"
            TSV = "tsv"
            PARQUET = "parquet"

from jsonpath_ng import parse

# Initialize Function App
app = func.FunctionApp()

# Configuration - Use environment variables in production
BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=llmreadapi;AccountKey=w27JkvsZvacC6/BE7q1CDwoZgi93sMMR+Sac+ALPnKrTI7Naj89CUkuRreEhcNaLjaRxQMpvm5YV+AStVO+Lmw==;EndpointSuffix=core.windows.net")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME", "readapicontainer")

# ADX Configuration
ADX_CLUSTER_URL = os.getenv("ADX_CLUSTER_URL", "https://your-adx-cluster.region.kusto.windows.net")
ADX_DATABASE = os.getenv("ADX_DATABASE", "ITDataDatabase")
ADX_CLIENT_ID = os.getenv("ADX_CLIENT_ID", "your-client-id")
ADX_CLIENT_SECRET = os.getenv("ADX_CLIENT_SECRET", "your-client-secret")
ADX_TENANT_ID = os.getenv("ADX_TENANT_ID", "your-tenant-id")

# SQL Configuration for mapping
SQL_CONNECTION_STRING = os.getenv("SQL_CONNECTION_STRING", "Driver={ODBC Driver 17 for SQL Server};Server=your-server;Database=your-db;UID=your-user;PWD=your-password")

class BlobDataExtractor:
    """Handles blob-to-blob data extraction using JSONPath"""
    
    def __init__(self, connection_string: str, container_name: str):
        self.connection_string = connection_string
        self.container_name = container_name
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_client = self.blob_service_client.get_container_client(container_name)
    
    def extract_data(self, source_blob: str, json_path_expr: str, destination_blob: str = "result.json") -> List[Dict]:
        """
        Extract data from source blob using JSONPath and optionally save to destination blob
        
        Args:
            source_blob: Name of source blob file
            json_path_expr: JSONPath expression to extract data
            destination_blob: Name of destination blob file (optional)
            
        Returns:
            List of dictionaries containing extracted data
        """
        try:
            logging.info(f"Starting data extraction from blob: {source_blob}")
            
            # Step 1: Download source JSON
            source_blob_client = self.container_client.get_blob_client(source_blob)
            if not source_blob_client.exists():
                raise FileNotFoundError(f"Source blob {source_blob} does not exist")
            
            blob_data = source_blob_client.download_blob().readall()
            json_data = json.loads(blob_data)
            logging.info(f"Downloaded JSON data from {source_blob} (size: {len(blob_data)} bytes)")
            
            # Step 2: Extract JSON using JSONPath
            jsonpath_expr = parse(json_path_expr)
            matches = [match.value for match in jsonpath_expr.find(json_data)]
            
            if not matches:
                logging.warning(f"No matches found for JSONPath: {json_path_expr}")
                return []
            
            # Flatten if single match contains a list
            extracted_data = []
            for match in matches:
                if isinstance(match, list):
                    extracted_data.extend(match)
                else:
                    extracted_data.append(match)
            
            logging.info(f"Extracted {len(extracted_data)} records using JSONPath: {json_path_expr}")
            
            # Step 3: Upload extracted JSON to destination blob (optional)
            if destination_blob:
                destination_blob_client = self.container_client.get_blob_client(destination_blob)
                destination_blob_client.upload_blob(
                    json.dumps(extracted_data, indent=4, default=str), 
                    overwrite=True
                )
                logging.info(f"Extracted data saved to {destination_blob}")
            
            return extracted_data
            
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in source blob {source_blob}: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Error extracting data from blob {source_blob}: {str(e)}")
            raise

class ADXManager:
    """Manages Azure Data Explorer operations"""
    
    def __init__(self, cluster_url: str, database: str, client_id: str, client_secret: str, tenant_id: str):
        self.cluster_url = cluster_url
        self.database = database
        
        # Initialize ADX clients
        try:
            kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                cluster_url, client_id, client_secret, tenant_id
            )
            self.kusto_client = KustoClient(kcsb)
            self.ingest_client = QueuedIngestClient(kcsb)
            logging.info("ADX clients initialized successfully")
        except Exception as e:
            logging.error(f"Failed to initialize ADX clients: {str(e)}")
            raise
    
    def create_table_if_not_exists(self, table_name: str, schema_mapping: Dict[str, str]) -> bool:
        """Create ADX table if it doesn't exist based on schema mapping"""
        try:
            # Check if table exists
            check_query = f".show tables | where TableName == '{table_name}'"
            result = self.kusto_client.execute(self.database, check_query)
            
            if len(list(result.primary_results[0])) == 0:
                # Table doesn't exist, create it
                columns = []
                
                # Add mapped columns
                for field_name, field_type in schema_mapping.items():
                    adx_type = self._convert_to_adx_type(field_type)
                    columns.append(f"[{field_name}]: {adx_type}")
                
                # Add standard audit columns
                columns.extend([
                    "[ingestion_time]: datetime",
                    "[site_name]: string",
                    "[connector_id]: int",
                    "[source_blob]: string"
                ])
                
                create_command = f".create table ['{table_name}'] ({', '.join(columns)})"
                logging.info(f"Creating ADX table with command: {create_command}")
                
                self.kusto_client.execute(self.database, create_command)
                logging.info(f"Successfully created table: {table_name}")
                return True
            else:
                logging.info(f"Table {table_name} already exists")
                return True
                
        except Exception as e:
            logging.error(f"Error creating ADX table {table_name}: {str(e)}")
            return False
    
    def _convert_to_adx_type(self, sql_type: str) -> str:
        """Convert SQL Server types to ADX types"""
        type_mapping = {
            'varchar': 'string',
            'nvarchar': 'string',
            'char': 'string',
            'nchar': 'string',
            'text': 'string',
            'ntext': 'string',
            'int': 'int',
            'bigint': 'long',
            'smallint': 'int',
            'tinyint': 'int',
            'bit': 'bool',
            'decimal': 'decimal',
            'numeric': 'decimal',
            'float': 'real',
            'real': 'real',
            'datetime': 'datetime',
            'datetime2': 'datetime',
            'date': 'datetime',
            'time': 'timespan',
            'uniqueidentifier': 'string',
            'json': 'dynamic'
        }
        
        # Extract base type (remove size specifications)
        base_type = sql_type.lower().split('(')[0].strip()
        return type_mapping.get(base_type, 'string')
    
    def ingest_data(self, table_name: str, data: List[Dict], site_name: str, connector_id: int, source_blob: str) -> bool:
        """Ingest data into ADX table using JSON format"""
        try:
            if not data:
                logging.warning("No data to ingest")
                return True
            
            # Add audit fields to each record
            enriched_data = []
            ingestion_time = datetime.utcnow()
            
            for record in data:
                enriched_record = record.copy()
                enriched_record['ingestion_time'] = ingestion_time.isoformat()
                enriched_record['site_name'] = site_name
                enriched_record['connector_id'] = connector_id
                enriched_record['source_blob'] = source_blob
                enriched_data.append(enriched_record)


                # Log enriched data information
            logging.info(f"Total rows in enriched_data: {len(enriched_data)}")

            if enriched_data:
                    first_100 = enriched_data[:100]
                    logging.info(f"First 100 rows of enriched_data: {json.dumps(first_100, default=str, indent=2)}")
            else:
                    logging.info("No enriched data to log")
            
   
            ingestion_props = IngestionProperties(
                database=self.database,
                table=table_name,
                data_format=DataFormat.JSON,
            )
            
            # Ingest data using the stream
            # Convert to DataFrame for ingestion
            df = pd.DataFrame(enriched_data)
            logging.info(f"Created DataFrame with shape: {df.shape}")
            logging.info(f"DataFrame columns: {list(df.columns)}")
            if not df.empty:
                logging.info(f"DataFrame dtypes: {df.dtypes.to_dict()}")
                logging.info(f"Sample DataFrame rows (first 5): {df.head().to_dict('records')}")



            self.ingest_client.ingest_from_dataframe(
                df, 
                ingestion_properties=ingestion_props
            )
            
            logging.info(f"Successfully queued {len(enriched_data)} records for ingestion to {table_name}")
            return True
            
        except Exception as e:
            logging.error(f"Error ingesting data to ADX: {str(e)}")
            logging.error(f"ADX Database: {self.database}, Table: {table_name}")
            return False

class MappingManager:
    """Manages field mappings and schema definitions from SQL database"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
    
    def get_mapping_config(self, connector_id: int, site_name: str) -> Optional[Dict[str, Any]]:
        """Get mapping configuration from SQL database"""
        try:
            with pyodbc.connect(self.connection_string) as conn:
                cursor = conn.cursor()
                
                # Query to get mapping configuration
                query = """
                SELECT 
                    table_name,
                    field_mappings,
                    schema_definition,
                    created_date,
                    updated_date
                FROM pipeline_mappings 
                WHERE connector_id = ? AND site_name = ? AND is_active = 1
                """
                
                cursor.execute(query, (connector_id, site_name))
                result = cursor.fetchone()
                
                if result:
                    return {
                        'table_name': result.table_name,
                        'field_mappings': json.loads(result.field_mappings) if result.field_mappings else {},
                        'schema_definition': json.loads(result.schema_definition) if result.schema_definition else {},
                        'created_date': result.created_date,
                        'updated_date': result.updated_date
                    }
                else:
                    logging.warning(f"No mapping found for connector_id: {connector_id}, site_name: {site_name}")
                    return None
                    
        except Exception as e:
            logging.error(f"Error getting mapping configuration: {str(e)}")
            return None
    
    def apply_field_mapping(self, data: List[Dict], field_mappings: Dict[str, str]) -> List[Dict]:
        """Apply field mapping transformations to data"""
        if not field_mappings:
            logging.info("No field mappings to apply")
            return data
        
        mapped_data = []
        for record in data:
            mapped_record = {}
            
            # Apply mappings
            for source_field, target_field in field_mappings.items():
                value = self._get_field_value(record, source_field)
                if value is not None:
                    mapped_record[target_field] = value
            
            # Include unmapped fields if they don't conflict with mapped fields
            mapped_field_sources = set(field_mappings.keys())
            mapped_field_targets = set(field_mappings.values())
            
            for key, value in record.items():
                if key not in mapped_field_sources and key not in mapped_field_targets:
                    mapped_record[key] = value
            
            mapped_data.append(mapped_record)
        
        logging.info(f"Applied field mappings to {len(mapped_data)} records")
        return mapped_data
    
    def _get_field_value(self, data: Dict, field_path: str):
        """Get field value supporting nested access with dot notation"""
        try:
            if '.' not in field_path:
                return data.get(field_path)
            
            keys = field_path.split('.')
            value = data
            for key in keys:
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    return None
            return value
        except:
            return None

class ITDataProcessor:
    """Main IT data processing pipeline"""
    
    def __init__(self):
        self.blob_extractor = BlobDataExtractor(BLOB_CONNECTION_STRING, BLOB_CONTAINER_NAME)
        self.adx_manager = ADXManager(ADX_CLUSTER_URL, ADX_DATABASE, ADX_CLIENT_ID, ADX_CLIENT_SECRET, ADX_TENANT_ID)
        self.mapping_manager = MappingManager(SQL_CONNECTION_STRING)
    
    def process_pipeline(self, source_blob: str, json_path: str, site_name: str, connector_id: int) -> Dict[str, Any]:
        """Execute complete IT data processing pipeline"""
        try:
            logging.info(f"Starting IT data pipeline - connector_id: {connector_id}, site_name: {site_name}")
            
            # Step 1: Extract data from blob
            logging.info(f"Step 1: Extracting data from blob: {source_blob}")
            extracted_data = self.blob_extractor.extract_data(source_blob, json_path)
            
            if not extracted_data:
                return {
                    'status': 'failed',
                    'error': 'No data extracted from source blob',
                    'records_processed': 0
                }
            
            logging.info(f"Extracted {len(extracted_data)} records from blob")
            
            # Step 2: Get mapping configuration
            logging.info(f"Step 2: Getting mapping configuration")
            mapping_config = self.mapping_manager.get_mapping_config(connector_id, site_name)
            
            if not mapping_config:
                return {
                    'status': 'failed',
                    'error': f'No mapping configuration found for connector_id: {connector_id}, site_name: {site_name}',
                    'records_processed': len(extracted_data)
                }
            
            # Step 3: Apply field mappings
            logging.info(f"Step 3: Applying field mappings")
            mapped_data = self.mapping_manager.apply_field_mapping(
                extracted_data, 
                mapping_config.get('field_mappings', {})
            )
            
            # Step 4: Create/verify ADX table
            logging.info(f"Step 4: Creating/verifying ADX table")
            table_name = mapping_config['table_name']
            schema_definition = mapping_config.get('schema_definition', {})
            
            if not self.adx_manager.create_table_if_not_exists(table_name, schema_definition):
                return {
                    'status': 'failed',
                    'error': f'Failed to create/verify ADX table: {table_name}',
                    'records_processed': len(mapped_data)
                }
            
            # Step 5: Ingest data to ADX
            logging.info(f"Step 5: Ingesting data to ADX table: {table_name}")
            if not self.adx_manager.ingest_data(table_name, mapped_data, site_name, connector_id, source_blob):
                return {
                    'status': 'failed',
                    'error': 'Failed to ingest data to ADX',
                    'records_processed': len(mapped_data)
                }
            
            return {
                'status': 'success',
                'records_processed': len(mapped_data),
                'table_name': table_name,
                'site_name': site_name,
                'connector_id': connector_id,
                'source_blob': source_blob
            }
            
        except Exception as e:
            logging.error(f"IT data pipeline error: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e),
                'records_processed': 0
            }

# Legacy function for backward compatibility
def blob_to_blob_extracted(source_blob: str, json_path_expr: str) -> List[Dict]:
    """
    Legacy function for backward compatibility with existing code
    """
    try:
        extractor = BlobDataExtractor(BLOB_CONNECTION_STRING, BLOB_CONTAINER_NAME)
        return extractor.extract_data(source_blob, json_path_expr)
    except Exception as e:
        logging.error(f"Error in blob_to_blob_extracted: {str(e)}")
        return []

# # Queue trigger function for IT data processing
@app.queue_trigger(arg_name="azqueue", queue_name="queuerestapi", connection="QueueConnectionString")
def process_it_data_queue(azqueue: func.QueueMessage) -> None:
    """Queue trigger for IT data processing and ADX ingestion"""
    logging.info('IT Data processing queue function triggered.')
    
    try:
        # Parse queue message
        try:
            message_body = azqueue.get_body().decode('utf-8')
            message_data = json.loads(message_body)
            
            # Extract required parameters
            source_blob = message_data.get('source_blob')
            json_path = message_data.get('json_path')
            site_name = message_data.get('site_name')
            connector_id = message_data.get('connector_id')
            
            # Validate required parameters
            if not all([source_blob, json_path, site_name, connector_id]):
                missing_params = []
                if not source_blob: missing_params.append('source_blob')
                if not json_path: missing_params.append('json_path')
                if not site_name: missing_params.append('site_name')
                if not connector_id: missing_params.append('connector_id')
                
                raise ValueError(f"Missing required parameters: {', '.join(missing_params)}")
            
            logging.info(f'Processing IT data - connector_id: {connector_id}, site_name: {site_name}, source_blob: {source_blob}, json_path: {json_path}')
            
        except (json.JSONDecodeError, ValueError) as e:
            error_msg = f'Invalid queue message format: {str(e)}'
            logging.error(error_msg)
            raise ValueError(error_msg)
        
        # Initialize and run IT data processor
        processor = ITDataProcessor()
        result = processor.process_pipeline(source_blob, json_path, site_name, connector_id)
        
        if result['status'] == 'success':
            logging.info(f"IT data processing completed successfully:")
            logging.info(f"  - Records processed: {result['records_processed']}")
            logging.info(f"  - Table: {result['table_name']}")
            logging.info(f"  - Site: {result['site_name']}")
            logging.info(f"  - Connector ID: {result['connector_id']}")
            logging.info(f"  - Source Blob: {result['source_blob']}")
        else:
            error_msg = f"IT data processing failed: {result['error']}"
            logging.error(error_msg)
            logging.error(f"Records processed before failure: {result['records_processed']}")
            # Re-raise to trigger poison queue handling
            raise Exception(error_msg)
            
    except Exception as e:
        logging.error(f"Queue processing error: {str(e)}")
        # Re-raise exception to trigger poison queue handling if configured
        raise

# ...existing code...