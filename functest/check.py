import azure.functions as func
import json
import logging
from azure.storage.blob import BlobServiceClient
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties, DataFormat
import pyodbc
from typing import List, Dict, Any
from jsonpath_ng import parse
import pandas as pd
from datetime import datetime

# ...existing code...

# ADX Configuration
ADX_CLUSTER_URL = "https://your-adx-cluster.region.kusto.windows.net"
ADX_DATABASE = "YourDatabase"
ADX_CLIENT_ID = "your-client-id"
ADX_CLIENT_SECRET = "your-client-secret"
ADX_TENANT_ID = "your-tenant-id"

# SQL Configuration for mapping
SQL_CONNECTION_STRING = "Driver={ODBC Driver 17 for SQL Server};Server=your-server;Database=your-db;UID=your-user;PWD=your-password"

class ADXManager:
    def __init__(self):
        # Initialize ADX clients
        kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
            ADX_CLUSTER_URL, ADX_CLIENT_ID, ADX_CLIENT_SECRET, ADX_TENANT_ID
        )
        self.kusto_client = KustoClient(kcsb)
        self.ingest_client = QueuedIngestClient(kcsb)
    
    def create_table_if_not_exists(self, table_name: str, schema_mapping: Dict[str, str]) -> bool:
        """Create ADX table if it doesn't exist based on schema mapping"""
        try:
            # Check if table exists
            check_query = f".show tables | where TableName == '{table_name}'"
            result = self.kusto_client.execute(ADX_DATABASE, check_query)
            
            if len(list(result.primary_results[0])) == 0:
                # Table doesn't exist, create it
                columns = []
                for field_name, field_type in schema_mapping.items():
                    # Convert SQL types to ADX types
                    adx_type = self._convert_to_adx_type(field_type)
                    columns.append(f"{field_name}: {adx_type}")
                
                # Add standard audit columns
                columns.extend([
                    "ingestion_time: datetime",
                    "site_name: string",
                    "connector_id: int"
                ])
                
                create_command = f".create table {table_name} ({', '.join(columns)})"
                logging.info(f"Creating ADX table: {create_command}")
                
                self.kusto_client.execute(ADX_DATABASE, create_command)
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
            'uniqueidentifier': 'string'
        }
        
        # Extract base type (remove size specifications)
        base_type = sql_type.lower().split('(')[0]
        return type_mapping.get(base_type, 'string')
    
    def ingest_data(self, table_name: str, data: List[Dict], site_name: str, connector_id: int) -> bool:
        """Ingest data into ADX table"""
        try:
            if not data:
                logging.warning("No data to ingest")
                return True
            
            # Add audit fields to each record
            enriched_data = []
            ingestion_time = datetime.utcnow()
            
            for record in data:
                enriched_record = record.copy()
                enriched_record['ingestion_time'] = ingestion_time
                enriched_record['site_name'] = site_name
                enriched_record['connector_id'] = connector_id
                enriched_data.append(enriched_record)
            
            # Convert to DataFrame for easier handling
            df = pd.DataFrame(enriched_data)
            
            # Convert DataFrame to JSON string for ingestion
            json_data = df.to_json(orient='records', date_format='iso')
            
            # Set ingestion properties
            ingestion_props = IngestionProperties(
                database=ADX_DATABASE,
                table=table_name,
                data_format=DataFormat.JSON
            )
            
            # Ingest data
            self.ingest_client.ingest_from_stream(
                json_data,
                ingestion_properties=ingestion_props
            )
            
            logging.info(f"Successfully queued {len(enriched_data)} records for ingestion to {table_name}")
            return True
            
        except Exception as e:
            logging.error(f"Error ingesting data to ADX: {str(e)}")
            return False

class MappingManager:
    def __init__(self):
        self.connection_string = SQL_CONNECTION_STRING
    
    def get_mapping_config(self, connector_id: int, site_name: str) -> Dict[str, Any]:
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
            return data
        
        mapped_data = []
        for record in data:
            mapped_record = {}
            for source_field, target_field in field_mappings.items():
                if source_field in record:
                    mapped_record[target_field] = record[source_field]
                else:
                    # Handle nested field access using dot notation
                    value = self._get_nested_value(record, source_field)
                    if value is not None:
                        mapped_record[target_field] = value
            
            # Include unmapped fields if they don't conflict
            for key, value in record.items():
                if key not in field_mappings and key not in mapped_record:
                    mapped_record[key] = value
            
            mapped_data.append(mapped_record)
        
        return mapped_data
    
    def _get_nested_value(self, data: Dict, field_path: str):
        """Get nested value using dot notation (e.g., 'user.profile.name')"""
        try:
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

def process_it_data_pipeline(source_blob: str, json_path: str, site_name: str, connector_id: int) -> Dict[str, Any]:
    """Complete IT data processing pipeline"""
    try:
        # Step 1: Extract data from blob using existing function
        logging.info(f"Extracting data from blob: {source_blob} using path: {json_path}")
        extracted_data = blob_to_blob_extracted(source_blob, json_path)
        
        if not extracted_data:
            return {
                'status': 'failed',
                'error': 'No data extracted from source blob'
            }
        
        # Step 2: Get mapping configuration
        mapping_manager = MappingManager()
        mapping_config = mapping_manager.get_mapping_config(connector_id, site_name)
        
        if not mapping_config:
            return {
                'status': 'failed',
                'error': f'No mapping configuration found for connector_id: {connector_id}, site_name: {site_name}'
            }
        
        # Step 3: Apply field mappings
        mapped_data = mapping_manager.apply_field_mapping(
            extracted_data, 
            mapping_config.get('field_mappings', {})
        )
        
        # Step 4: Initialize ADX manager and create table if needed
        adx_manager = ADXManager()
        table_name = mapping_config['table_name']
        schema_definition = mapping_config.get('schema_definition', {})
        
        if not adx_manager.create_table_if_not_exists(table_name, schema_definition):
            return {
                'status': 'failed',
                'error': f'Failed to create/verify ADX table: {table_name}'
            }
        
        # Step 5: Ingest data to ADX
        if not adx_manager.ingest_data(table_name, mapped_data, site_name, connector_id):
            return {
                'status': 'failed',
                'error': 'Failed to ingest data to ADX'
            }
        
        return {
            'status': 'success',
            'records_processed': len(mapped_data),
            'table_name': table_name,
            'site_name': site_name,
            'connector_id': connector_id
        }
        
    except Exception as e:
        logging.error(f"IT data pipeline error: {str(e)}")
        return {
            'status': 'failed',
            'error': str(e)
        }

@app.queue_trigger(arg_name="azqueue", queue_name="itdataqueue",
                   connection="itdataqueue_STORAGE") 
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
                raise ValueError("Missing required parameters: source_blob, json_path, site_name, connector_id")
            
            logging.info(f'Processing IT data - connector_id: {connector_id}, site_name: {site_name}, source_blob: {source_blob}, json_path: {json_path}')
            
        except (json.JSONDecodeError, ValueError) as e:
            error_msg = f'Invalid queue message format: {str(e)}'
            logging.error(error_msg)
            raise ValueError(error_msg)
        
        # Process the IT data pipeline
        result = process_it_data_pipeline(source_blob, json_path, site_name, connector_id)
        
        if result['status'] == 'success':
            logging.info(f"IT data processing completed successfully:")
            logging.info(f"  - Records processed: {result['records_processed']}")
            logging.info(f"  - Table: {result['table_name']}")
            logging.info(f"  - Site: {result['site_name']}")
            logging.info(f"  - Connector ID: {result['connector_id']}")
        else:
            error_msg = f"IT data processing failed: {result['error']}"
            logging.error(error_msg)
            # Re-raise to trigger poison queue handling
            raise Exception(error_msg)
            
    except Exception as e:
        logging.error(f"Queue processing error: {str(e)}")
        # Re-raise exception to trigger poison queue handling if configured
        raise

# ...existing code...