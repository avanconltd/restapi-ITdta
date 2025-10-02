def connect_to_cluster(site_name, cnxn):
    """
    Determine which ADX cluster to connect to based on site environment
    Returns cluster URL and database name
    """
    sql = "select IsProductionENV from plant_site where AutomationTableName = '" + \
        str(site_name)+"'"
    temp_df = pd.read_sql(sql, cnxn)
    isprodenv = temp_df['IsProductionENV'][0]   
    ADX_DATABASE = site_name 
    
    if isprodenv == 1:
        cluster = os.getenv("ADX_CLUSTER_URL_PROD")
        logging.info(f"Connecting to PRODUCTION ADX cluster for site: {site_name}")
    else:
        cluster = os.getenv("ADX_CLUSTER_URL_STAG")
        logging.info(f"Connecting to STAGING ADX cluster for site: {site_name}")
    
    if not cluster:
        env_var = "ADX_CLUSTER_URL_PROD" if isprodenv == 1 else "ADX_CLUSTER_URL_STAG"
        raise ValueError(f"Environment variable {env_var} is not set")
    
    return cluster, ADX_DATABASE

class ITDataProcessor:
    """Main IT data processing pipeline"""
    
    def __init__(self):
        self.blob_extractor = BlobDataExtractor(BLOB_CONNECTION_STRING, BLOB_CONTAINER_NAME)
        self.mapping_manager = MappingManager(SQL_CONNECTION_STRING)
        # Note: ADX manager will be initialized dynamically based on site
        self.adx_manager = None
    
    def initialize_adx_manager(self, cluster_url: str, database: str):
        """Initialize ADX manager with specific cluster and database"""
        self.adx_manager = ADXManager(
            cluster_url=cluster_url,
            database=database,
            client_id=ADX_CLIENT_ID,
            client_secret=ADX_CLIENT_SECRET,
            tenant_id=ADX_TENANT_ID
        )
    
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

            # Step 4: Connect to appropriate ADX cluster
            logging.info(f"Step 4: Connecting to ADX cluster for site: {site_name}")
            try:
                cnxn = pyodbc.connect(SQL_CONNECTION_STRING)
                cluster_url, database = connect_to_cluster(site_name, cnxn)
                cnxn.close()
                
                # Initialize ADX manager with the correct cluster and database
                self.initialize_adx_manager(cluster_url, database)
                
            except Exception as e:
                return {
                    'status': 'failed',
                    'error': f'Failed to connect to ADX cluster: {str(e)}',
                    'records_processed': len(mapped_data)
                }
            
            # Step 5: Create/verify ADX table
            logging.info(f"Step 5: Creating/verifying ADX table")
            table_name = mapping_config['table_name']
            
            if not self.adx_manager.create_table_if_not_exists(table_name):
                return {
                    'status': 'failed',
                    'error': f'Failed to create/verify ADX table: {table_name}',
                    'records_processed': len(mapped_data)
                }
            
            # Step 6: Ingest data to ADX
            logging.info(f"Step 6: Ingesting data to ADX table: {table_name}")
            if not self.adx_manager.ingest_data(table_name, mapped_data):
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
                'source_blob': source_blob,
                'cluster_url': cluster_url,
                'database': database
            }
            
        except Exception as e:
            logging.error(f"IT data pipeline error: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e),
                'records_processed': 0
            }