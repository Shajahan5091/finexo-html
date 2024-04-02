from flask import Flask, render_template, request, jsonify
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import streamlit_connection as st

def Migration_report(connection,database,schema):
    try:
        print('inside migration report')
        connection_cursor=connection.cursor()
        connection_cursor.execute(f"create  stage if not exists {database}.{schema}.Migration_Report;")
        connection_cursor.execute("put file://D:\Stream_lit_code_frame\environment.yml  @{database}.{schema}.Migration_Report/REPORT_FLD AUTO_COMPRESS=FALSE".format(database=database,schema=schema))
        connection_cursor.execute("put file://D:\Stream_lit_code_frame\streamlit.py  @{database}.{schema}.Migration_Report/REPORT_FLD AUTO_COMPRESS=FALSE".format(database=database,schema=schema))
        connection_cursor.execute("create or replace  STREAMLIT {database}.{schema}.Migration_Report ROOT_LOCATION='@{database}.{schema}.Migration_Report/REPORT_FLD' MAIN_FILE = '/streamlit.py', QUERY_WAREHOUSE =  SNOW_MIGRATE_WAREHOUSE ;".format(database=database,schema=schema))
    except Exception as error:
        print(error)



app = Flask(__name__)

@app.route('/')
# First app route that will render the index.html page
def index():
    return render_template('index.html')

@app.route('/biq', methods=['POST'])
# This app route will render the file_upload.html page after the bigquery option selected from services
def biq():
    return render_template('file_upload.html')

@app.route('/upload', methods=['POST'])
# This app route will render the schemas_copy.html after the submitting the json file
def upload():
    
    # Check if the POST request has the file part
    if 'file' not in request.files:
        return 'No file part'
    
    file = request.files['file']
    global bucket_name
    bucket_name = request.form.get("bucket")
    # If the user does not select a file, the browser submits an empty file without a filename
    if file.filename == '':
        return 'No selected file'

    if file and file.filename.endswith('.json'):
        # Read JSON file and establish connection to BigQuery
        json_content = file.read()
        credentials = service_account.Credentials.from_service_account_info(json.loads(json_content))
        global project_id
        project_id = credentials.project_id

        if not project_id:
            return 'Invalid JSON format. Please provide project ID.'

        try:
            # Establish connection to BigQuery using the provided credentials
            global bq_client
            bq_client = bigquery.Client(credentials=credentials, project=project_id)

            # Fetch schemas from BigQuery and display them
            schemas = fetch_schemas(bq_client)
            
            return render_template('schemas_copy.html', schemas=schemas)
        except Exception as e:
            return f'Error establishing connection to BigQuery: {str(e)}'

    else:
        return 'Invalid file format'
    
def fetch_schemas(client):
    datasets = list(client.list_datasets())
    schemas = []
    for dataset in datasets:
        schemas.append(dataset.dataset_id)
    return schemas

@app.route('/get_tables',  methods =["GET", "POST"])
def get_tables():
    Client= bq_client
    schema_name = request.form.get("connect-btn")
    # Fetch tables for the given schema
    tables = fetch_schemas_tables(Client, schema_name)
    print(tables)
    # return render_template("schemas.html", tables = tables, schema = schema_name,show_popup=True)
    return render_template('schemas_copy.html', show_popup=True,tables = tables, schema = schema_name)


#Function to return the tables in a schema
def fetch_schemas_tables(client,schema_name):
    print(schema_name)
    datasets = list(client.list_datasets())
    table_list = []
    schema_list = []
    Tables = []
    for dataset in datasets:
        schema_list.append(dataset.reference)
        table_list = list(client.list_tables(dataset.reference))
        for table in table_list:
            if(str(dataset.dataset_id) == str(schema_name)):
                table_ref = dataset.reference.table(table.table_id)
                print(table_ref.table_id)
                Tables.append(table_ref.table_id)
    return Tables

@app.route('/snowflake_form', methods = ["GET", "POST"])
def snowflake_form():
    global schema_list
    schema_list = request.form.getlist("schema-name")
    print(schema_list)
    return render_template("snowflake_form_copy.html")

@app.route('/connect_snowflake', methods = ["GET", "POST"])
def connect_snowflake():
    # schemas_list = schema_list
    # print(schemas_list)
    global account_name, role, username, password, warehouse, database, schema
    account_name = request.form.get("accountname")
    print(account_name)
    role = request.form.get("role")
    print(role)
    username = request.form.get("username")
    print(username)
    password = request.form.get("password")
    print(password)
    warehouse = request.form.get("warehouse")
    print(warehouse)
    database = request.form.get("database")
    print(database)
    schema = request.form.get("schema")
    print(schema)
    global conn
    conn = snowflake.connector.connect(
        user= username,
        password= password,
        account= account_name,
        warehouse= warehouse,
        role = role,
        database = database,
        schema = schema
    )
    self_execute(conn, bucket_name , schema , database )
    # result = create_schemas_and_copy_table(conn,schemas_list)
    return render_template('test_snowflake.html')


def self_execute(conn, bucket_name , schema , database ):
    query_use_role = """USE role {role};"""
    query_self_integration = """ 
      CREATE OR REPLACE STORAGE INTEGRATION SNOW_MIGRATE_INTEGRATION
      TYPE = EXTERNAL_STAGE
      STORAGE_PROVIDER = 'GCS'
      ENABLED = TRUE
      STORAGE_ALLOWED_LOCATIONS = ('gcs://{bkt}');"""

    
    query_use_db = """USE DATABASE {db};"""
    query_create_schema = """CREATE SCHEMA IF NOT EXISTS {schema};"""
    
    query_create_ff = """CREATE FILE FORMAT IF NOT EXISTS 
      {db}.{schema}.my_parquet_format TYPE = PARQUET NULL_IF = ('NULL', 'null');"""
    
    
    query_create_stage = """ CREATE OR Replace STAGE {db}.{schema}.SNOW_MIGRATE_STAGE
      URL='gcs://{bkt}'
      STORAGE_INTEGRATION = SNOW_MIGRATE_INTEGRATION
      FILE_FORMAT = {db}.{schema}.my_parquet_format; """ 
    
    print(query_use_role.format(role = role))
    print(query_use_db.format(db = database))
    print(query_create_schema.format(schema = schema))
    print(query_self_integration.format(bkt = bucket_name))
    print(query_create_ff.format(db = database , schema = schema))
    print(query_create_stage.format(db = database,schema = schema ,bkt = bucket_name))

    # conn.cursor().execute(query_self_integration.format(bkt = bucket_name))
    # conn.cursor().execute(query_use_db.format(db = database))
    # conn.cursor().execute(query_create_schema.format(schema = schema))
    # conn.cursor().execute(query_create_ff.format(db = database , schema = schema))
    # conn.cursor().execute(query_create_stage.format(bkt = bucket_name))

    try :
        # conn.cursor().execute(query_self_updated)
        conn.cursor().execute(query_use_role.format(role = role))
        conn.cursor().execute(query_use_db.format(db = database))
        conn.cursor().execute(query_create_schema.format(schema = schema))
        conn.cursor().execute(query_self_integration.format(bkt = bucket_name))
        conn.cursor().execute(query_create_ff.format(db = database , schema = schema))
        conn.cursor().execute(query_create_stage.format(db = database,schema = schema ,bkt = bucket_name))
    
        # print(query_self_updated);
        print('Execution was succesfull');

    except snowflake.connector.errors.ProgrammingError as e:
        print('SQL Execu tion Error: {0}'.format(e.msg))
        print('Snowflake Query Id: {0}'.format(e.sfqid))
        print('Error Number: {0}'.format(e.errno))
        print('SQL State: {0}'.format(e.sqlstate))

# Endpoint to test GCP service account connection
@app.route('/test_connection', methods=['POST'])
def test_connection():
    # Replace this with your actual testing logic
    print("Received request to test service account connection")
    cursor = conn.cursor()
    if isinstance(cursor, str):
        # If cursor is a string, it means an error occurred during Snowflake connection
        return jsonify({"success": False, "error": cursor})
    else:
        # Connection successful, perform your logic here
        try:
            cursor.execute('SELECT 5+5')
            row = cursor.fetchone()
            if row[0] == 10:
                success = True
                print(row[0])
            # Simulate successful connection
            return jsonify({"success": True})
        except Exception as e:
            error_message = str(e).split(":")[-1].strip()
            return jsonify({"success": False, "error": error_message})
        

# Endpoint to check if required roles are granted
@app.route('/GrantAccessCheck', methods=['POST'])
def grant_access_check():
    cursor = conn.cursor()
    if isinstance(cursor, str):
        # If cursor is a string, it means an error occurred during Snowflake connection
        return jsonify({"success": False, "error": cursor})
    else:
        try:
            # Replace this with your actual logic to check access for Warehouse, Database, Role, and User
            warehouse_access = check_warehouse_access(cursor)
            database_access = check_database_access(cursor)
            role_access = check_role_access(cursor)
            user_access = check_user_access(cursor)

            if warehouse_access and database_access and role_access and user_access:
                return jsonify({"success": True})
            else:
                error_message = ""
                if not warehouse_access:
                    error_message += "Warehouse access failed. "
                if not database_access:
                    error_message += "Database access failed. "
                if not role_access:
                    error_message += "Role access failed. "
                if not user_access:
                    error_message += "User access failed. "
                return jsonify({"success": False, "error": error_message.strip()})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)})

# Simulated functions to check access for Warehouse, Database, Role, and User
def check_warehouse_access(cursor):
    # Replace with your actual logic
    warehouse_check = "USE WAREHOUSE {}".format(warehouse)
    cursor.execute(warehouse_check)

    if(cursor.fetchone()):
             return True
    else:
        return False

def check_database_access(cursor):
    # Replace with your actual logic
    database_check = "USE DATABASE {}".format(database)
    cursor.execute(database_check)
    if(cursor.fetchone()):
             return True
    else:
        return False

def check_role_access(cursor):
    # Replace with your actual logic
    role_check = "USE ROLE {}".format(role)
    cursor.execute(role_check)
    if(cursor.fetchone()):
             return True
    else:
        return False

def check_user_access(cursor):
    # Replace with your actual logic
    user_check = "SHOW USERS LIKE '{}'".format(username)
    cursor.execute(user_check)
    if(cursor.fetchone()):
             return True
    else:
        return False


# Endpoint to check if creating table and schema is allowed
@app.route('/CheckCreatePermissions', methods=['POST'])
def check_create_permissions():
    cursor = conn.cursor()
    if isinstance(cursor, str):
        # If cursor is a string, it means an error occurred during Snowflake connection
        return jsonify({"success": False, "error": cursor})
    else:
        try:
            # Replace this with your actual logic to check permissions
            schema_creation_allowed = check_schema_creation_permission(cursor)
            table_creation_allowed = check_table_creation_permission(cursor)

            if table_creation_allowed and schema_creation_allowed:
                return jsonify({"success": True})
            else:
                error_message = ""
                if not table_creation_allowed:
                    error_message += "Table creation not allowed. "
                if not schema_creation_allowed:
                    error_message += "Schema creation not allowed. "
                return jsonify({"success": False, "error": error_message.strip()})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)})

# Function to check if creating table is allowed
def check_table_creation_permission(cursor):
    try:
        database_check = "USE DATABASE {}".format(database)
        cursor.execute(database_check)
        cursor.execute("USE SCHEMA test_schema")
        cursor.execute("CREATE TABLE IF NOT EXISTS test_table (id INT)")
        cursor.execute("DROP TABLE IF EXISTS test_table")
        cursor.execute("DROP SCHEMA IF EXISTS test_schema")
        return True
    except Exception as e:
        return False

# Function to check if creating schema is allowed
def check_schema_creation_permission(cursor):
    try:
        database_check = "USE DATABASE {}".format(database)
        cursor.execute(database_check)
        cursor.execute("CREATE SCHEMA IF NOT EXISTS test_schema")
        return True
    except Exception as e:
        return False


# Endpoint to check Storage integration, file fromat, stage exist
@app.route('/IntegrationObjectExistence', methods=['POST'])
def Integration_Object_Exist():
    cursor = conn.cursor()
    if isinstance(cursor, str):
        # If cursor is a string, it means an error occurred during Snowflake connection
        return jsonify({"success": False, "error": cursor})
    else:
        try:
            storage_integration_exists = check_object_exists(cursor, 'INTEGRATIONS', 'SNOW_MIGRATE_INTEGRATION')
            print(f"Storage Integration Exists: {storage_integration_exists}")
        
            if storage_integration_exists:
                    return jsonify({"success": True})
            else:
                error_message = ""
                if not storage_integration_exists:
                    error_message += "Storage Integration Object not Exist. "
                return jsonify({"success": False, "error": error_message.strip()})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)})
        

# Endpoint to check Storage integration, file fromat, stage exist
@app.route('/FileFormatObjectExistence', methods=['POST'])
def FileFormat_Object_Exist():
    cursor = conn.cursor()
    if isinstance(cursor, str):
        # If cursor is a string, it means an error occurred during Snowflake connection
        return jsonify({"success": False, "error": cursor})
    else:
        try:
            file_format_exists = check_object_exists(cursor, 'FILE FORMATS', 'my_parquet_format')
            print(f"File Format Exists: {file_format_exists}")

            if file_format_exists:
                    return jsonify({"success": True})
            else:
                error_message = ""
                if not file_format_exists:
                    error_message += "File Format Object not Exist. "
                return jsonify({"success": False, "error": error_message.strip()})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)})
        

# Endpoint to check Storage integration, file fromat, stage exist
@app.route('/StageObjectExistence', methods=['POST'])
def Stage_Object_Exist():
    cursor = conn.cursor()
    if isinstance(cursor, str):
        # If cursor is a string, it means an error occurred during Snowflake connection
        return jsonify({"success": False, "error": cursor})
    else:
        try:
            stage_exists = check_object_exists(cursor, 'STAGES', 'SNOW_MIGRATE_STAGE')
            print(f"Stage Exists: {stage_exists}")

            if stage_exists:
                    return jsonify({"success": True})
            else:
                error_message = ""
                if not stage_exists:
                    error_message += "Stage Object not Exist. "
                return jsonify({"success": False, "error": error_message.strip()})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)})
        
    
# Function to check if an object exists
def check_object_exists(cursor, object_type, object_name):
    try:
            query = f"SHOW {object_type} LIKE '{object_name}'"
            cursor.execute(query)
            if(len(cursor.fetchall()) > 0):
                print(len(cursor.fetchall()))
                return True
            else:
                return False
    except Exception as e:
            return False
    


# Endpoint to check access for Storage integration, file fromat, stage exist
@app.route('/IntegrationAccess', methods=['POST'])
def IntegrationAccess():
    cursor = conn.cursor()
    if isinstance(cursor, str):
        # If cursor is a string, it means an error occurred during Snowflake connection
        return jsonify({"success": False, "error": cursor})
    else:
        try:
             # Check grants for integration
            integration_name = 'SNOW_MIGRATE_INTEGRATION'
            role_name = role
            privilege = 'OWNERSHIP'
            granted_on = 'INTEGRATION'
            integration_grants = check_grants(cursor, privilege, granted_on, integration_name, role_name)
            print(f"Grants for integration '{integration_name}': {integration_grants}")

            if integration_grants:
                    return jsonify({"success": True})
            else:
                error_message = ""
                if not integration_grants:
                    error_message += "Storage Integration Object not Accessible. "
                return jsonify({"success": False, "error": error_message.strip()})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)})
        

# Endpoint to check access for Storage integration, file fromat, stage exist
@app.route('/FormatAccess', methods=['POST'])
def FormatAccess():
    cursor = conn.cursor()
    if isinstance(cursor, str):
        # If cursor is a string, it means an error occurred during Snowflake connection
        return jsonify({"success": False, "error": cursor})
    else:
        try:
            # Check grants for file format
            file_format_name = "{}.{}.MY_PARQUET_FORMAT".format(database, schema)
            role_name = role
            privilege = 'OWNERSHIP'
            granted_on = 'FILE FORMAT'
            file_format_grants = check_grants(cursor, privilege, granted_on, file_format_name, role_name)
            print(f"Grants for file format '{file_format_name}': {file_format_grants}")
            if file_format_grants:
                    return jsonify({"success": True})
            else:
                error_message = ""
                if not file_format_grants:
                    error_message += "File Format Object not Accessible. "
                return jsonify({"success": False, "error": error_message.strip()})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)})
        

# Endpoint to check access for Storage integration, file fromat, stage exist
@app.route('/StageAccess', methods=['POST'])
def StageAccess():
    cursor = conn.cursor()
    if isinstance(cursor, str):
        # If cursor is a string, it means an error occurred during Snowflake connection
        return jsonify({"success": False, "error": cursor})
    else:
        try:
            # Check grants for stage
            stage_name = "{}.{}.SNOW_MIGRATE_STAGE".format(database,schema)
            role_name = role
            privilege = 'OWNERSHIP'
            granted_on = 'STAGE'
            stage_grants = check_grants(cursor, privilege, granted_on, stage_name, role_name)
        
            print(f"Grants for stage '{stage_name}': {stage_grants}")

            if stage_grants:
                    return jsonify({"success": True})
            else:
                error_message = ""
                if not stage_grants:
                    error_message += "Stage Object not Accessible. "
                return jsonify({"success": False, "error": error_message.strip()})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)})

    
# Function to check if access is granted to the specified object
def check_grants(cursor, privilege, granted_on, object_name, role_name):      
    try:
        query = f"SHOW GRANTS ON {granted_on} {object_name}"
        cursor.execute(query)
        results = cursor.fetchall()
        if(granted_on == 'FILE FORMAT'):
            granted_on='FILE_FORMAT'
        for row in results:
            if row[1] == privilege and row[2] == granted_on and row[3] == object_name and row[5] == role_name:
                return True
        return False
    except Exception as e:
            return False
    
@app.route('/migration_result', methods=['POST'])
def migration_result():
    streamlit(database,schema)
    result = create_schemas_and_copy_table(conn,schema_list)
    return result

def create_schemas_and_copy_table(conn,schema_list):
   query = """
   select schema_name from `{}`.INFORMATION_SCHEMA.SCHEMATA
   """
   project_query = query.format(project_id)
   query_job = bq_client.query(project_query)
   rows = query_job.result()
   Columns = ['TABLE_CATALOG','TABLE_SCHEMA','TABLE_NAME','TABLE_COLUMNS','EXPORT_TYPE','COPY_DONE']
   copy_table = pd.DataFrame(columns = Columns)
   for row in rows:
       schema_local = row.schema_name
       print(schema_local)
       print("777777777777")
       print(schema_list)
       if schema_local in schema_list :
           print("Gathering ddl {}".format(schema_local))
           query = """
            select  c.table_catalog, c.table_schema, c.table_name,  string_agg('$1:'||c.column_name) as table_columns , case when t.ddl like 
            '%STRUCT%' or ddl like '%ARRAY%' then 'parquet' else 'parquet' end as export_type, 'N' as copy_done FROM 
            `{}`.{}.INFORMATION_SCHEMA.TABLES as t join
            `{}`.{}.INFORMATION_SCHEMA.COLUMNS as c on c.table_name = t.table_name group by c.table_catalog, 
            c.table_name,c.table_schema,t.ddl;
           """
    
           print("Gathering ddl for tables in schema {}".format(schema_local))
           table_query = """
           SELECT table_name,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace
           (replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(ddl,'`',''),'INT64','INT'),'FLOAT64','FLOAT'),
           'BOOL','BOOLEAN'),'STRUCT','VARIANT'),'PARTITION BY','CLUSTER BY ('),';',');'),'CREATE TABLE ','CREATE TABLE if not exists '), "table INT,", 
           '"table" INT,'),'_"table" INT,','_table INT,'),'ARRAY<STRING>','ARRAY'),'from','"from"'),'_"from"','_from'),'"from"_','from_'),
           'DATE(_PARTITIONTIME)','date(loaded_at)'),' OPTIONS(',', //'),'));',');'),'_at);','_at));'),'start ','"start" '),'_"start"','_start'),
           'order ','"order" '),'<',', //'),'_"order"','_order') as ddl
           FROM `{}`.{}.INFORMATION_SCHEMA.TABLES where table_type='BASE TABLE'
           """
           
           # FOR SCHEMAS
           ddl_query = query.format(project_id,schema_local,project_id,schema_local)
           query_job = bq_client.query(ddl_query)
           ddl_set = query_job.result()
           
           for row in ddl_set:
            df = pd.DataFrame(data=[list(row.values())],columns = Columns) 
            copy_table = pd.concat([copy_table,df] , ignore_index=True)
            table_ddl = " create or replace TABLE {}.{}.BQ_COPY_TABLE ( TABLE_CATALOG VARCHAR(16777216), TABLE_SCHEMA VARCHAR(16777216), TABLE_NAME VARCHAR(16777216),TABLE_COLUMNS VARCHAR(16777216), EXPORT_TYPE VARCHAR(16777216), COPY_DONE VARCHAR) ".format(database, schema)

            conn.cursor().execute(table_ddl)
            write_pandas(conn, copy_table , 'BQ_COPY_TABLE', database, schema )
            print("BQ_COPY_TABLE created succesfully")
            
            schema_name = row.table_schema
            create_schema = "create schema if not exists {}.{}".format(database, schema_name)
            conn.cursor().execute(create_schema)

            print("Schema {} created in {} Database".format(schema_name, database))
    
           #FOR TABLES
           ddl_table_query = table_query.format(project_id,schema_local)
           query_table_job = bq_client.query(ddl_table_query)
           ddl_table_set = query_table_job.result()
    
           for my_row in ddl_table_set:
               table_name = my_row.table_name
               ddl = my_row.ddl
               ddl2 = ddl.replace(project_id, database)
               print(ddl2)
               print("Running ddl for table {} in Snowflake".format(table_name))
               use_schema = "use schema {}.{}".format(database, schema)
               conn.cursor().execute(use_schema)
               conn.cursor().execute(ddl2)
               print("Table {} created in {}.{} schema".format(table_name, database, schema))

    # FOR EXPORTING DATA
           export_query = """
           select table_name,case when ddl like '%STRUCT%' or ddl like '%ARRAY%' then 'parquet' else 'parquet' end as export_type
           FROM `{}`.{}.INFORMATION_SCHEMA.TABLES where table_type='BASE TABLE'
           """
           ddl_query = export_query.format(project_id,schema_local)
           query_job = bq_client.query(ddl_query)
           ddl_export_set = query_job.result()

           for row in ddl_export_set:
               table_name = row.table_name
               export_type = row.export_type
               print("Exporting data for table {} ...export type is {}".format(table_name,export_type))
               destination_uri = "gs://{}/{}/{}/{}-*.{}".format(bucket_name,schema_local,table_name,table_name,export_type)
               print(destination_uri)
               dataset_ref = bigquery.DatasetReference(project_id, schema_local)
               table_ref = dataset_ref.table(table_name)
               configuration = bigquery.job.ExtractJobConfig()
               configuration.destination_format ='PARQUET'
               if export_type == 'parquet':
                   extract_job = bq_client.extract_table(
                       table_ref,
                       destination_uri,
                       job_config=configuration,
                       location="US"
                       )
               else:
                   extract_job = bq_client.extract_table(
                       table_ref,
                       destination_uri,
                       location="US"
                       )
               extract_job.result()  # Waits for job to complete.
               print("Exported successfully.. {}:{}.{} to {}".format(project_id, schema_local, table_name, destination_uri)) 

           # LOAD DATA
           SF_query = "select table_name,table_schema,table_columns,export_type from BQ_COPY_TABLE where copy_done ='N'"
           
           cur = conn.cursor()
           cur.execute(SF_query)
           result = cur.fetchall()
           column_info = cur.description
           column_names = [info[0] for info in column_info]
           df2 = pd.DataFrame(result , columns=column_names)
           counter = 0
           i=0
           for i in range(0,len(df2)):
                table_name = df2['TABLE_NAME'].iloc[i];
                table_schema  = df2['TABLE_SCHEMA'].iloc[i];
                table_columns = df2['TABLE_COLUMNS'].iloc[i];
                export_type  = df2['EXPORT_TYPE'].iloc[i];
                copy_command = "copy into {db}.{sc}.{tb} from ( select {col_list} from @{db}.{sch}.snow_migrate_stage/{sc}/{tb}/{tb}(file_format => my_parquet_format))"
                print(table_name + export_type)

                copy_command = copy_command.replace('{db}', database,2)
                copy_command = copy_command.replace('{sc}', table_schema,2)
                copy_command = copy_command.replace('{sch}', schema)
                copy_command = copy_command.replace('{tb}', table_name,3)
                copy_command = copy_command.replace('{col_list}', table_columns )

                conn.cursor().execute(copy_command)
                counter+=1
                i+=1
                print(counter)
                print("{} Data Loaded succesfully with {}".format(table_name,copy_command))
       else :
            print("D`one")
   auditing_log_into_Snowflake(conn,project_id,schema_list)
   
   Migration_report(conn,database,schema)   
   return "Success"




def auditing_log_into_Snowflake(snowflake_connection_config,project_name,schema_names):
    print(schema_names)
    if len(schema_names)<2:
        schema_name_single=schema_names[0]
        # print(schema_name)
        query_TABLE_DETAILS = (f"""select table_catalog,table_schema,table_name,total_rows from `{project_name}`.`region-US`.INFORMATION_SCHEMA.TABLE_STORAGE where table_type='BASE TABLE' and deleted=false and  table_schema in ('{schema_name_single}');""")
        print(query_TABLE_DETAILS)
    else:
        schema_name_tuple=tuple(schema_names)
        query_TABLE_DETAILS = (f"""select table_catalog,table_schema,table_name,total_rows from `{project_name}`.`region-US`.INFORMATION_SCHEMA.TABLE_STORAGE where table_type='BASE TABLE' and deleted=false and  table_schema in {schema_name_tuple};""")
        print(query_TABLE_DETAILS)
    query_job = bq_client.query(query_TABLE_DETAILS)
    results_schema_database_lst = query_job.result()
    print(results_schema_database_lst)
    schema_list_name = [field.name for field in results_schema_database_lst.schema]
    print(schema_list_name)
    # Create DataFrame with both column names and data---------------------------------------------------------------------------------------------
    dataframe_schema_table_info = pd.DataFrame(data=[list(row.values()) for row in results_schema_database_lst], columns=schema_list_name)
    print(dataframe_schema_table_info)
    if len(schema_names)<2:
        query_ddl =(f"""select table_catalog,table_schema,table_name,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace
        (replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(ddl,'`',''),'INT64','INT'),'FLOAT64','FLOAT'),
        'BOOL','BOOLEAN'),'STRUCT','VARIANT'),'PARTITION BY','CLUSTER BY ('),';',');'),'CREATE TABLE ','CREATE TABLE if not exists '), "table INT,",
        '"table" INT,'),'_"table" INT,','_table INT,'),'ARRAY<STRING>','ARRAY'),'from','"from"'),'_"from"','_from'),'"from"_','from_'),
        'DATE(_PARTITIONTIME)','date(loaded_at)'),' OPTIONS(',', //'),'));',');'),'_at);','_at));'),'start ','"start" '),'_"start"','_start'),
        'order ','"order" '),'<',', //'),'_"order"','_order') as ddl from `{project_name}`.`region-US`.INFORMATION_SCHEMA.TABLES where  table_schema ='{schema_name_single}' """)
    else :
        query_ddl =(f"""select table_catalog,table_schema,table_name,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace
        (replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(ddl,'`',''),'INT64','INT'),'FLOAT64','FLOAT'),
        'BOOL','BOOLEAN'),'STRUCT','VARIANT'),'PARTITION BY','CLUSTER BY ('),';',');'),'CREATE TABLE ','CREATE TABLE if not exists '), "table INT,",
        '"table" INT,'),'_"table" INT,','_table INT,'),'ARRAY<STRING>','ARRAY'),'from','"from"'),'_"from"','_from'),'"from"_','from_'),
        'DATE(_PARTITIONTIME)','date(loaded_at)'),' OPTIONS(',', //'),'));',');'),'_at);','_at));'),'start ','"start" '),'_"start"','_start'),
        'order ','"order" '),'<',', //'),'_"order"','_order') as ddl from `{project_name}`.`region-US`.INFORMATION_SCHEMA.TABLES where  table_schema  in {schema_name_tuple} """)
    print(query_ddl)
    query_job = bq_client.query(query_ddl)
    results_ddl_St_db = query_job.result()
    schema_list_name_2 = [field.name for field in results_ddl_St_db.schema]
    dataframe_ddl_table_info = pd.DataFrame(data=[list(row.values()) for row in results_ddl_St_db ], columns=schema_list_name_2)
    print("1 frame")
    print(dataframe_ddl_table_info)
    
    if len(schema_names)<2:
        query_copy_dol=(f"""select  c.table_catalog, c.table_schema , c.table_name,  string_agg('$1:'||c.column_name) as table_columns  FROM
            `{project_name}`.`region-US`.INFORMATION_SCHEMA.TABLES as t join
            `{project_name}`.`region-US`.INFORMATION_SCHEMA.COLUMNS as c on c.table_name = t.table_name where c.table_schema ='{schema_name_single}' group by c.table_catalog,
            c.table_name,c.table_schema,t.ddl;""")
    else:
        query_copy_dol=(f"""select  c.table_catalog, c.table_schema , c.table_name,  string_agg('$1:'||c.column_name) as table_columns  FROM
            `{project_name}`.`region-US`.INFORMATION_SCHEMA.TABLES as t join
            `{project_name}`.`region-US`.INFORMATION_SCHEMA.COLUMNS as c on c.table_name = t.table_name where c.table_schema  in {schema_name_tuple} group by c.table_catalog,
            c.table_name,c.table_schema,t.ddl;""")
    
    query_job = bq_client.query(query_copy_dol)
    results_copy_dol = query_job.result()
    schema_3 = [field.name for field in results_copy_dol.schema]
    # ----------------------------------------Create DataFrame with both column names and data-----------------------------------------------------------
    dataframe_copy_dol = pd.DataFrame(data=[list(row.values()) for row in results_copy_dol], columns=schema_3)
    print(dataframe_copy_dol)

    result_ddl_ed_table = pd.merge(dataframe_schema_table_info, dataframe_ddl_table_info, how="outer", on=["table_catalog","table_schema","table_name"])
    result_ddl_ed_table = pd.merge(result_ddl_ed_table,dataframe_copy_dol, how="outer", on=["table_catalog","table_schema","table_name"])
    write_pandas(snowflake_connection_config,result_ddl_ed_table,'META_TABLES_STRUCT_SOURCE',database=database,schema=schema, auto_create_table=True,overwrite=True,table_type="transient")
    print(result_ddl_ed_table)
    
    if len(schema_names)<2:
        query = (f"""select table_catalog,table_schema,table_name,column_name,ordinal_position,is_nullable,data_type from `{project_name}`.`region-US`.INFORMATION_SCHEMA.COLUMNS where table_schema = '{schema_name_single}' ;""")
    else:
        query = (f"""select table_catalog,table_schema,table_name,column_name,ordinal_position,is_nullable,data_type from `{project_name}`.`region-US`.INFORMATION_SCHEMA.COLUMNS where table_schema in {schema_name_tuple} ;""")
    
    query_job = bq_client.query(query)
    results_column_lst= query_job.result()
    schema_4 = [field.name for field in results_column_lst.schema]
    dataframe_column_info = pd.DataFrame(data=[list(row.values()) for row in results_column_lst], columns=schema_4)
    write_pandas(snowflake_connection_config,dataframe_column_info,'META_COLUMNS_STRUCT_SOURCE',database=database,schema=schema, auto_create_table=True,overwrite=True,table_type="transient")
 
def streamlit(database,schema):
    stream_script=("""
import streamlit as st
import pandas 
import matplotlib.pyplot as plt
import numpy as np
import matplotlib

from snowflake.snowpark.context import get_active_session


st.set_page_config(layout="wide")
st.header("Migration Report")
st.button(":arrows_counterclockwise:", type="primary")
st.divider()
tab1,tab3 = st.tabs(["Table Loaded","Table Structure"])
session = get_active_session()
time=session.sql('select getdate();').collect()
def Table_report():
    session.write_pandas(df_table_Source,f'Migration_Report_Schema_/time/',database="{database}",schema="{schema}", auto_create_table=True,overwrite=True,table_type="transient")

with tab1:
    col1,col2,col3=st.columns([1,2,1])
    with col1:
        def Schema_report():
            session.write_pandas(df_schema,f'Migration_Report_Schema_/time/',database="{database}",schema="{schema}", auto_create_table=True,overwrite=True,table_type="transient")
        st.write("List of Schema Avaiable",)
        st.button(":inbox_tray:",help="download Schema Report",on_click=Schema_report)
        dataframe=session.sql('select distinct("table_schema") from {database}.{schema}.META_TABLES_STRUCT_SOURCE')
        df_schema=dataframe.to_pandas()
        df_schema.insert(1,"Schema Available On Target",'❌')
        list_schema_target=session.sql("select SCHEMA_NAME from {database}.INFORMATION_SCHEMA.SCHEMATA;").collect()
        count_Schema=len(df_schema)
        count_load_Schema =0
        count_unloade_schema=0
        for i in range (len(df_schema)):
            schema_source=df_schema.iloc[i][0]
            upper_schema_source=schema_source.upper()
            for len_schema_target in range (len(list_schema_target)):
                if(list_schema_target[len_schema_target]['SCHEMA_NAME'] == upper_schema_source):
                    df_schema.iloc[i,1]='✅' 
                    count_load_Schema+=1
                    break
        st.dataframe(df_schema)
       
        count_unloade_schema=count_Schema-count_load_Schema
        if(count_Schema==count_load_Schema):
            st.success('All schema have been successfully created.', icon="✅")
        else:
            st.warning('Some schema are missing. Please verify them.', icon="❌")

        st.caption(f"Total Number of Schema: /count_Schema/")
        y = np.array([count_load_Schema,count_unloade_schema])
        mylabels = [ count_load_Schema,count_unloade_schema]
        lables_info = ["No of successful create schema", "No of unsuccessful create schema " ]
        myexplode = [0, 0.3]
        mycolors = ["#ACEC6B", "#FF7777"]
        plt.pie(y, labels = mylabels, explode = myexplode,startangle = 90,colors = mycolors)
        plt.legend(title = "Schema Status:",labels=lables_info )
        a=plt.show() 
        st.set_option('deprecation.showPyplotGlobalUse', False)
        st.pyplot(a)
        
      
            
    with col2:
        st.write("List of Table Avaiable")
        dataframe_table_source=session.sql('select distinct "table_schema"as "Table Schema On Source","table_name" as "Table On Source" ,case when "total_rows" is null then 0 else "total_rows" end as "Table Row Count Source" from {database}.{schema}.META_TABLES_STRUCT_SOURCE;')
        st.button(":inbox_tray:",help="download Table Report",on_click=Table_report)
        df_table_Source=dataframe_table_source.to_pandas()
        dataframe_table_target=session.sql("select TABLE_SCHEMA,TABLE_NAME,ROW_COUNT from {database}.INFORMATION_SCHEMA.TABLES where Table_schema !='INFORMATION_SCHEMA';") 
        df_table_Target=dataframe_table_target.to_pandas()
        df_table_Source.insert(3,"Table Available On Target",'❌')
        df_table_Source.insert(4,"Load Row Count On Target",0)
        df_table_Source.insert(5,"Successfully Load On Table",'❌')
        count_table_source=len(df_table_Source)
        count_table_target=0
        count_table_row_Target=0
        
        for len_table in range(count_table_source):
            table_source=df_table_Source.iloc[len_table][1]
            upper_table_source=table_source.upper()
            for len_table_target in range(len(df_table_Target)):
                if(df_table_Target.iloc[len_table_target]["TABLE_NAME"]==upper_table_source):
                    df_table_Source.iloc[len_table,3]='✅' 
                    count_table_target +=1
                    if((df_table_Target.iloc[len_table_target]["ROW_COUNT"])==(df_table_Source.iloc[len_table]["Table Row Count Source"])):
                         df_table_Source.iloc[len_table,4]=df_table_Target.iloc[len_table_target]["ROW_COUNT"]
                         df_table_Source.iloc[len_table,5]='✅'
                         count_table_row_Target +=1
                    else:
                        df_table_Source.iloc[len_table,4]=df_table_Target.iloc[len_table_target]["ROW_COUNT"]
        st.dataframe(df_table_Source)
        if(count_table_source==count_table_target):
            st.success('All table are create successfully In Snowflake.', icon="✅")
        else:
            st.warning('Some table are missing Please verify them.', icon="❌")
        if(count_table_source==count_table_row_Target):
            st.success('All data has been successfully loaded into the table', icon="✅")
        else:
            st.warning('Some data did not load into the table. Please verify them.', icon="❌")
        unloaded_table_count=count_table_source-count_table_target
        
    with col3:
        st.subheader("Table Loaded Report:")
        st.caption(f"Total Number of Tables In Big Query : /count_table_source/")
        y = np.array([count_table_target,unloaded_table_count])
        mylabels = [count_table_target,unloaded_table_count]
        lables_info = ["No of table Loaded ", "No of table unloaded " ]
        myexplode = [0, 0.3]
        mycolors = ["#ACEC6B", "#FF7777"]
        plt.pie(y, labels = mylabels, explode = myexplode,startangle = 90,colors = mycolors)
        plt.legend(title = "Table Loaded Status:",labels=lables_info )
        a=plt.show() 
        st.set_option('deprecation.showPyplotGlobalUse', False)
        st.pyplot(a)
    

   
with tab3:
    col1,col2,col3=st.columns([1,1,0.5])
    def Table_Struct():
        session.write_pandas(source_table,f'Migration_Report_Table_Struct_/time/',database="{database}",schema="{schema}", auto_create_table=True,overwrite=True,table_type="transient")        

    with col1:
        schema_list=session.sql('select distinct "table_schema" from {database}.{schema}.META_TABLES_STRUCT_SOURCE;').collect()
        schema_name=st.selectbox('Bigquery Schema List',schema_list,help='select Schema need to view',placeholder="Select schema .....",)
    with col2:
        
        Table_SQL=('''select distinct "table_name" from {database}.{schema}.META_TABLES_STRUCT_SOURCE where "table_schema"='/schema_name/';''').format(schema_name=schema_name)
        Table_List =session.sql(Table_SQL).collect()
        table_name=st.selectbox('Biquery Table List',Table_List,help='select Table need to view',placeholder="Select table .....",)
        st.button(":inbox_tray:",help="download Tabel Struct Report",on_click=Table_Struct)
    source_table_sql=('''select "column_name" as  "Column Available On Source","data_type"as  "Data type On Source"  from {database}.{schema}.META_COLUMNS_STRUCT_SOURCE where "table_schema"='/schema_name/' and "table_name"='/table_name/' ;''').format(schema_name=schema_name,table_name=table_name)
    source_table=session.sql(source_table_sql)
    source_table=source_table.to_pandas()
    source_table.insert(2,"Column Available On Target",'❌')
    source_table.insert(3,"Column Type On Target",'❌')
    source_table.insert(4,"Column Type Available On Target","INT")
    # source_table.insert(4,"Load Row Count On Target",0)
    len_column=len(source_table)
    Len_column_count_check=0
    

    for len_column_count in range (len_column):
        column_name=source_table.iloc[len_column_count]["Column Available On Source"]
        column_name=column_name.upper()
        sql_target_column=("select column_name,data_type from {database}.INFORMATION_SCHEMA.COLUMNS where  table_schema=upper('/schema_name/') and table_name=upper('/table_name/') ;").format(schema_name=schema_name,table_name=table_name)
        target_table_column=session.sql(sql_target_column)
        target_table_column=target_table_column.to_pandas()
        if(len(target_table_column)<1):
            break
        for len_column_count_target in range(len(target_table_column)):
            name=target_table_column.iloc[len_column_count_target]['COLUMN_NAME']
            if(column_name==target_table_column.iloc[len_column_count_target]['COLUMN_NAME']):
                source_table.iloc[len_column_count,2]='✅'
                Len_column_count_check +=1
                source_table.iloc[len_column_count,4]=target_table_column.iloc[len_column_count_target]['DATA_TYPE']
                if(source_table.iloc[len_column_count]["Data type On Source"]==('STRING')):
                    if(target_table_column.iloc[len_column_count_target]['DATA_TYPE']==("TEXT")):
                        source_table.iloc[len_column_count,3]='✅'
                if(source_table.iloc[len_column_count]["Data type On Source"]==('INT64')):
                    if(target_table_column.iloc[len_column_count_target]['DATA_TYPE']==("INT") or ("NUMBER")):
                        source_table.iloc[len_column_count,3]='✅'
                if(source_table.iloc[len_column_count]["Data type On Source"]==('FLOAT64')):
                    if(target_table_column.iloc[len_column_count_target]['DATA_TYPE']==("FLOAT4") or ("FLOAT")):
                        source_table.iloc[len_column_count,3]='✅'
                if(source_table.iloc[len_column_count]["Data type On Source"]==('TIMESTAMP')):
                    if(target_table_column.iloc[len_column_count_target]['DATA_TYPE']==("TIMESTAMP") or ("TIMESTAMP_LTZ")):
                        source_table.iloc[len_column_count,3]='✅'
                if(source_table.iloc[len_column_count]["Data type On Source"]==('DATE')):
                    if(target_table_column.iloc[len_column_count_target]['DATA_TYPE']==("DATE") ):
                        source_table.iloc[len_column_count,3]='✅'
                if(source_table.iloc[len_column_count]["Data type On Source"]==('DATETIME')):
                    if(target_table_column.iloc[len_column_count_target]['DATA_TYPE']==("TIMESTAMP_NTZ") ):
                        source_table.iloc[len_column_count,3]='✅'
                break
    if(len(target_table_column)<1):
        st.error("No Table Found On Snowflake") 
        st.exception("No Table Found On Snowflake")
        
            
    else:
        st.dataframe(source_table)
        with col3:
            unloaded_column=len_column-Len_column_count_check
            Table_Struct = np.array([Len_column_count_check,unloaded_column])
            Table_Column_count = [Len_column_count_check,unloaded_column]
            lables_info = ["No of column Loaded", "No of Column Unloaded " ]
            myexplode = [0, 0.3]
            mycolors = ["#ACEC6B", "#FF7777"]
            plt.pie(Table_Struct, labels = Table_Column_count, explode = myexplode,startangle = 90,colors = mycolors)
            plt.legend(title = "Table Column Status:",labels=lables_info )
            a=plt.show() 
            st.set_option('deprecation.showPyplotGlobalUse', False)
            st.pyplot(a)
        if(len_column==Len_column_count_check):
            st.success('All Column are Avaiable In Table.', icon="✅")   
        else:
            st.warning('Some Column are missing Please verify them.', icon="❌")
""".format(database=database,schema=schema))
    stream_script=stream_script.replace("/count_table_source/","{count_table_source}")
    stream_script=stream_script.replace("Migration_Report_Schema_/time/","Migration_Report_Schema_{time[0][0]}")
    stream_script=stream_script.replace("/count_Schema/","{count_Schema}")
    stream_script=stream_script.replace("/schema_name/","{schema_name}")
    stream_script=stream_script.replace("/table_name/","{table_name}")     
    results = stream_script
    # print(results)
    text_file_path = 'D:\Stream_lit_code_frame\streamlit.py'
    with open(text_file_path, 'w', encoding='utf-8') as text_file:
        text_file.write(stream_script)


    
if __name__ == '__main__':
    app.run(debug=True, port=8000)