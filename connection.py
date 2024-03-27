'''
from flask import Flask, render_template, request, jsonify
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas


app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/biq', methods=['POST'])
def biq():
    return render_template('file_upload.html')

@app.route('/upload', methods=['POST'])
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
            global schemas
            schemas = fetch_schemas(bq_client)
            # Tables = fetch_tables(bq_client)
            # for i in schemas:
            #     print(fetch_tables(project_id, i, bq_client))
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
    # return render_template("Tables.html", tables = tables, schema = schema_name, schemas= schemas)
    return render_template('schemas_copy.html', show_popup=True,tables = tables, schema = schema_name)

#Function to return the tables in a schema
def fetch_schemas_tables(client,schema_name):
    # print(schema_name)
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
    schemas_list = schema_list
    print(schemas_list)
    account_name = request.form.get("accountname")
    print(account_name)
    username = request.form.get("username")
    print(username)
    password = request.form.get("password")
    print(password)
    warehouse = request.form.get("warehouse")
    conn = snowflake.connector.connect(
        user= username,
        password= password,
        account= account_name,
        warehouse= warehouse,
        role = 'ACCOUNTADMIN'
    )
    result = create_schemas_and_copy_table(conn,schemas_list)
    # cursor=conn.cursor()
    # cursor.execute('SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.USERS')
    # data = cursor.fetchall()
    # print(data)
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
       schema = row.schema_name
       print(schema)
       print("777777777777")
       print(schema_list)
       if schema in schema_list :
           print("Gathering ddl {}".format(schema))
           query = """
            select  c.table_catalog, c.table_schema, c.table_name,  string_agg('$1:'||c.column_name) as table_columns , case when t.ddl like 
            '%STRUCT%' or ddl like '%ARRAY%' then 'parquet' else 'parquet' end as export_type, 'N' as copy_done FROM 
            `{}`.{}.INFORMATION_SCHEMA.TABLES as t join
            `{}`.{}.INFORMATION_SCHEMA.COLUMNS as c on c.table_name = t.table_name group by c.table_catalog, 
            c.table_name,c.table_schema,t.ddl;
           """
    
           print("Gathering ddl for tables in schema {}".format(schema))
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
           ddl_query = query.format(project_id,schema,project_id,schema)
           query_job = bq_client.query(ddl_query)
           ddl_set = query_job.result() 
           
           for row in ddl_set:

            df = pd.DataFrame(data=[list(row.values())],columns = Columns) 
            copy_table = pd.concat([copy_table,df] , ignore_index=True)
            table_ddl = " create or replace TABLE SNOWMIGRATE.PUBLIC.BQ_COPY_TABLE ( TABLE_CATALOG VARCHAR(16777216), TABLE_SCHEMA VARCHAR(16777216), TABLE_NAME VARCHAR(16777216),TABLE_COLUMNS VARCHAR(16777216), EXPORT_TYPE VARCHAR(16777216), COPY_DONE VARCHAR) "

            conn.cursor().execute(table_ddl)
            write_pandas(conn, copy_table , 'BQ_COPY_TABLE', 'SNOWMIGRATE','PUBLIC' )
            print("BQ_COPY_TABLE created succesfully in SNOWMIGRATE.PUBLIC")
            
            schema_name = row.table_schema
            create_schema = "create schema if not exists SNOWMIGRATE.{}".format(schema_name)
            conn.cursor().execute(create_schema)

            print("Schema {} created in SNOWMIGRATE Database".format(schema_name))
    
           #FOR TABLES
           ddl_table_query = table_query.format(project_id,schema)
           query_table_job = bq_client.query(ddl_table_query)
           ddl_table_set = query_table_job.result()
    
           for my_row in ddl_table_set:
               table_name = my_row.table_name
               ddl = my_row.ddl
               ddl2 = ddl.replace(project_id, "SNOWMIGRATE")
               print(ddl2)
               print("Running ddl for table {} in Snowflake".format(table_name))
               use_schema = "use schema SNOWMIGRATE.{}".format(schema)
               conn.cursor().execute(use_schema)
               conn.cursor().execute(ddl2)
               print("Table {} created in SNOWMIGRATE.{} schema".format(table_name,schema))

    # FOR EXPORTING DATA
           export_query = """
           select table_name,case when ddl like '%STRUCT%' or ddl like '%ARRAY%' then 'parquet' else 'parquet' end as export_type
           FROM `{}`.{}.INFORMATION_SCHEMA.TABLES where table_type='BASE TABLE'
           """
           ddl_query = export_query.format(project_id,schema)
           query_job = bq_client.query(ddl_query)
           ddl_export_set = query_job.result()

           for row in ddl_export_set:
               table_name = row.table_name
               export_type = row.export_type
               print("Exporting data for table {} ...export type is {}".format(table_name,export_type))
               destination_uri = "gs://{}/{}/{}/{}-*.{}".format(bucket_name,schema,table_name,table_name,export_type)
               print(destination_uri)
               dataset_ref = bigquery.DatasetReference(project_id, schema)
               table_ref = dataset_ref.table(table_name)
               configuration = bigquery.job.ExtractJobConfig()
               configuration.destination_format ='PARQUET'
               if export_type == 'parquet':
                   extract_job = bq_client.extract_table(
                       table_ref,
                       destination_uri,
                       job_config=configuration,
                       location="US",
                       )
               else:
                   extract_job = bq_client.extract_table(
                       table_ref,
                       destination_uri,
                       location="US"
                       )
               extract_job.result()  # Waits for job to complete.
               print("Exported successfully.. {}:{}.{} to {}".format(project_id, schema, table_name, destination_uri)) 

           
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
                copy_command = "copy into SNOWMIGRATE.{sc}.{tb} from ( select {col_list} from @SNOWMIGRATE.PUBLIC.snowmigrate_stage/{sc}/{tb}/{tb}(file_format => my_parquet_format))"
                print(table_name + export_type)

                copy_command = copy_command.replace('{sc}', table_schema,2)
                copy_command = copy_command.replace('{tb}', table_name,3)
                copy_command = copy_command.replace('{col_list}', table_columns )

                conn.cursor().execute(copy_command)
                counter+=1
                i+=1
                print(counter)
                print("{} Data Loaded succesfully with {}".format(table_name,copy_command))
       else :
            print("Done")
   return "Success"

if __name__ == '__main__':
    app.run(debug=True, port=8000)  '''

from flask import Flask, render_template, request, jsonify
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas




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
    schemas_list = schema_list
    print(schemas_list)
    result = create_schemas_and_copy_table(conn,schemas_list)
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
       schema = row.schema_name
       print(schema)
       print("777777777777")
       print(schema_list)
       if schema in schema_list :
           print("Gathering ddl {}".format(schema))
           query = """
            select  c.table_catalog, c.table_schema, c.table_name,  string_agg('$1:'||c.column_name) as table_columns , case when t.ddl like 
            '%STRUCT%' or ddl like '%ARRAY%' then 'parquet' else 'parquet' end as export_type, 'N' as copy_done FROM 
            `{}`.{}.INFORMATION_SCHEMA.TABLES as t join
            `{}`.{}.INFORMATION_SCHEMA.COLUMNS as c on c.table_name = t.table_name group by c.table_catalog, 
            c.table_name,c.table_schema,t.ddl;
           """
    
           print("Gathering ddl for tables in schema {}".format(schema))
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
           ddl_query = query.format(project_id,schema,project_id,schema)
           query_job = bq_client.query(ddl_query)
           ddl_set = query_job.result() 
           
           for row in ddl_set:
            df = pd.DataFrame(data=[list(row.values())],columns = Columns) 
            copy_table = pd.concat([copy_table,df] , ignore_index=True)
            table_ddl = " create or replace TABLE SNOWMIGRATE.PUBLIC.BQ_COPY_TABLE ( TABLE_CATALOG VARCHAR(16777216), TABLE_SCHEMA VARCHAR(16777216), TABLE_NAME VARCHAR(16777216),TABLE_COLUMNS VARCHAR(16777216), EXPORT_TYPE VARCHAR(16777216), COPY_DONE VARCHAR) "

            conn.cursor().execute(table_ddl)
            write_pandas(conn, copy_table , 'BQ_COPY_TABLE', 'SNOWMIGRATE','PUBLIC' )
            print("BQ_COPY_TABLE created succesfully in SNOWMIGRATE.PUBLIC")
            
            schema_name = row.table_schema
            create_schema = "create schema if not exists SNOWMIGRATE.{}".format(schema_name)
            conn.cursor().execute(create_schema)

            print("Schema {} created in SNOWMIGRATE Database".format(schema_name))
    
           #FOR TABLES
           ddl_table_query = table_query.format(project_id,schema)
           query_table_job = bq_client.query(ddl_table_query)
           ddl_table_set = query_table_job.result()
    
           for my_row in ddl_table_set:
               table_name = my_row.table_name
               ddl = my_row.ddl
               ddl2 = ddl.replace(project_id, "SNOWMIGRATE")
               print(ddl2)
               print("Running ddl for table {} in Snowflake".format(table_name))
               use_schema = "use schema SNOWMIGRATE.{}".format(schema)
               conn.cursor().execute(use_schema)
               conn.cursor().execute(ddl2)
               print("Table {} created in SNOWMIGRATE.{} schema".format(table_name,schema))

    # FOR EXPORTING DATA
           export_query = """
           select table_name,case when ddl like '%STRUCT%' or ddl like '%ARRAY%' then 'parquet' else 'parquet' end as export_type
           FROM `{}`.{}.INFORMATION_SCHEMA.TABLES where table_type='BASE TABLE'
           """
           ddl_query = export_query.format(project_id,schema)
           query_job = bq_client.query(ddl_query)
           ddl_export_set = query_job.result()

           for row in ddl_export_set:
               table_name = row.table_name
               export_type = row.export_type
               print("Exporting data for table {} ...export type is {}".format(table_name,export_type))
               destination_uri = "gs://{}/{}/{}/{}-*.{}".format(bucket_name,schema,table_name,table_name,export_type)
               print(destination_uri)
               dataset_ref = bigquery.DatasetReference(project_id, schema)
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
               print("Exported successfully.. {}:{}.{} to {}".format(project_id, schema, table_name, destination_uri)) 

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
                copy_command = "copy into SNOWMIGRATE.{sc}.{tb} from ( select {col_list} from @SNOWMIGRATE.PUBLIC.snowmigrate_stage/{sc}/{tb}/{tb}(file_format => my_parquet_format))"
                print(table_name + export_type)

                copy_command = copy_command.replace('{sc}', table_schema,2)
                copy_command = copy_command.replace('{tb}', table_name,3)
                copy_command = copy_command.replace('{col_list}', table_columns )

                conn.cursor().execute(copy_command)
                counter+=1
                i+=1
                print(counter)
                print("{} Data Loaded succesfully with {}".format(table_name,copy_command))
       else :
            print("Done")
   
   auditing_log_into_Snowflake(conn,project_id,schema_list)
   return "Success"


# Endpoint to test GCP service account connection
@app.route('/test_connection', methods=['POST'])
def test_connection():

    account_name = request.form.get("accountname")
    print(account_name)
    username = request.form.get("username")
    print(username)
    
    password = request.form.get("password")
    print(password)
    warehouse = request.form.get("warehouse")
    global conn
    conn = snowflake.connector.connect(
        user= username,
        password= password,
        account= account_name,
        warehouse= warehouse,
        role = 'ACCOUNTADMIN'
    )

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
    cursor.execute('USE WAREHOUSE SNOW_MIGRATE_WAREHOUSE')
    if(cursor.fetchone()):
             return True
    else:
        return False

def check_database_access(cursor):
    # Replace with your actual logic
    cursor.execute('USE DATABASE SNOW_MIGRATE_DATABASE')
    if(cursor.fetchone()):
             return True
    else:
        return False

def check_role_access(cursor):
    # Replace with your actual logic
    cursor.execute('USE ROLE SNOW_MIGRATE_ROLE')
    if(cursor.fetchone()):
             return True
    else:
        return False

def check_user_access(cursor):
    # Replace with your actual logic
    cursor.execute("SHOW USERS LIKE 'SNOW_MIGRATE_USER'")
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
        cursor.execute("USE DATABASE SNOW_MIGRATE_DATABASE")
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
        cursor.execute("USE DATABASE SNOW_MIGRATE_DATABASE")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS test_schema")
        return True
    except Exception as e:
        return False




# Endpoint to check Storage integration, file fromat, stage exist
@app.route('/CheckObjectExistence', methods=['POST'])
def check_Object_Exist():
    cursor = conn.cursor()
    if isinstance(cursor, str):
        # If cursor is a string, it means an error occurred during Snowflake connection
        return jsonify({"success": False, "error": cursor})
    else:
        try:
            storage_integration_exists = check_object_exists(cursor, 'INTEGRATIONS', 'SNOW_MIGRATE_INTEGRATION')
            file_format_exists = check_object_exists(cursor, 'FILE FORMATS', 'my_parquet_format')
            stage_exists = check_object_exists(cursor, 'STAGES', 'SNOW_MIGRATE_STAGE')

            print(f"Storage Integration Exists: {storage_integration_exists}")
            print(f"File Format Exists: {file_format_exists}")
            print(f"Stage Exists: {stage_exists}")
            if storage_integration_exists and file_format_exists and stage_exists:
                    return jsonify({"success": True})
            else:
                error_message = ""
                if not storage_integration_exists:
                    error_message += "Storage Integration Object not Exist. "
                if not file_format_exists:
                    error_message += "File Format Object not Exist. "
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
@app.route('/StorageIntegrationAccess', methods=['POST'])
def StorageIntegrationAccess():
    cursor = conn.cursor()
    if isinstance(cursor, str):
        # If cursor is a string, it means an error occurred during Snowflake connection
        return jsonify({"success": False, "error": cursor})
    else:
        try:
             # Check grants for integration
            integration_name = 'SNOW_MIGRATE_INTEGRATION'
            role_name = 'SNOW_MIGRATE_ROLE'
            privilege = 'OWNERSHIP'
            granted_on = 'INTEGRATION'
            integration_grants = check_grants(cursor, privilege, granted_on, integration_name, role_name)
            
            # Check grants for file format
            file_format_name = 'SNOW_MIGRATE_DATABASE.SNOW_MIGRATE_SCHEMA.MY_PARQUET_FORMAT'
            role_name = 'SNOW_MIGRATE_ROLE'
            privilege = 'OWNERSHIP'
            granted_on = 'FILE FORMAT'
            file_format_grants = check_grants(cursor, privilege, granted_on, file_format_name, role_name)
            
            # Check grants for stage
            stage_name = 'SNOW_MIGRATE_DATABASE.SNOW_MIGRATE_SCHEMA.SNOW_MIGRATE_STAGE'
            role_name = 'SNOW_MIGRATE_ROLE'
            privilege = 'OWNERSHIP'
            granted_on = 'STAGE'
            stage_grants = check_grants(cursor, privilege, granted_on, stage_name, role_name)
        
            print(f"Grants for integration '{integration_name}': {integration_grants}")
            print(f"Grants for file format '{file_format_name}': {file_format_grants}")
            print(f"Grants for stage '{stage_name}': {stage_grants}")

            if integration_grants and file_format_grants and stage_grants:
                    return jsonify({"success": True})
            else:
                error_message = ""
                if not integration_grants:
                    error_message += "Storage Integration Object not Accessible. "
                if not file_format_grants:
                    error_message += "File Format Object not Accessible. "
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
    
    query_job = bq_client.query(query_TABLE_DETAILS)
    results_schema_database_lst = query_job.result()
    schema = [field.name for field in results_schema_database_lst.schema]
    # Create DataFrame with both column names and data---------------------------------------------------------------------------------------------
    dataframe_schema_table_info = pd.DataFrame(data=[list(row.values()) for row in results_schema_database_lst], columns=schema)
    
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
    
    query_job = bq_client.query(query_ddl)
    results_ddl_St_db = query_job.result()
    schema = [field.name for field in results_ddl_St_db.schema]
    dataframe_ddl_table_info = pd.DataFrame(data=[list(row.values()) for row in results_ddl_St_db ], columns=schema)
    
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
    schema = [field.name for field in results_copy_dol.schema]
    # ----------------------------------------Create DataFrame with both column names and data-----------------------------------------------------------
    dataframe_copy_dol = pd.DataFrame(data=[list(row.values()) for row in results_copy_dol], columns=schema)
 
    result_ddl_ed_table = pd.merge(dataframe_schema_table_info, dataframe_ddl_table_info, how="outer", on=["table_catalog","table_schema","table_name"])
    result_ddl_ed_table = pd.merge(result_ddl_ed_table,dataframe_copy_dol, how="outer", on=["table_catalog","table_schema","table_name"])
    write_pandas(snowflake_connection_config,result_ddl_ed_table,'META_TABLES_STRUCT_SOURCE',database="SNOWMIGRATE",schema="PUBLIC", auto_create_table=True,overwrite=True,table_type="transient")
    print(result_ddl_ed_table)
    
    if len(schema_names)<2:
        query = (f"""select table_catalog,table_schema,table_name,column_name,ordinal_position,is_nullable,data_type from `{project_name}`.`region-US`.INFORMATION_SCHEMA.COLUMNS where table_schema = '{schema_name_single}' ;""")
    else:
        query = (f"""select table_catalog,table_schema,table_name,column_name,ordinal_position,is_nullable,data_type from `{project_name}`.`region-US`.INFORMATION_SCHEMA.COLUMNS where table_schema in {schema_name_tuple} ;""")
    
    query_job = bq_client.query(query)
    results_column_lst= query_job.result()
    schema = [field.name for field in results_column_lst.schema]
    dataframe_column_info = pd.DataFrame(data=[list(row.values()) for row in results_column_lst], columns=schema)
    write_pandas(snowflake_connection_config,dataframe_column_info,'META_COLUMNS_STRUCT_SOURCE',database="SNOWMIGRATE",schema="PUBLIC", auto_create_table=True,overwrite=True,table_type="transient")

if __name__ == '__main__':
    app.run(debug=True, port=8000)