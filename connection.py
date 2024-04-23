from flask import Flask, render_template, request, jsonify, abort, send_file
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import streamlit_connection as st
from google.api_core.exceptions import NotFound
from googleapiclient import discovery
from google.cloud import storage
import csv
import logging
import os, re


def setup_logger(log_filename):
    # Create a logger with the specified filename
    logger = logging.getLogger(__name__)
    
    # Set up logging configuration for the logger
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)8s - %(name)10s > %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler = logging.FileHandler(r"C:\Users\Swetha\Desktop\Log/migration.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger


def log_multiline_message(logger, message):
    # Log the entire multiline message as a single entry with proper indentation and formatting
    logger.info('\n'.join([' ' * 4 + line.strip() for line in message.split('\n')]) + '\n')


def log_system_message(logger, err_message):
    # Log the entire multiline message as a single entry with proper indentation and formatting
    logger.error('\n'.join([' ' * 4 + line.strip() for line in err_message.split('\n')]) + '\n')

logger = setup_logger(r"C:\Users\Swetha\Desktop\Log/migration.log")

def Migration_report(connection, database, schema):
    try:
        logger.info('Creating Stage for Streamlit App')
        msg = f'create stage if not exists {database}.{schema}.Migration_Report;'
        logger.info(msg)

        print('inside migration report')
        connection_cursor = connection.cursor()
        connection_cursor.execute(f"create stage if not exists {database}.{schema}.Migration_Report;")
        logger.info('Stage named Migration_Report Created Successfully')
        connection_cursor.execute(r"put file://C:\Users\Swetha\Desktop\streamlit\environment.yml @{database}.{schema}.Migration_Report/REPORT_FLD AUTO_COMPRESS=FALSE")
        connection_cursor.execute(r"put file://C:\Users\Swetha\Desktop\streamlit\streamlit.py @{database}.{schema}.Migration_Report/REPORT_FLD AUTO_COMPRESS=FALSE")
        logger.info('Creating Streamlit App')
        msg = f"""create or replace STREAMLIT {database}.{schema}.Migration_Report
            ROOT_LOCATION = '@{database}.{schema}.Migration_Report/REPORT_FLD'
            MAIN_FILE = '/streamlit.py',
            QUERY_WAREHOUSE = SNOW_MIGRATE_WAREHOUSE;"""
        log_multiline_message(logger, msg)
        print(msg)
        connection_cursor.execute(f"create or replace STREAMLIT {database}.{schema}.Migration_Report ROOT_LOCATION = '@{database}.{schema}.Migration_Report/REPORT_FLD' MAIN_FILE = '/streamlit.py', QUERY_WAREHOUSE = SNOW_MIGRATE_WAREHOUSE;")
        logger.info('Streamlit App \'Migration_Report\' Created Successfully')
        log_multiline_message(logger, """create or replace table {}.{}.load_history as
                              (select * from {}.INFORMATION_SCHEMA.LOAD_HISTORY)""".format(database, schema, database))
        connection_cursor.execute(f"create or replace table {database}.{schema}.load_history as(select * from {database}.INFORMATION_SCHEMA.LOAD_HISTORY)")
        return None
    except Exception as error:
        log_system_message(logger, """Error occurred during Streamlit app Creation process:
                            {}""".format(str(error)))
        print(error)
        return error


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
        logger.error('No file part')
        return 'No file part'
    
    file = request.files['file']
    global bucket_name
    bucket_name = request.form.get("bucket")
    # If the user does not select a file, the browser submits an empty file without a filename
    if file.filename == '':
        logger.error('No selected file')
        return 'No selected file'

    if file and file.filename.endswith('.json'):
        # Read JSON file and establish connection to BigQuery
        json_content = file.read()
        global credentials
        credentials = service_account.Credentials.from_service_account_info(json.loads(json_content))
        global project_id
        project_id = credentials.project_id

        if not project_id:
            logger.error('Invalid JSON format. Please provide project ID.')
            return 'Invalid JSON format. Please provide project ID.'

        try:
            # Establish connection to BigQuery using the provided credentials
            global bq_client
            bq_client = bigquery.Client(credentials = credentials, project = project_id)
            global storage_client
            storage_client = storage.Client(credentials = credentials, project = project_id)
            # Fetch schemas from BigQuery and display them
            logger.info(' Connection established successfully')
            return render_template('file_upload.html', show_popup = True)
        except Exception as e:
            log_system_message(logger, """Error establishing connection to BigQuery:
                            {}""".format(str(e)))
            return f'Error establishing connection to BigQuery: {str(e)}'

    else:
        logger.error('Invalid file format')
        return 'Invalid file format'
#Run this pip install command for this library to work
#pip install google-api-python-client google-auth google-auth-oauthlib google-auth-httplib2
#pip install google-cloud-storage
    
def test_service_account_connection():
    try:
        client = bq_client
        # Try to get the service account email to check the connection
        service_account_email = client.get_service_account_email()

        if service_account_email:
            log_multiline_message(logger, """Successfully connected to GCP. Service account email:
                            {}""".format(service_account_email))
            return True
        else:
            logger.error("Failed to retrieve service account email. Connection to GCP failed.")
            return False
    except Exception as e:
        log_system_message(logger, """Error connecting to GCP:
                            {}""".format(str(e)))
        return False



def Check_role_permissions():
    try:
        # Build the IAM service
        service = discovery.build('iam', 'v1', credentials = credentials)

        # Name of the role to search for
        role_name = 'projects/' + project_id + '/roles/MigrateRole'

        # Make a request to get details of the specific role
        role_details = service.projects().roles().get(name = role_name).execute()

        permissions = role_details.get('includedPermissions', [])

        # Define the expected permissions
        expected_permissions = {
            'bigquery.datasets.create',
            'bigquery.datasets.get',
            'bigquery.datasets.update',
            'bigquery.jobs.create',
            'bigquery.routines.get',
            'bigquery.routines.list',
            'bigquery.tables.export',
            'bigquery.tables.get',
            'bigquery.tables.getData',
            'bigquery.tables.list',
            'bigquery.tables.replicateData',
            'bigquery.tables.updateData',
            'bigquery.transfers.get',
            'bigquery.transfers.update',
            'resourcemanager.projects.get',
            'storage.buckets.get',
            'storage.objects.create',
            'storage.objects.delete'
        }

        # Finding missing permissions
        missing_permissions = expected_permissions - set(permissions)

        missing_permissions_str = ', '.join(sorted(missing_permissions))

        # Check for failures
        failure_message = ""
        if missing_permissions:
            failure_message += f"Missing Permissions: {missing_permissions_str}\n"

        # Log role details
        logger.info('Role Name: ')
        msg = role_details['name']
        logger.info(msg)
        msg = ""
        for index, permission in enumerate(sorted(permissions)):
            if index == len(permissions) - 1:  # Check if it's the last permission
                msg += f"- {permission}"
            else:
                msg += f"- {permission}\n"
        log_multiline_message(logger, """Permissions:
                    {}""".format(msg))



        # Log failure message if any
        if failure_message:
            logger.error("Failure:")
            failure_message = failure_message + "\nFollow steps in GCP Setup page to create Custom Role"
            log_system_message(logger, failure_message)
            return failure_message
        
        return ""
    
    except Exception as e:
        log_system_message(logger, """Failed to create dataset:
                            {}""".format(str(e)))
        return jsonify({"success": False})
    

        
def Check_Bucket_Existence():
    """Check if a bucket exists in the specified GCP project."""
    try:
        bucket = storage_client.get_bucket(bucket_name)
        logger.info(f"""The bucket '{bucket_name}' exists in the project '{project_id}'.""")
        return True
    except Exception as e:
        if isinstance(e, NotFound):
            logger.error(f"""The bucket '{bucket_name}' does not exist in the
                                project '{project_id}'.""")
            return False
        else:
            logger.error("An error occurred:", e)
            return False


@app.route('/test_service_account_connection', methods = ['POST'])
def test_service_account_connection_route():
    logger.info("Received request to test service account connection")
    print("Received request to test bigquery service account connection")
    try:
        success = test_service_account_connection()
        if success:
            logger.info('Successfully to connect to GCP')
            return jsonify({'success': True})
        else:
            logger.error("Failed to connect to GCP")
            return jsonify({'success': False, 'error': 'Failed to connect to GCP'}), 500
    except Exception as e:
        log_system_message(logger, """An error occurred:
                            {}""".format(str(e)))
        return jsonify({'success': False, 'error': 'An error occurred while testing the service account connection'}), 500


####

@app.route('/GrantAccessCheckBigquery', methods = ['POST'])
def CheckBigqueryDatasetsCreatePermissions():
    print("Checking Permissions in MigrateRole")
    logger.info("Checking Permissions in MigrateRole")
    try:
        status = Check_role_permissions()
        if(status == ""):
            logger.info("Permissions checked successfully.")
            return jsonify({'success': True})
        else:
            err_msg = str({status})
            log_system_message(logger, """Error:
                            {}""".format(err_msg))
            return jsonify({'success': False, 'error':status}), 500
    except Exception as e:
        print(f"Error occurred: {e}")
        log_system_message(logger, """An error occurred:
                            {}""".format(str(e)))
        return jsonify({'success': False, 'error': 'An error occurred while checking permissions'}), 500


@app.route('/BucketExistCheck', methods = ['POST'])
def CheckBucketCreated():
    print("Checking whether GCS Bucket Exist or not")
    logger.info("Checking whether GCS Bucket Exists or not")
    try:
        success = Check_Bucket_Existence()
        if success:
            logger.info("Bucket exists with the given name.")
            return jsonify({'success': True})
        else:
            log_system_message(logger, """Bucket does not exist with the given name.
                    Make sure to give the correct name in the UI""")
            return jsonify({'success': False, 'error':'Bucket does not Exist with the given name, Make sure to give the correct name in the UI'}), 500
    except Exception as e:
        print(f"Error occurred: {e}")
        log_system_message(logger, """Error occurred:
                            {}""".format(str(e)))
        return jsonify({'success': False, 'error': 'An error occurred while checking permissions'}), 500



@app.route('/fetch_schemas', methods = ['POST'])    
def fetch_schemas():
    logger.info("Fetching schemas from BigQuery")
    try:
        client = bq_client
        datasets = list(client.list_datasets())
        schemas = {}
        for dataset in datasets:
            sch = dataset.dataset_id
            tables = fetch_schemas_tables(bq_client, sch)
            schemas[sch] = tables
        return render_template('schemas_copy.html', schemas = schemas)
    except Exception as e:
        log_system_message(logger, """An error occurred while fetching schemas:
                            {}""".format(str(e)))
        return jsonify({'success': False, 'error': 'An error occurred while fetching schemas'}), 500



# @app.route('/get_tables',  methods =["GET", "POST"])
# def get_tables():
#     Client= bq_client
#     schema_name = request.form.get("connect-btn")
#     # Fetch tables for the given schema
#     tables = fetch_schemas_tables(Client, schema_name)
#     print(tables)
#     # return render_template("schemas.html", tables = tables, schema = schema_name,show_popup=True)
#     return render_template('schemas_copy.html', show_popup=True,tables = tables, schema = schema_name)


#Function to return the tables in a schema
def fetch_schemas_tables(client, schema_name):
    print(schema_name)
    logger.info(f"Fetching tables for schema '{schema_name}'")
    try:
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
    except Exception as e:
        log_system_message(logger, """An error occurred while fetching tables for schema '{}':
                            {}""".format(schema_name, str(e)))
        return []


@app.route('/snowflake_form', methods = ["GET", "POST"])
def snowflake_form():
    try:
        global inner_dict
        if(request.method == 'GET'):
            data = dict(request.args)
            # input_dict = json.loads(request.args)
            inner_key = next(iter(data))
            inner_dict = json.loads(inner_key) 
            print(inner_dict)
            print('----------------------')
        return render_template("snowflake_form_copy.html")
    except Exception as e:
        log_system_message(logger, """An error occurred
                            {}""".format(str(e)))
        return e
    
###

@app.route('/connect_snowflake', methods = ["GET", "POST"])
def connect_snowflake():
    try:
        global account_name, role, username, password, warehouse, database, schema
        account_name = request.form.get("accountname")
        print(account_name)
        logger.info(f"Received account name: {account_name}")

        role = request.form.get("role")
        print(role)
        logger.info(f"Received role: {role}")

        username = request.form.get("username")
        print(username)
        logger.info(f"Received username: {username}")

        password = request.form.get("password")
        print(password)
        logger.info("Password received but not logged for security reasons")

        warehouse = request.form.get("warehouse")
        print(warehouse)
        logger.info(f"Received warehouse: {warehouse}")

        database = request.form.get("database")
        print(database)
        logger.info(f"Received database: {database}")

        schema = request.form.get("schema")
        print(schema)
        logger.info(f"Received schema: {schema}")

        global conn
        conn = snowflake.connector.connect(
            user = username,
            password = password,
            account = account_name,
            warehouse = warehouse,
            role = role,
            database = database,
            schema = schema
        )
        self_execute(conn, bucket_name , schema , database )
        # result = create_schemas_and_copy_table(conn,schemas_list)
        return render_template('snowflake_form_copy.html', show_popup = True)
    except Exception as e:
        log_system_message(logger, """An error occurred
                            {}""".format(str(e)))
        return e

    
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
    print(query_create_ff.format(db = database, schema = schema))
    print(query_create_stage.format(db = database, schema = schema, bkt = bucket_name))

    # conn.cursor().execute(query_self_integration.format(bkt = bucket_name))
    # conn.cursor().execute(query_use_db.format(db = database))
    # conn.cursor().execute(query_create_schema.format(schema = schema))
    # conn.cursor().execute(query_create_ff.format(db = database , schema = schema))
    # conn.cursor().execute(query_create_stage.format(bkt = bucket_name))

    try :
        # conn.cursor().execute(query_self_updated)
        logger.info("Executing SQL queries")
        conn.cursor().execute(query_use_role.format(role = role))
        logger.info("USE role query executed successfully")
        
        conn.cursor().execute(query_use_db.format(db = database))
        logger.info("USE DATABASE query executed successfully")

        conn.cursor().execute(query_create_schema.format(schema = schema))
        logger.info("CREATE SCHEMA query executed successfully")

        conn.cursor().execute(query_self_integration.format(bkt = bucket_name))
        logger.info("Self integration query executed successfully")

        conn.cursor().execute(query_create_ff.format(db = database, schema = schema))
        logger.info("CREATE FILE FORMAT query executed successfully")

        conn.cursor().execute(query_create_stage.format(db = database, schema = schema, bkt = bucket_name))
        logger.info("CREATE STAGE query executed successfully")
        
        # print(query_self_updated);
        print('Execution was succesfull');
        logger.info("SQL queries executed successfully")

    except snowflake.connector.errors.ProgrammingError as e:
        print('SQL Execution Error: {0}'.format(e.msg))
        log_system_message(logger, """SQL Execution Error:
                            {}""".format(str(e.msg)))
        
        print('Snowflake Query Id: {0}'.format(e.sfqid))
        log_system_message(logger, """Snowflake Query Id:
                            {}""".format(str(e.sfqid)))
        
        print('Error Number: {0}'.format(e.errno))
        log_system_message(logger, """Error Number:
                            {}""".format(str(e.errno)))
        
        print('SQL State: {0}'.format(e.sqlstate))
        log_system_message(logger, """SQL State:
                            {}""".format(str(e.sqlstate)))
    

# Endpoint to test GCP service account connection
@app.route('/test_connection', methods = ['POST'])
def test_connection():
    # Replace this with your actual testing logic
    print("Received request to test snowflake service account connection")
    logger.info("Received request to test service account connection")
    cursor = conn.cursor()
    if isinstance(cursor, str):
        # If cursor is a string, it means an error occurred during Snowflake connection
        error_message = cursor
        log_system_message(logger, """Snowflake connection error:
                            {}""".format(str(e)))
        return jsonify({"success": False, "error": error_message}), 500
    else:
        # Connection successful, perform your logic here
        try:
            cursor.execute('SELECT 5+5')
            row = cursor.fetchone()
            if row[0] == 10:
                success = True
                print(row[0])
                logger.info("Successfully tested service account connection")
            return jsonify({"success": True})
        except Exception as e:
            error_message = str(e).split(":")[-1].strip()
            log_system_message(logger, """An error occurred while testing service account connection:
                            {}""".format(error_message))
            return jsonify({"success": False, "error": error_message}), 500
    

# Endpoint to check if required roles are granted
@app.route('/GrantAccessCheck', methods = ['POST'])
def grant_access_check():
    cursor = conn.cursor()
    if isinstance(cursor, str):
        # If cursor is a string, it means an error occurred during Snowflake connection
        error_message = cursor
        log_system_message(logger, """Snowflake connection error:
                            {}""".format(str(e)))
        return jsonify({"success": False, "error": error_message}), 500
    else:
        try:
            # Replace this with your actual logic to check access for Warehouse, Database, Role, and User
            warehouse_access = check_warehouse_access(cursor)
            database_access = check_database_access(cursor)
            role_access = check_role_access(cursor)
            user_access = check_user_access(cursor)

            if warehouse_access and database_access and role_access and user_access:
                logger.info("Required roles are granted")
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
                log_system_message(logger, """Access check failed:
                            {}""".format(str(e)))
                return jsonify({"success": False, "error": error_message.strip()}), 500
        except Exception as e:
            log_system_message(logger, """An error occurred while checking role access: 
                            {}""".format(str(e)))
            return jsonify({"success": False, "error": str(e)}), 500


# Simulated functions to check access for Warehouse, Database, Role, and User
def check_warehouse_access(cursor):
    # Replace with your actual logic
    try:
        logger.info("Checking access for Warehouse")
        warehouse_check = "USE WAREHOUSE {}".format(warehouse)
        cursor.execute(warehouse_check)

        if(cursor.fetchone()):
            logger.info("Warehouse access check successful")
            return True
        else:
            logger.error("Warehouse access check failed")
            return False
    except Exception as e:
        log_system_message(logger, """An error occurred while checking warehouse access: 
                            {}""".format(str(e)))
        return jsonify({"success": False})
    

def check_database_access(cursor):
    # Replace with your actual logic
    try:
        logger.info("Checking access for Database")
        database_check = "USE DATABASE {}".format(database)
        cursor.execute(database_check)
        if(cursor.fetchone()):
            logger.info("Database access check successful")
            return True
        else:
            logger.error("Database access check failed")
            return False
    except Exception as e:
        log_system_message(logger, """An error occurred while checking database access: 
                            {}""".format(str(e)))
        return jsonify({"success": False})
    

def check_role_access(cursor):
    # Replace with your actual logic
    try:
        role_check = "USE ROLE {}".format(role)
        cursor.execute(role_check)
        if(cursor.fetchone()):
            logger.info("Role access check successful")
            return True
        else:
            logger.error("Role access check failed")
            return False
    except Exception as e:
        log_system_message(logger, """An error occurred while checking role access: 
                            {}""".format(str(e)))
        return jsonify({"success": False})
    
    
def check_user_access(cursor):
    try:
        logger.info("Checking access for User")
        user_check = "SHOW USERS LIKE '{}'".format(username)
        cursor.execute(user_check)
        if(cursor.fetchone()):
            logger.info("User access check successful")
            return True
        else:
            logger.error("User access check failed")
            return False
    except Exception as e:
        log_system_message(logger, """An error occurred while checking user access: 
                            {}""".format(str(e)))
        return jsonify({"success": False})


@app.route('/CheckCreatePermissions', methods = ['POST'])
def check_create_permissions():
    try:
        logger.info("Checking if creating table and schema is allowed")
        cursor = conn.cursor()
        if isinstance(cursor, str):
            # If cursor is a string, it means an error occurred during Snowflake connection
            error_message = cursor
            log_system_message(logger, """Snowflake connection error: 
                            {}""".format(str(e)))
            return jsonify({"success": False, "error": error_message}), 500
        else:
            try:
                schema_creation_allowed = check_schema_creation_permission(cursor)
                table_creation_allowed = check_table_creation_permission(cursor)
    
                if table_creation_allowed and schema_creation_allowed:
                    logger.info("Table and schema creation allowed")
                    return jsonify({"success": True})
                else:
                    error_message = ""
                    if not table_creation_allowed:
                        error_message += "Table creation not allowed. "
                    if not schema_creation_allowed:
                        error_message += "Schema creation not allowed. "
                    logger.error(f"Permission check failed: ")
                    err_msg = str(error_message.strip())
                    logger.error(err_msg)
                    return jsonify({"success": False, "error": error_message.strip()}), 500
                
            except Exception as e:
                log_system_message(logger, """An error occurred while checking permissions:
                            {}""".format(str(e)))
                return jsonify({"success": False, "error": str(e)}), 500
    except Exception as e:
        log_system_message(logger, """An error occurred while checking create permissions:
                            {}""".format(str(e)))
        return jsonify({"success": False, "error": str(e)}), 500

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
@app.route('/IntegrationObjectExistence', methods = ['POST'])
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
@app.route('/FileFormatObjectExistence', methods = ['POST'])
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
@app.route('/IntegrationAccess', methods = ['POST'])
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
@app.route('/FormatAccess', methods = ['POST'])
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
@app.route('/StageAccess', methods = ['POST'])
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
    result = create_schemas_and_copy_table(conn,inner_dict)
    return result

#Logging the Execution status
@app.route('/log')
def log():
    # Path to the log file
    log_file_path = "C:\Users\Swetha\Desktop\Log/migration.log"
    
    # Read log content from the file
    if os.path.exists(log_file_path):
        with open(log_file_path, 'r') as log_file:
            log_content = log_file.read()
    else:
        log_content = "Log file not found"
    return render_template('Log.html', log_content_html = log_content)



@app.route('/download-log')
def download_log():
    log_file_path = "C:\Users\Swetha\Desktop\Log/migration.log"
    if os.path.exists(log_file_path):
        return send_file(log_file_path, as_attachment = True, download_name = 'migration.log')
    else:
        abort(404, description = "Log file not found")



# Function to create the schemas and the tables from bigquery to snowflake
def create_schemas_and_copy_table(conn,Dist_user_input):
   query = """select schema_name from `{}`.INFORMATION_SCHEMA.SCHEMATA"""
   log_multiline_message(logger, """Executing query to fetch schema names: 
                          {}""".format(query))
   project_query = query.format(project_id)
   query_job = bq_client.query(project_query)
   rows = query_job.result()
   Columns = ['TABLE_CATALOG', 'TABLE_SCHEMA', 'TABLE_NAME', 'TABLE_COLUMNS', 'EXPORT_TYPE', 'COPY_DONE']
   copy_table = pd.DataFrame(columns = Columns)
#    Preparing a formatted string with column names for logging
   columns_formatted = '\n'.join(['    - ' + column for column in Columns])

   # Logging using the log_multiline_message function
   log_multiline_message(logger, """DataFrame 'copy_table' created with columns:
                    {}""".format(columns_formatted))
  
   schema_list_user_input=tuple(Dist_user_input.keys())
   table_ddl = """create or replace TABLE {}.{}.BQ_COPY_TABLE ( TABLE_CATALOG VARCHAR(16777216),
            TABLE_SCHEMA VARCHAR(16777216), TABLE_NAME VARCHAR(16777216),TABLE_COLUMNS VARCHAR(16777216), 
            EXPORT_TYPE VARCHAR(16777216), COPY_DONE VARCHAR) """.format(database, schema)
   log_multiline_message(logger, """Executing query to create BQ_COPY_TABLE: 
                        {}""".format(table_ddl))
   conn.cursor().execute(table_ddl) 
   print("BQ_COPY_TABLE created succesfully")
   logger.info("BQ_COPY_TABLE created successfully in {}.{}".format(database, schema))


   for row in rows:
       schema_local = row.schema_name
       print(schema_local)
       print("-------------------")
       print(schema_list_user_input)

       logger.info("Processing schema: %s", schema_local)
       logger.info("-------------------")
       logger.info("Target schema list: %s", schema_list_user_input)

       if schema_local in schema_list_user_input:
           table_tuple = tuple(Dist_user_input[schema_local])
           print(table_tuple)
           print("Gathering ddl {}".format(schema_local))
           logger.info("Gathering DDL for schema:{}".format(schema_local))

           query = """
            select  c.table_catalog, c.table_schema, c.table_name,  string_agg('$1:'||c.column_name) as table_columns ,
            case when t.ddl like '%STRUCT%' or ddl like '%ARRAY%' then 'parquet' else 'parquet' end as export_type,
            'N' as copy_done 
            FROM `{}`.{}.INFORMATION_SCHEMA.TABLES as t join 
            `{}`.{}.INFORMATION_SCHEMA.COLUMNS as c on
            c.table_name = t.table_name where c.table_name in {} group by c.table_catalog, c.table_name,c.table_schema,t.ddl;
           """.strip()
           log_multiline_message(logger, """Executing query to gather DDLs:
                        {}""".format(query))
           
           query_2 = """
            select  c.table_catalog, c.table_schema, c.table_name,  string_agg('$1:'||c.column_name) as table_columns , 
            case when t.ddl like '%STRUCT%' or ddl like '%ARRAY%' then 'parquet' else 'parquet' end as export_type,
            'N' as copy_done 
            FROM `{}`.{}.INFORMATION_SCHEMA.TABLES as t join
            `{}`.{}.INFORMATION_SCHEMA.COLUMNS as c on 
            c.table_name = t.table_name where c.table_name in ('{}') group by c.table_catalog,c.table_name,c.table_schema,t.ddl;
           """.strip()
    
           print("Gathering ddl for tables in schema {}".format(schema_local))

           table_query = """
           SELECT table_name,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace
           (replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(ddl,'`',''),
           'INT64','INT'),'FLOAT64','FLOAT'),'BOOL','BOOLEAN'),'STRUCT','VARIANT'),'PARTITION BY','CLUSTER BY ('),';',');'),
           'CREATE TABLE ','CREATE TABLE if not exists '), "table INT,", '"table" INT,'),'_"table" INT,','_table INT,'),
           'ARRAY<STRING>','ARRAY'),'from','"from"'),'_"from"','_from'),'"from"_','from_'),'DATE(_PARTITIONTIME)',
           'date(loaded_at)'),' OPTIONS(',', //'),'));',');'),'_at);','_at));'),'start ','"start" '),'_"start"','_start'),
           'order ','"order" '),'<',', //'),'_"order"','_order') as ddl
           FROM `{}`.{}.INFORMATION_SCHEMA.TABLES where table_type='BASE TABLE' and table_name in {}
           """.strip()

           table_query_2 = """
           SELECT table_name,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace
           (replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(ddl,'`',''),
           'INT64','INT'),'FLOAT64','FLOAT'),'BOOL','BOOLEAN'),'STRUCT','VARIANT'),'PARTITION BY','CLUSTER BY ('),';',');'),
           'CREATE TABLE ','CREATE TABLE if not exists '), "table INT,", '"table" INT,'),'_"table" INT,','_table INT,'),
           'ARRAY<STRING>','ARRAY'),'from','"from"'),'_"from"','_from'),'"from"_','from_'),'DATE(_PARTITIONTIME)',
           'date(loaded_at)'),' OPTIONS(',', //'),'));',');'),'_at);','_at));'),'start ','"start" '),'_"start"','_start'),
           'order ','"order" '),'<',', //'),'_"order"','_order') as ddl
           FROM `{}`.{}.INFORMATION_SCHEMA.TABLES where table_type='BASE TABLE' and table_name in ('{}')
           """.strip()

           log_multiline_message(logger, """Executing query to gather DDLs for tables in schema:
                        {}""".format(table_query))
           
           # FOR SCHEMAS
           logger.info("Execution related to Schemas")
           if(len(table_tuple) < 2):
                table_tuple_1 = table_tuple[0]
                print(table_tuple)
                logger.info("Schema named '{}' has only {} Table available".format(schema_local, len(table_tuple)))
                log_multiline_message(logger, """Listing Tables in the Schema '{}':
                            {}""".format(schema_local, table_tuple))
                ddl_query = query_2.format(project_id, schema_local, project_id, schema_local, table_tuple_1)
                print(ddl_query)
           else:
               ddl_query = query.format(project_id, schema_local, project_id, schema_local, table_tuple)
               logger.info("Schema named '{}' has {} Table available".format(schema_local, len(table_tuple)))
               log_multiline_message(logger, """Listing Tables in the Schema '{}':
                            {}""".format(schema_local, table_tuple))
           query_job = bq_client.query(ddl_query)
           ddl_set = query_job.result()
           
           for row in ddl_set:
            df = pd.DataFrame(data=[list(row.values())],columns = Columns) 
            copy_table = pd.concat([copy_table,df] , ignore_index = True)

            write_pandas(conn, copy_table , 'BQ_COPY_TABLE', database, schema )
            schema_name = row.table_schema
            create_schema = "create schema if not exists {}.{}".format(database, schema_name)
            log_multiline_message(logger, """Creating schema with command: 
                        {}""".format(create_schema))
            conn.cursor().execute(create_schema)
            print("Schema {} created in {} Database".format(schema_name, database))
            logger.info(f"Schema {schema_name} created in {database} Database")
            print(len(table_tuple))
            print(table_tuple)

            
           #FOR TABLES
           logger.info("Execution related to Tables")
           if(len(table_tuple) < 2):
               table_tuple_2 = table_tuple[0]
               ddl_table_query = table_query_2.format(project_id, schema_local, table_tuple_2)
               print(ddl_table_query)
               print(table_tuple)
               log_multiline_message(logger, """Executing query to gather required columns from INFORMATION_SCHEMA.TABLES:
                        {}""".format(ddl_table_query))
           else:
               ddl_table_query = table_query.format(project_id, schema_local, table_tuple)   
               print('else block')
               print(ddl_table_query)
               log_multiline_message(logger, """Executing query to gather required columns from INFORMATION_SCHEMA.TABLES:
                        {}""".format(ddl_table_query))
           query_table_job = bq_client.query(ddl_table_query)
           ddl_table_set = query_table_job.result()

           for my_row in ddl_table_set:
               table_name = my_row.table_name
               ddl = my_row.ddl
               ddl2 = ddl.replace(project_id, database)
               log_multiline_message(logger, ddl2)
               logger.info("Running ddl for table {} in Snowflake".format(table_name))

               print(ddl2)
               print("Running ddl for table {} in Snowflake".format(table_name))
               use_schema = "use schema {}.{}".format(database, schema)
               try:
                    conn.cursor().execute(use_schema)
                    conn.cursor().execute(ddl2)
                    logger.info("Table {} created in {}.{} schema".format(table_name, database, schema))           
               except Exception as e:
                    log_system_message(logger, """Error creating table {}: 
                            {}""".format(table_name, str(e)))
                    print("Table {} created in {}.{} schema".format(table_name, database, schema))
                    return e

       # FOR EXPORTING DATA
           logger.info("Execution related to Exporting Data")
           try:
                export_query = """
                select table_name,case when ddl like '%STRUCT%' or ddl like '%ARRAY%' then
                'parquet' else 'parquet' end as export_type
                FROM `{}`.{}.INFORMATION_SCHEMA.TABLES where table_type = 'BASE TABLE' and table_name in {}
                """.strip()

                export_query_2 = """
                select table_name,case when ddl like '%STRUCT%' or ddl like '%ARRAY%' then
                'parquet' else 'parquet' end as export_type
                FROM `{}`.{}.INFORMATION_SCHEMA.TABLES where table_type = 'BASE TABLE' and table_name in ('{}')
                """.strip()
                # log_multiline_message(logger, """Executing query to Export data: 
                #             {}""".format(export_query))
                
                if(len(table_tuple)<2):
                    table_tuple_3 = table_tuple[0]
                    ddl_query = export_query_2.format(project_id, schema_local, table_tuple_3)
                else:
                    ddl_query = export_query.format(project_id, schema_local, table_tuple) 
                query_job = bq_client.query(ddl_query)
                ddl_export_set = query_job.result()
           except Exception as e:
                log_system_message(logger, """Failed to execute export query or fetch results. Error: 
                            {}""".format(str(e)))
                return e

           for row in ddl_export_set:
               table_name = row.table_name
               export_type = row.export_type
               logger.info("Exporting data to snowflake...")
               logger.info("Exporting data for table {} ...export type is {}".format(table_name, export_type))
               print("Exporting data for table {} ...export type is {}".format(table_name, export_type))
               destination_uri = "gs://{}/{}/{}/{}-*.{}".format(bucket_name, schema_local, table_name, table_name, export_type)
               print(destination_uri)
               log_multiline_message(logger, """Destination URI: 
                        {}""".format(destination_uri))

               dataset_ref = bigquery.DatasetReference(project_id, schema_local)
               table_ref = dataset_ref.table(table_name)
               logger.info("Dataset Reference: {}".format(dataset_ref))
               logger.info("Table Reference: {}".format(table_ref))
               configuration = bigquery.job.ExtractJobConfig()
               configuration.destination_format ='PARQUET'
               try:
                    if export_type == 'parquet':
                        log_multiline_message(logger, """Initiating extract job for table:
                                     {} to {} with format PARQUET.""".format(table_name, destination_uri))
                        extract_job = bq_client.extract_table(
                            table_ref,
                            destination_uri,
                            job_config = configuration,
                            location = "US"
                            )
                    else:
                        log_multiline_message(logger, """Initiating extract job for table: 
                                        {} to {} with default format (non-PARQUET).""".format(table_name, destination_uri))
                        extract_job = bq_client.extract_table(
                            table_ref,
                            destination_uri,
                            location = "US"
                            )
                    extract_job.result()  # Waits for job to complete.
                    log_multiline_message(logger, """Exported successfully.. {}:{}.{} to 
                                    {}""".format(project_id, schema_local, table_name, destination_uri))
                    print("Exported successfully.. {}:{}.{} to {}".format(project_id, schema_local, table_name, destination_uri)) 
               except Exception as e:
                    log_system_message(logger, """Failed to export table {}: 
                                {}""".format(table_name, str(e)))
                    return e
           # LOAD DATA
           SF_query = "select table_name, table_schema, table_columns, export_type from BQ_COPY_TABLE where copy_done ='N'"
           cur = conn.cursor()
           cur.execute(SF_query)
           log_multiline_message(logger, """Executing query in Snowflake: 
                            {}""".format(SF_query))
           result = cur.fetchall()
           column_info = cur.description
           column_names = [info[0] for info in column_info]
           df2 = pd.DataFrame(result , columns = column_names)
           counter = 0
           i = 0

           for i in range(0, len(df2)):
                table_name = df2['TABLE_NAME'].iloc[i];
                table_schema  = df2['TABLE_SCHEMA'].iloc[i];
                table_columns = df2['TABLE_COLUMNS'].iloc[i];
                export_type  = df2['EXPORT_TYPE'].iloc[i];

                logger.info("Preparing to load data for table {table_name} with export type {export_type}")
                
                # Splitting the column list string into individual columns
                columns_list = table_columns.split(',')

                # Creating a multi-line string with each column on its own line
                formatted_columns_list = "\n".join(columns_list)

                copy_command = """copy into {db}.{sc}.{tb} from ( select
                {col_list}
                from @{db}.{sch}.snow_migrate_stage/{sc}/{tb}/{tb}
                (file_format => my_parquet_format))"""
                
                print(table_name + export_type)
            
                copy_command = copy_command.replace('{db}', database,2)
                copy_command = copy_command.replace('{sc}', table_schema,2)
                copy_command = copy_command.replace('{sch}', schema)
                copy_command = copy_command.replace('{tb}', table_name,3)
                copy_command = copy_command.replace('{col_list}', table_columns)

                try:
                    conn.cursor().execute(copy_command)
                    log_multiline_message(logger, """Executing COPY command (Moving data from snow_migrate_stage to respective table): 
                                    {}""".format(copy_command))
                    counter += 1
                    i += 1
                    print(counter)
                    print("{} Data Loaded succesfully with {}".format(table_name, copy_command))
                    log_multiline_message(logger, """{}: Data Loaded successfully for {} using command: 
                                {}""".format(counter, table_name, copy_command))
                except Exception as e:
                    log_system_message(logger, """Failed to load data for table {} : 
                            {}""".format(table_name, e))
                    return e
       else :
            print("Done")
   auditing_log_into_Snowflake(conn, project_id, inner_dict)
   Migration_report(conn, database, schema)  
   return render_template('result.html')


           

# Function to create audit log tables in snowflake
def auditing_log_into_Snowflake(snowflake_connection_config,project_name,Dist_user_input):
    # log_multiline_message(logger, """Starting auditing log function with project_name:
    #              {} and schema_names: {}""".format(project_name, schema_names))
    logger.info("Executing Auditing Log into Snowflake")
    
    table_struct_cln = ['TABLE_CATALOG', 'TABLE_SCHEMA', 'TABLE_NAME', 'TABLE_ROWS', 'DDL', 'TABLE_COLUMNS']
    columns_struct_cln = ['TABLE_CATALOG', 'TABLE_SCHEMA', 'TABLE_NAME', 'COLUMN_NAME', 'ORDINAL_POSITION', 'IS_NULLABLE', 'DATA_TYPE']
    table_struct = pd.DataFrame(columns = table_struct_cln)
    columns_struct= pd.DataFrame(columns = columns_struct_cln)
    schema_list_user_input = tuple(Dist_user_input.keys())
    for schema_name in schema_list_user_input:
        table_tuple = tuple(Dist_user_input[schema_name])
        if len(table_tuple) < 2:
            table_name_single=table_tuple[0]
            # print(schema_name)
            query_TABLE_DETAILS = (f"""select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TOTAL_ROWS from 
                                   `{project_name}`.`region-US`.INFORMATION_SCHEMA.TABLE_STORAGE where table_type = 'BASE TABLE'
                                   and deleted = false and  TABLE_SCHEMA =('{schema_name}') and TABLE_NAME in ('{table_name_single}') ;""")
            print(query_TABLE_DETAILS)
            log_multiline_message(logger, """Executing query for a single schema:
                         {}""".format(query_TABLE_DETAILS))
        else:
            schema_name_tuple=tuple(schema_name)
            query_TABLE_DETAILS = (f"""select TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,TOTAL_ROWS from `{project_name}`.`region-US`.INFORMATION_SCHEMA.TABLE_STORAGE where table_type='BASE TABLE' and deleted=false and  TABLE_SCHEMA =('{schema_name}') and TABLE_NAME in {table_tuple};""")
            print(query_TABLE_DETAILS)
            log_multiline_message(logger, """Executing query for a multiple schemas:
                         {}""".format(query_TABLE_DETAILS))
        query_job = bq_client.query(query_TABLE_DETAILS)
        results_schema_database_lst = query_job.result()
        print(results_schema_database_lst)
        logger.info("Query results fetched successfully.")
        schema_list_name = [field.name for field in results_schema_database_lst.schema]
        log_multiline_message(logger, """Schema column names retrieved: 
                    {}""".format(schema_list_name))
        print(schema_list_name)
    
        # Create DataFrame with both column names and data---------------------------------------------------------------------------------------------
        dataframe_schema_table_info = pd.DataFrame(data = [list(row.values()) for row in results_schema_database_lst], columns = schema_list_name)
        print(dataframe_schema_table_info)
        log_multiline_message(logger, """DataFrame created successfully with the following columns: 
                        {}""".format(schema_list_name))

        if len(table_tuple)<2:
            query_ddl = (f"""select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace
                (replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(ddl,'`',''),'INT64','INT'),'FLOAT64','FLOAT'),
                'BOOL','BOOLEAN'),'STRUCT','VARIANT'),'PARTITION BY','CLUSTER BY ('),';',');'),'CREATE TABLE ','CREATE TABLE if not exists '), "table INT,",
                '"table" INT,'),'_"table" INT,','_table INT,'),'ARRAY<STRING>','ARRAY'),'from','"from"'),'_"from"','_from'),'"from"_','from_'),
                'DATE(_PARTITIONTIME)','date(loaded_at)'),' OPTIONS(',', //'),'));',');'),'_at);','_at));'),'start ','"start" '),'_"start"','_start'),
                'order ','"order" '),'<',', //'),'_"order"','_order') as DDL from `{project_name}`.`region-US`.INFORMATION_SCHEMA.TABLES 
                where  TABLE_SCHEMA ='{schema_name}' and TABLE_NAME in ('{table_name_single}') """)
            
            query_ddl_log = (f"""
            select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, replace(replace(replace(replace(replace(replace(replace
            (replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace
            (replace(replace(replace(ddl,'`',''),'INT64','INT'),'FLOAT64','FLOAT'),'BOOL','BOOLEAN'),'STRUCT',
            'VARIANT'),'PARTITION BY','CLUSTER BY ('),';',');'),'CREATE TABLE ','CREATE TABLE if not exists '),
            "table INT,",'"table" INT,'),'_"table" INT,','_table INT,'),'ARRAY<STRING>','ARRAY'),'from','"from"'),
            '_"from"','_from'),'"from"_','from_'),'DATE(_PARTITIONTIME)','date(loaded_at)'),' OPTIONS(',', //'),'));
            ',');'),'_at);','_at));'),'start ','"start" '),'_"start"','_start'),'order ','"order" '),'<',', //'),
            '_"order"','_order') as DDL from `{project_name}`.`region-US`.INFORMATION_SCHEMA.TABLES 
            where  TABLE_SCHEMA ='{schema_name}' and TABLE_NAME in ('{table_name_single}') """).strip()

            log_multiline_message(logger, """Query for single schema prepared:
                            {}""".format(query_ddl_log))
            
        else :
            query_ddl = (f"""select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace
                    (replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(ddl,'`',''),'INT64','INT'),'FLOAT64','FLOAT'),
                    'BOOL','BOOLEAN'),'STRUCT','VARIANT'),'PARTITION BY','CLUSTER BY ('),';',');'),'CREATE TABLE ','CREATE TABLE if not exists '), "table INT,",
                    '"table" INT,'),'_"table" INT,','_table INT,'),'ARRAY<STRING>','ARRAY'),'from','"from"'),'_"from"','_from'),'"from"_','from_'),
                    'DATE(_PARTITIONTIME)','date(loaded_at)'),' OPTIONS(',', //'),'));',');'),'_at);','_at));'),'start ','"start" '),'_"start"','_start'),
                    'order ','"order" '),'<',', //'),'_"order"','_order') as ddl from `{project_name}`.`region-US`.INFORMATION_SCHEMA.TABLES 
                    where  TABLE_SCHEMA = ' {schema_name}' and TABLE_NAME in {table_tuple} """)
            
            query_ddl_log = (f"""
                    select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, replace(replace(replace(replace(replace(replace
                    (replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace
                    (replace(replace(replace(replace(replace(ddl,'`',''),'INT64','INT'),'FLOAT64','FLOAT'),'BOOL',
                    'BOOLEAN'),'STRUCT','VARIANT'),'PARTITION BY','CLUSTER BY ('),';',');'),'CREATE TABLE ',
                    'CREATE TABLE if not exists '), "table INT,",'"table" INT,'),'_"table" INT,','_table INT,'),
                    'ARRAY<STRING>','ARRAY'),'from','"from"'),'_"from"','_from'),'"from"_','from_'),'DATE(_PARTITIONTIME)',
                    'date(loaded_at)'),' OPTIONS(',', //'),'));',');'),'_at);','_at));'),'start ','"start" '),'_"start"',
                    '_start'),'order ','"order" '),'<',', //'),'_"order"','_order') as ddl 
                    from `{project_name}`.`region-US`.INFORMATION_SCHEMA.TABLES 
                    where  TABLE_SCHEMA = ' {schema_name}' and TABLE_NAME in {table_tuple} """)
        
        log_multiline_message(logger, """Query for multiple schemas prepared:
                            {}""".format(query_ddl_log))
        
        print(query_ddl)
        logger.info("Executing SQL query...")
        query_job = bq_client.query(query_ddl)
        results_ddl_St_db = query_job.result()
        logger.info("Query executed and results fetched.")

        schema_list_name_2 = [field.name for field in results_ddl_St_db.schema]
        log_multiline_message(logger, """Column names retrieved from results: 
                        {}""".format(schema_list_name_2))
        
        dataframe_ddl_table_info = pd.DataFrame(data=[list(row.values()) for row in results_ddl_St_db ], columns = schema_list_name_2)
        logger.info("DataFrame created from query results.")
        print("1 frame")
        print(dataframe_ddl_table_info)
        

        if len(table_tuple) < 2:
            query_copy_dol=(f"""
                    select  c.TABLE_CATALOG, c.TABLE_SCHEMA , c.TABLE_NAME,  string_agg('$1:'||c.column_name) as TABLE_COLUMNS
                    FROM `{project_name}`.`region-US`.INFORMATION_SCHEMA.TABLES as t join
                    `{project_name}`.`region-US`.INFORMATION_SCHEMA.COLUMNS as c on c.TABLE_NAME = t.TABLE_NAME
                    where c.TABLE_SCHEMA = '{schema_name}' and C.TABLE_NAME in ('{table_name_single}') group by c.TABLE_CATALOG,
                    c.TABLE_NAME, c.TABLE_SCHEMA, t.ddl ;""").strip()
            
            log_multiline_message(logger, """Query for single schema prepared: 
                        {}""".format(query_copy_dol))
        else:
            query_copy_dol=(f"""
                    select  c.TABLE_CATALOG, c.TABLE_SCHEMA , c.TABLE_NAME,  string_agg('$1:'||c.column_name) as TABLE_COLUMNS
                    FROM `{project_name}`.`region-US`.INFORMATION_SCHEMA.TABLES as t join
                    `{project_name}`.`region-US`.INFORMATION_SCHEMA.COLUMNS as c on c.TABLE_NAME = t.TABLE_NAME
                    where c.TABLE_SCHEMA = '{schema_name}' and C.TABLE_NAME in {table_tuple}  group by c.TABLE_CATALOG,
                    c.TABLE_NAME, c.TABLE_SCHEMA, t.ddl;""")
            log_multiline_message(logger, """Query for multiple schemas prepared:
                        {}""".format(query_copy_dol))
        
        logger.info("Executing SQL query...")
        query_job = bq_client.query(query_copy_dol)
        try:
            results_copy_dol = query_job.result()
            logger.info("Query executed successfully and results fetched.")
        except Exception as e:
            log_system_message(logger, """An error occurred while executing the query: 
                            {}""".format(e))
            return e

        schema_3 = [field.name for field in results_copy_dol.schema]
        log_multiline_message(logger, """Column names retrieved from results: 
                        {}""".format(schema_3))

        # ----------------------------------------Create DataFrame with both column names and data-----------------------------------------------------------
        dataframe_copy_dol = pd.DataFrame(data=[list(row.values()) for row in results_copy_dol], columns = schema_3)
        print(dataframe_copy_dol)


        try:
            logger.info("Merging dataframes...")
            result_ddl_ed_table = pd.merge(dataframe_schema_table_info, dataframe_ddl_table_info, how = "outer", on = ["TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME"])
            result_ddl_ed_table = pd.merge(result_ddl_ed_table, dataframe_copy_dol, how = "outer", on=["TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME"])
            table_struct = pd.concat([table_struct, result_ddl_ed_table] , ignore_index = True)
            print(result_ddl_ed_table)
            
            logger.info("Dataframes merged successfully.")
            # logger.info("Writing merged dataframe to Snowflake...")
            # write_pandas(snowflake_connection_config,result_ddl_ed_table,'META_TABLES_STRUCT_SOURCE',database=database,schema=schema, auto_create_table=True,overwrite=True,table_type="transient")
            # logger.info("Data written to Snowflake successfully.")
            print(result_ddl_ed_table)
        except Exception as e:
            log_system_message(logger, """Failed to Merge Dataframes: 
                        {}""".format(e))
            return e
            
        if len(table_tuple)<2:
            query = (f"""select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, DATA_TYPE
                    from `{project_name}`.`region-US`.INFORMATION_SCHEMA.COLUMNS
                    where TABLE_SCHEMA = '{schema_name}' and TABLE_NAME in ('{table_name_single}') ;""").strip()
        else:
            query = (f"""select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, DATA_TYPE
                    from `{project_name}`.`region-US`.INFORMATION_SCHEMA.COLUMNS
                    where TABLE_SCHEMA = '{schema_name}' and TABLE_NAME in {table_tuple} ;""").strip()

        query_job = bq_client.query(query)
        results_column_lst = query_job.result()
        log_multiline_message(logger, """Executing query to retrieve column list: 
                            {}""".format(query))
        schema_4 = [field.name for field in results_column_lst.schema]
        dataframe_column_info = pd.DataFrame(data = [list(row.values()) for row in results_column_lst], columns = schema_4)
        columns_struct = pd.concat([columns_struct, dataframe_column_info] , ignore_index = True)
        
    try:
        logger.info("Writing column information to Snowflake...")
        write_pandas(snowflake_connection_config,columns_struct, 'META_COLUMNS_STRUCT_SOURCE', database = database, schema = schema, auto_create_table = True, overwrite = True, table_type = "transient")
        write_pandas(snowflake_connection_config,table_struct, 'META_TABLES_STRUCT_SOURCE', database = database, schema = schema, auto_create_table = True, overwrite = True, table_type = "transient")
        logger.info("Column information written to Snowflake successfully.")
    except Exception as e:
        log_system_message(logger, """An error occurred while executing or processing the query:
                    {}""".format(e))
        return e


def streamlit(database, schema):
    stream_script = ("""
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import matplotlib

from snowflake.snowpark.context import get_active_session


st.set_page_config(layout="wide")
st.header("Migration Report")
st.button(":arrows_counterclockwise:", type="primary")
st.divider()
tab1,tab3,tab4 = st.tabs(["Table Loaded","Table Structure","incremental Load"])
session = get_active_session()
time=session.sql('select getdate();').collect()
backgroundColor="#FFFFFF"
def Table_report():
    session.write_pandas(df_table_Source,f'Migration_Report_Schema_/time/',database="{database}",schema="{schema}", auto_create_table=True,overwrite=True,table_type="transient")

with tab1:
    col1,col2,col3=st.columns([1,2,0.5])
    with col1:
        def Schema_report():
            session.write_pandas(df_schema,f'Migration_Report_Schema_/time/',database="{database}",schema="{schema}", auto_create_table=True,overwrite=True,table_type="transient")
        st.write("Schema  Availability:")
        st.button(":inbox_tray:",help="download Schema Report",on_click=Schema_report)
        dataframe=session.sql('select distinct(table_schema) as "Schema Available in BigQuery"  from {database}.{schema}.META_TABLES_STRUCT_SOURCE')
        df_schema=dataframe.to_pandas()
        df_schema.insert(1,"Schema Loaded in snowflake",'❌')
        list_schema_target=session.sql("select SCHEMA_NAME,CREATED from {database}.INFORMATION_SCHEMA.SCHEMATA;").collect()
        count_Schema=len(df_schema)
        count_load_Schema =0
        count_unloade_schema=0
        schema_list=df_schema["Schema Available in BigQuery"].tolist()
        df_schema.insert(2,"Schema Create Date",np.nan)
        for i in range (len(df_schema)):
            schema_source=df_schema.iloc[i][0]
            upper_schema_source=schema_source.upper()
            for len_schema_target in range (len(list_schema_target)):
                if(list_schema_target[len_schema_target]['SCHEMA_NAME'] == upper_schema_source):
                    df_schema.iloc[i,1]='✅' 
                    df_schema.iloc[i,2]=list_schema_target[len_schema_target]['CREATED']
                    count_load_Schema+=1
                    schema_list.remove(schema_source)
                    break
        st.table(df_schema)
       
        count_unloade_schema=count_Schema-count_load_Schema
        
        st.caption(f"Total Number of Schema: /count_Schema/")
        y = np.array([count_load_Schema,count_unloade_schema])
        mylabels = [ count_load_Schema,count_unloade_schema]
        lables_info = ["No of successful create schema", "No of unsuccessful create schema " ]
        myexplode = [0, 0.1]
        mycolors = ["#ACEC6B", "#FF7777"]
        plt.pie(y, labels = mylabels, explode = myexplode,startangle = 90,colors = mycolors)
        plt.legend(title = "Schema Status:",labels=lables_info )
        a=plt.show() 
        st.set_option('deprecation.showPyplotGlobalUse', False)
        st.pyplot(a)
        if(count_Schema==count_load_Schema):
            st.success('All schema have been successfully created.', icon="✅")
        else:
            st.write("Unavailable Schema In Snowflake:")
            df = pd.DataFrame(schema_list, columns =['Schema Name'])
            st.dataframe(df)
            st.warning('Some schema are missing. Please verify them.', icon="❌")
       
      
            
    with col2:
        st.write("Table  Availability:")
        dataframe_table_source=session.sql('select distinct table_schema as "Table Schema On Source",table_name as "Table On Source" ,case when total_rows is null then 0 else total_rows end as "Table Row Count in BigQuery" from {database}.{schema}.META_TABLES_STRUCT_SOURCE;')
        st.button(":inbox_tray:",help="download Table Report",on_click=Table_report)
        df_table_Source=dataframe_table_source.to_pandas()
        dataframe_table_target=session.sql("select TABLE_SCHEMA,TABLE_NAME,ROW_COUNT,CREATED from {database}.INFORMATION_SCHEMA.TABLES where Table_schema !='INFORMATION_SCHEMA';") 
        df_table_Target=dataframe_table_target.to_pandas()
        df_table_Source.insert(3,"Table Create Date",np.nan)
        df_table_Source.insert(4,"Table Create in snowflake",'❌')
        df_table_Source.insert(5,"Loaded Row Count in snowflake",0)
        df_table_Source.insert(6," Compare Loaded RowCount in snowflake and BigQuery ",'❌')
        count_table_source=len(df_table_Source)
        count_table_target=0
        count_table_row_Target=0
        table_list=df_table_Source["Table On Source"].tolist()
        table_load_list=df_table_Source["Table On Source"].tolist()
       
        for len_table in range(count_table_source):
            table_source=df_table_Source.iloc[len_table][1]
            upper_table_source=table_source.upper()
            for len_table_target in range(len(df_table_Target)):
                if(df_table_Target.iloc[len_table_target]["TABLE_NAME"]==upper_table_source):
                    df_table_Source.iloc[len_table,4]='✅' 
                    df_table_Source.iloc[len_table,3]=df_table_Target.iloc[len_table_target]["CREATED"]
                    count_table_target +=1
                    table_list.remove(table_source)
                    snowflake_count=df_table_Target.iloc[len_table_target]["ROW_COUNT"]
                    bigquery_count=df_table_Source.iloc[len_table]["Table Row Count in BigQuery"]
                    if(snowflake_count==bigquery_count):
                         df_table_Source.iloc[len_table,5]=df_table_Target.iloc[len_table_target]["ROW_COUNT"]
                         df_table_Source.iloc[len_table,6]='✅'
                         count_table_row_Target +=1
                         table_load_list.remove(table_source)
                    else:
                        df_table_Source.iloc[len_table,5]=df_table_Target.iloc[len_table_target]["ROW_COUNT"]
        st.table(df_table_Source)
        
        unloaded_table_count=count_table_source-count_table_target
        
    with col3:
        st.write("Table loaded overview")
        st.caption(f"Total Number of Tables In Big Query : /count_Schema/")
        y = np.array([count_table_target,unloaded_table_count])
        mylabels = [count_table_target,unloaded_table_count]
        lables_info = ["No of table Loaded ", "No of table unloaded " ]
        myexplode = [0, 0.1]
        mycolors = ["#ACEC6B", "#FF7777"]
        plt.pie(y, labels = mylabels, explode = myexplode,startangle = 90,colors = mycolors)
        plt.legend(title = "Table Loaded Status:",labels=lables_info )
        a=plt.show() 
        st.set_option('deprecation.showPyplotGlobalUse', False)
        st.pyplot(a)
        if(count_table_source==count_table_target):
            st.success('All table are create successfully In Snowflake.', icon="✅")
        else:
            table_frame= pd.DataFrame(table_list, columns =['Table Name'])
            st.divider()
            st.write('Unavailable table in snowflake:')
            st.dataframe(table_frame)
            st.warning('Some table are missing Please verify them.', icon="❌")
        if(count_table_source==count_table_row_Target):
            st.success('All data has been successfully loaded into the table', icon="✅")
        else:
            st.divider()
            table_load_frame= pd.DataFrame(table_load_list, columns =['Table Name'])
            st.write("Loading Failed Table:")
            st.dataframe(table_load_frame)
            st.warning('Some data did not load into the table. Please verify them.', icon="❌")
    

with tab3:
    def table_overview(schema,table):
        dataframe_table_source=session.sql(f'''select distinct table_schema as "Table Schema On Source",table_name as "Table On Source" ,case when total_rows is null then 0 else total_rows end as "Table Row Count in BigQuery" from {database}.{schema}.META_TABLES_STRUCT_SOURCE where "Table Schema On Source" ='/schema_name/' and "Table On Source" ='/table_name/' ;''')
        df_table_Source=dataframe_table_source.to_pandas()
        dataframe_table_target=session.sql("select TABLE_SCHEMA,TABLE_NAME,ROW_COUNT,CREATED from {database}.INFORMATION_SCHEMA.TABLES where Table_schema !='INFORMATION_SCHEMA';") 
        df_table_Target=dataframe_table_target.to_pandas()
        df_table_Source.insert(3,"Table Create Date",np.nan)
        df_table_Source.insert(4,"Table Create in snowflake",'❌')
        df_table_Source.insert(5,"Loaded Row Count in snowflake",0)
        df_table_Source.insert(6," Compare Loaded RowCount in snowflake and BigQuery ",'❌')
        count_table_source=len(df_table_Source)
        count_table_target=0
        count_table_row_Target=0
        table_list=df_table_Source["Table On Source"].tolist()
        table_load_list=df_table_Source["Table On Source"].tolist()
       
        for len_table in range(count_table_source):
            table_source=df_table_Source.iloc[len_table][1]
            upper_table_source=table_source.upper()
            for len_table_target in range(len(df_table_Target)):
                if(df_table_Target.iloc[len_table_target]["TABLE_NAME"]==upper_table_source):
                    df_table_Source.iloc[len_table,4]='✅' 
                    df_table_Source.iloc[len_table,3]=df_table_Target.iloc[len_table_target]["CREATED"]
                    count_table_target +=1
                    table_list.remove(table_source)
                    snowflake_count=df_table_Target.iloc[len_table_target]["ROW_COUNT"]
                    bigquery_count=df_table_Source.iloc[len_table]["Table Row Count in BigQuery"]
                    if(snowflake_count==bigquery_count):
                         df_table_Source.iloc[len_table,5]=df_table_Target.iloc[len_table_target]["ROW_COUNT"]
                         df_table_Source.iloc[len_table,6]='✅'
                         count_table_row_Target +=1
                         table_load_list.remove(table_source)
                    else:
                        df_table_Source.iloc[len_table,5]=df_table_Target.iloc[len_table_target]["ROW_COUNT"]
        st.table(df_table_Source)
    table_input_struct=st.form('table Struct')
    
    schema_list=session.sql('select distinct table_schema from {database}.{schema}.META_TABLES_STRUCT_SOURCE;').collect()
    def Table_Struct():
        session.write_pandas(source_table,f'Migration_Report_Table_Struct_/time/',database="{database}",schema="PUBLIC", auto_create_table=True,overwrite=True,table_type="transient")        
    schema_list=session.sql('select distinct table_schema from {database}.{schema}.META_TABLES_STRUCT_SOURCE;').collect()
    schema_name=table_input_struct.selectbox('Bigquery Schema List',schema_list,help='select Schema need to view',key="schema_name")
    Table_SQL=('''select distinct table_name from {database}.{schema}.META_TABLES_STRUCT_SOURCE where table_schema='/schema_name/';''').format(schema_name=schema_name)
    Table_List =session.sql(Table_SQL).collect()
    table_name=table_input_struct.selectbox('Bigquery Table List',Table_List,help='select Table need to view')
    
    submit=table_input_struct.form_submit_button("Fetch Details")
    if submit:
        st.write("Table overview:")
        table_overview(schema_name,table_name)
        col1,col2=st.columns([1,0.2])
        
        source_table_sql=(f'''select column_name as  "Column Available In Source",data_type as  "Data type In BigQuery"  from {database}.{schema}.META_COLUMNS_STRUCT_SOURCE where table_schema='/schema_name/' and table_name='/table_name/'  ;''').format(schema_name=schema_name,table_name=table_name)
        source_table=session.sql(source_table_sql)
        source_table=source_table.to_pandas()
        source_table.insert(2,"Column Available In Snowflake",'❌')
        source_table.insert(3,"Column Type In Snowflake",'❌')
        source_table.insert(4,"Column Type Available On Target","INT")
        # source_table.insert(4,"Load Row Count On Target",0)
        len_column=len(source_table)
        Len_column_count_check=0
        column_list=source_table["Column Available In Source"].tolist()
        
    
        for len_column_count in range (len_column):
            column_name_capital=source_table.iloc[len_column_count]["Column Available In Source"]
            column_name=column_name_capital.upper()
            sql_target_column=("select column_name,data_type from {database}.INFORMATION_SCHEMA.COLUMNS where  table_schema=upper('/schema_name/') and table_name=upper('/table_name/' ) ;").format(schema_name=schema_name,table_name=table_name)
            target_table_column=session.sql(sql_target_column)
            target_table_column=target_table_column.to_pandas()
            if(len(target_table_column)<1):
                break
            for len_column_count_target in range(len(target_table_column)):
                name=target_table_column.iloc[len_column_count_target]['COLUMN_NAME']
                if(column_name==target_table_column.iloc[len_column_count_target]['COLUMN_NAME']):
                    column_list.remove(column_name_capital)
                    source_table.iloc[len_column_count,2]='✅'
                    Len_column_count_check +=1
                    source_table.iloc[len_column_count,4]=target_table_column.iloc[len_column_count_target]['DATA_TYPE']
                    if(source_table.iloc[len_column_count]["Data type In BigQuery"]==('STRING')):
                        if(target_table_column.iloc[len_column_count_target]['DATA_TYPE']==("TEXT")):
                            source_table.iloc[len_column_count,3]='✅'
                    if(source_table.iloc[len_column_count]["Data type In BigQuery"]==('INT64')):
                        if(target_table_column.iloc[len_column_count_target]['DATA_TYPE']==("INT") or ("NUMBER")):
                            source_table.iloc[len_column_count,3]='✅'
                    if(source_table.iloc[len_column_count]["Data type In BigQuery"]==('FLOAT64')):
                        if(target_table_column.iloc[len_column_count_target]['DATA_TYPE']==("FLOAT4") or ("FLOAT")):
                            source_table.iloc[len_column_count,3]='✅'
                    if(source_table.iloc[len_column_count]["Data type In BigQuery"]==('TIMESTAMP')):
                        if(target_table_column.iloc[len_column_count_target]['DATA_TYPE']==("TIMESTAMP") or ("TIMESTAMP_LTZ")):
                            source_table.iloc[len_column_count,3]='✅'
                    if(source_table.iloc[len_column_count]["Data type In BigQuery"]==('DATE')):
                        if(target_table_column.iloc[len_column_count_target]['DATA_TYPE']==("DATE") ):
                            source_table.iloc[len_column_count,3]='✅'
                    if(source_table.iloc[len_column_count]["Data type In BigQuery"]==('DATETIME')):
                        if(target_table_column.iloc[len_column_count_target]['DATA_TYPE']==("TIMESTAMP_NTZ") ):
                            source_table.iloc[len_column_count,3]='✅'
                    break
        if(len(target_table_column)<1):
            st.error("No Table Found On Snowflake") 
            st.exception("No Table Found On Snowflake")
        else:
            with col1:
                st.write(" Column Structure:")
                st.dataframe(source_table)
                st.button(":inbox_tray:",help="download Tabel Struct Report",on_click=Table_Struct)
                st.write("Initial Load File:")
                capital_schema_name=schema_name.upper()
                capital_table_name=table_name.upper()
                sql_inital_load_file=("select FILE_NAME,last_load_time,status,row_count,row_parsed  from {database}.{schema}.LOAD_HISTORY where schema_name=upper('/schema_name/')  and table_name=upper('/table_name/' ) ;").format(schema_name=schema_name,table_name=table_name)
                inital_load_frame=session.sql(sql_inital_load_file)
                inital_load_frame=inital_load_frame.to_pandas()
                st.dataframe(inital_load_frame)
            unloaded_column=len_column-Len_column_count_check
            Table_Struct = np.array([Len_column_count_check,unloaded_column])
            Table_Column_count = [Len_column_count_check,unloaded_column]
            lables_info = ["No of column Loaded", "No of Column Unloaded " ]
            myexplode = [0, 0.1]
            mycolors = ["#ACEC6B", "#FF7777"]
            plt.pie(Table_Struct, labels = Table_Column_count, explode = myexplode,startangle = 90,colors = mycolors)
            plt.legend(title = "Table Column Status:",labels=lables_info )
            a=plt.show() 
            st.set_option('deprecation.showPyplotGlobalUse', False)
            with col2:
                st.pyplot(a)
                if(len_column==Len_column_count_check):
                    st.success('All Column are Available In Table.', icon="✅")   
                else:
                    st.warning('Column are missing Please verify them.', icon="❌")
                    st.write("unavailable column in table:")
                    column_list= pd.DataFrame(column_list, columns =['Column Name'])
                    st.table(column_list)
with tab4:
    incremental_input_struct=st.form('incremental Struct')
    schema_list=session.sql('select distinct table_schema from {database}.{schema}.META_TABLES_STRUCT_SOURCE;').collect()
    schema_name=incremental_input_struct.selectbox('Bigquery Schema List',schema_list,help='select Schema need to view')
    Table_SQL=('''select distinct table_name from {database}.{schema}.META_TABLES_STRUCT_SOURCE where table_schema='/schema_name/';''').format(schema_name=schema_name)
    Table_List =session.sql(Table_SQL).collect()
    table_name=incremental_input_struct.selectbox('Bigquery Table List',Table_List,help='select Table need to view',)
    submit=incremental_input_struct.form_submit_button("Fetch Details")
""".format(database=database, schema=schema))
    stream_script=stream_script.replace("/count_table_source/","{count_table_source}")
    stream_script=stream_script.replace("Migration_Report_Schema_/time/","Migration_Report_Schema_{time[0][0]}")
    stream_script=stream_script.replace("/count_Schema/","{count_Schema}")
    stream_script=stream_script.replace("/schema_name/","{schema_name}")
    stream_script=stream_script.replace("/table_name/","{table_name}")     
    results = stream_script
    # print(results)
    text_file_path = r'C:\Users\Swetha\Desktop\streamlit\streamlit.py'
    with open(text_file_path, 'w', encoding='utf-8') as text_file:
        text_file.write(results)

@app.route('/increment', methods=['POST'])
def incremental():
    return render_template('incremental_form.html')


if __name__ == '__main__':
    app.run(debug=True, port=8000)



    