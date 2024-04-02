import numpy as np
import snowflake.connector

def streamlit(conn,database,schema):
    connection_cursor=conn.cursor()
    connection_cursor.execute(f"create  stage if not exists {database}.{schema}.Migration_RP;")
    print(database,schema)
    print(f"put file://D:\snowmigrate_tool\streamlit\streamlit.py @{database}.{schema}.Migration_RP AUTO_COMPRESS=FALSE")
    a=(f"put file://D:\snowmigrate_tool\streamlit\environment.yml  @{database}.{schema}.Migration_RP AUTO_COMPRESS=FALSE")
    connection_cursor.execute(a)
    connection_cursor.execute(f"put file://D:\snowmigrate_tool\streamlit\streamlit.py @{database}.{schema}.Migration_RP AUTO_COMPRESS=FALSE")
    connection_cursor.execute(f"create or replace  STREAMLIT {database}.{schema}.Migration_Report ROOT_LOCATION='@{database}.{schema}.MIGRATION_RP' MAIN_FILE = '/streamlit.py', QUERY_WAREHOUSE = SNOW_MIGRATE_WAREHOUSE ;" )
