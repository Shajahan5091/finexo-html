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
    print(results)
    text_file_path = 'D:\finexo-html\streamlit\streamlit.py'
    with open(text_file_path, 'w', encoding='utf-8') as text_file:
        text_file.write(stream_script)

streamlit('SNOW_MIGRATE_DATABASE','SNOW_MIGRATE_SCHEMA')