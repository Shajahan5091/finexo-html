
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
    session.write_pandas(df_table_Source,f'Migration_Report_Schema_{time[0][0]}',database="SNOW_MIGRATE_DATABASE",schema="SNOW_MIGRATE_SCHEMA", auto_create_table=True,overwrite=True,table_type="transient")

with tab1:
    col1,col2,col3=st.columns([1,2,0.5])
    with col1:
        def Schema_report():
            session.write_pandas(df_schema,f'Migration_Report_Schema_{time[0][0]}',database="SNOW_MIGRATE_DATABASE",schema="SNOW_MIGRATE_SCHEMA", auto_create_table=True,overwrite=True,table_type="transient")
        st.write("Schema  Availability:")
        st.button(":inbox_tray:",help="download Schema Report",on_click=Schema_report)
        dataframe=session.sql('select distinct("table_schema") as "Schema Available in BigQuery"  from SNOW_MIGRATE_DATABASE.SNOW_MIGRATE_SCHEMA.META_TABLES_STRUCT_SOURCE')
        df_schema=dataframe.to_pandas()
        df_schema.insert(1,"Schema Loaded in snowflake",'❌')
        list_schema_target=session.sql("select SCHEMA_NAME,CREATED from SNOW_MIGRATE_DATABASE.INFORMATION_SCHEMA.SCHEMATA;").collect()
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
        
        st.caption(f"Total Number of Schema: {count_Schema}")
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
        dataframe_table_source=session.sql('select distinct "table_schema"as "Table Schema On Source","table_name" as "Table On Source" ,case when "total_rows" is null then 0 else "total_rows" end as "Table Row Count in BigQuery" from SNOW_MIGRATE_DATABASE.SNOW_MIGRATE_SCHEMA.META_TABLES_STRUCT_SOURCE;')
        st.button(":inbox_tray:",help="download Table Report",on_click=Table_report)
        df_table_Source=dataframe_table_source.to_pandas()
        dataframe_table_target=session.sql("select TABLE_SCHEMA,TABLE_NAME,ROW_COUNT,CREATED from SNOW_MIGRATE_DATABASE.INFORMATION_SCHEMA.TABLES where Table_schema !='INFORMATION_SCHEMA';") 
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
        st.caption(f"Total Number of Tables In Big Query : {count_Schema}")
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
        dataframe_table_source=session.sql(f'''select distinct "table_schema"as "Table Schema On Source","table_name" as "Table On Source" ,case when "total_rows" is null then 0 else "total_rows" end as "Table Row Count in BigQuery" from SNOW_MIGRATE_DATABASE.SNOW_MIGRATE_SCHEMA.META_TABLES_STRUCT_SOURCE where "Table Schema On Source" ='{schema_name}' and "Table On Source" ='{table_name}' ;''')
        df_table_Source=dataframe_table_source.to_pandas()
        dataframe_table_target=session.sql("select TABLE_SCHEMA,TABLE_NAME,ROW_COUNT,CREATED from SNOW_MIGRATE_DATABASE.INFORMATION_SCHEMA.TABLES where Table_schema !='INFORMATION_SCHEMA';") 
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
    
    schema_list=session.sql('select distinct "table_schema" from SNOW_MIGRATE_DATABASE.SNOW_MIGRATE_SCHEMA.META_TABLES_STRUCT_SOURCE;').collect()
    def Table_Struct():
        session.write_pandas(source_table,f'Migration_Report_Table_Struct_/time/',database="SNOW_MIGRATE_DATABASE",schema="PUBLIC", auto_create_table=True,overwrite=True,table_type="transient")        
    schema_list=session.sql('select distinct "table_schema" from SNOW_MIGRATE_DATABASE.SNOW_MIGRATE_SCHEMA.META_TABLES_STRUCT_SOURCE;').collect()
    schema_name=table_input_struct.selectbox('Bigquery Schema List',schema_list,help='select Schema need to view',key="schema_name")
    Table_SQL=('''select distinct "table_name" from SNOW_MIGRATE_DATABASE.SNOW_MIGRATE_SCHEMA.META_TABLES_STRUCT_SOURCE where "table_schema"='{schema_name}';''').format(schema_name=schema_name)
    Table_List =session.sql(Table_SQL).collect()
    table_name=table_input_struct.selectbox('Bigquery Table List',Table_List,help='select Table need to view')
    
    submit=table_input_struct.form_submit_button("Fetch Details")
    if submit:
        st.write("Table overview:")
        table_overview(schema_name,table_name)
        col1,col2=st.columns([1,0.2])
        
        source_table_sql=(f'''select "column_name" as  "Column Available In Source","data_type"as  "Data type In BigQuery"  from SNOW_MIGRATE_DATABASE.SNOW_MIGRATE_SCHEMA.META_COLUMNS_STRUCT_SOURCE where "table_schema"='{schema_name}' and "table_name"='{table_name}'  ;''').format(schema_name=schema_name,table_name=table_name)
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
            sql_target_column=("select column_name,data_type from SNOW_MIGRATE_DATABASE.INFORMATION_SCHEMA.COLUMNS where  table_schema=upper('{schema_name}') and table_name=upper('{table_name}' ) ;").format(schema_name=schema_name,table_name=table_name)
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
                sql_inital_load_file=("select FILE_NAME,last_load_time,status,row_count,row_parsed  from SNOW_MIGRATE_DATABASE.SNOW_MIGRATE_SCHEMA.LOAD_HISTORY where schema_name=upper('{schema_name}')  and table_name=upper('{table_name}' ) ;").format(schema_name=schema_name,table_name=table_name)
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
                    st.success('All Column are Avaiable In Table.', icon="✅")   
                else:
                    st.warning('Column are missing Please verify them.', icon="❌")
                    st.write("unavailable column in table:")
                    column_list= pd.DataFrame(column_list, columns =['Column Name'])
                    st.table(column_list)
with tab4:
    incremental_input_struct=st.form('incremental Struct')
    schema_list=session.sql('select distinct "table_schema" from SNOW_MIGRATE_DATABASE.SNOW_MIGRATE_SCHEMA.META_TABLES_STRUCT_SOURCE;').collect()
    schema_name=incremental_input_struct.selectbox('Bigquery Schema List',schema_list,help='select Schema need to view')
    Table_SQL=('''select distinct "table_name" from SNOW_MIGRATE_DATABASE.SNOW_MIGRATE_SCHEMA.META_TABLES_STRUCT_SOURCE where "table_schema"='{schema_name}';''').format(schema_name=schema_name)
    Table_List =session.sql(Table_SQL).collect()
    table_name=incremental_input_struct.selectbox('Bigquery Table List',Table_List,help='select Table need to view',)
    submit=incremental_input_struct.form_submit_button("Fetch Details")
