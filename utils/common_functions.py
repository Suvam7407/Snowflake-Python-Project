
import snowflake.connector
import os
from  connection_details import  *
connection_parameters={
    "account":account,
    "user":username,
    "password":password,
    'warehouse': warehouse,
    'database': database,
    'schema':schema
}

with snowflake.connector.connect(**connection_parameters) as conn:
    # Create a cursor object
    with conn.cursor() as cur:
        try:
            # Execute a simple query
            #for folder_, t_table in [('\\raw','emp'),]:
                #for file in  os.listdir(folder_name+folder_):
                   # if file.endswith(".sql"):
                        #file_path=os.path.join(folder_name+folder_,file)
                        #with open(file_path,'r') as f:
                            #contents = f.read() 
                        #for sql_command in contents.split(";"):
            cur.execute('''create or replace temporary table temp1
                    as
                    select emp_name, emp_gmail_id from p1
                    union all
                    select emp_name, emp_gmail_id from p2
                    union all
                    select emp_name, emp_gmail_id from p3
                     ''')
            cur.execute('''MERGE INTO emp as t
                USING temp1 as s
                ON t.emp_name=s.emp_name
                WHEN MATCHED THEN
                UPDATE SET 
                t.emp_gmail_id=s.emp_gmail_id
                WHEN NOT MATCHED THEN
                INSERT (t.emp_name,t.emp_gmail_id)
                VALUES (s.emp_name,s.emp_gmail_id)
                ''')

            # Fetch the result 
        except Exception as e:
            print(e)    