import os
from io import BytesIO
from fastapi import FastAPI, UploadFile
from azure import identity
from azure.storage.blob import BlobServiceClient
import pyodbc, struct
import csv
from fastavro import reader, writer
from datetime import datetime
import pandas as pd

#Get connection string for Azure SQL and Storage Account
connection_string = os.environ.get('AZURE_SQL_CONNECTIONSTRING')
storage_connection_string = os.environ.get('AZURE_STORAGE_CONNECTIONSTRING')
blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)



app = FastAPI()

@app.get("/loadfiles")
async def load_files_from_blob():
    output = ''

    try:
        with get_conn() as conn:
            cursor = conn.cursor()

            #Ingest departments
            cursor.execute(f"TRUNCATE TABLE [STAGE].[Departments];")
            sql = "BULK INSERT [STAGE].[Departments] FROM 'sources/sourcefiles/departments.csv' WITH ( DATA_SOURCE = 'SourceAzureBlobStorage', FORMAT = 'CSV', CODEPAGE = 65001, FIRSTROW = 1, ROWTERMINATOR = '0x0a', BATCHSIZE = 1000, ERRORFILE = 'sources/loaderrors/departments_load_errors_{}', ERRORFILE_DATA_SOURCE = 'SourceAzureBlobStorage', MAXERRORS = 999, TABLOCK);"
            now = datetime.now()
            formatted_datetime = now.strftime("%Y-%m-%d %H:%M:%S")
            new_sql = sql.format(formatted_datetime)
            cursor.execute(new_sql)
            conn.commit()

            #Ingest jobs
            cursor.execute(f"TRUNCATE TABLE [STAGE].[Jobs];")
            sql = "BULK INSERT [STAGE].[Jobs] FROM 'sources/sourcefiles/jobs.csv' WITH ( DATA_SOURCE = 'SourceAzureBlobStorage', FORMAT = 'CSV', CODEPAGE = 65001, FIRSTROW = 1, ROWTERMINATOR = '0x0a', BATCHSIZE = 1000, ERRORFILE = 'sources/loaderrors/jobs_load_errors_{}', ERRORFILE_DATA_SOURCE = 'SourceAzureBlobStorage', MAXERRORS = 999, TABLOCK);"
            now = datetime.now()
            formatted_datetime = now.strftime("%Y-%m-%d %H:%M:%S")
            new_sql = sql.format(formatted_datetime)
            cursor.execute(new_sql)
            conn.commit()

            #Ingest hired employees
            cursor.execute(f"TRUNCATE TABLE [STAGE].[HiredEmployees];")
            sql = "BULK INSERT [STAGE].[HiredEmployees] FROM 'sources/sourcefiles/hired_employees.csv' WITH ( DATA_SOURCE = 'SourceAzureBlobStorage', FORMAT = 'CSV', CODEPAGE = 65001, FIRSTROW = 1, ROWTERMINATOR = '0x0a', BATCHSIZE = 1000, ERRORFILE = 'sources/loaderrors/hired_employees_load_errors_{}', ERRORFILE_DATA_SOURCE = 'SourceAzureBlobStorage', MAXERRORS = 999, TABLOCK);"
            now = datetime.now()
            formatted_datetime = now.strftime("%Y-%m-%d %H:%M:%S")
            new_sql = sql.format(formatted_datetime)
            cursor.execute(new_sql)
            conn.commit()

            cursor.execute(f"EXEC [STAGE].[USP_LOAD_DATA_TO_FINAL_TABLES];")
            conn.commit()

        output = {"message": "Successful data load"}
        
    except Exception as e:
        print(e)
        output = {"message": f""+str(e)+""}

    return output



@app.get("/backupdata")
async def backup_data_to_avro():
    
    #Delete local files if existing
    if os.path.exists("backup_jobs.avro"):
        os.remove("backup_jobs.avro")
    if os.path.exists("backup_departments.avro"):
        os.remove("backup_departments.avro")
    if os.path.exists("backup_hiredemployees.avro"):
        os.remove("backup_hiredemployees.avro")
    
    #Declare avro schema for tables
    jobs_avro_schema = {
          "type": "record",
          "name": "MyRecord",
          "fields": [
               {"name": "id", "type": "int"},
               {"name": "job", "type": ["null","string"]}
          ]
    }
    departments_avro_schema = {
          'type': 'record',
          'name': 'MyRecord',
          'fields': [
               {'name': 'id', 'type': 'int'},
               {'name': 'department', 'type': ["null","string"]}
          ]
    }
    hiredemployees_avro_schema = {
          'type': 'record',
          'name': 'MyRecord',
          'fields': [
               {'name': 'id', 'type': 'int'},
               {'name': 'name', 'type': ["null","string"]},
               {'name': 'datetime', 'type': ["null","string"]},
               {'name': 'department_id', 'type': 'int'},
               {'name': 'job_id', 'type': 'int'}
          ]
    }

    #########################
    ###### Backup Jobs ######
    #########################
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM [dbo].[Jobs]")

        results = cursor.fetchall()
        
        with open('backup_jobs.avro', 'ab+') as avro_file:
            for row in results:
                data = {
                    "id": row[0],
                    "job": row[1]
                }

                writer(avro_file, jobs_avro_schema, [data])

        
        with open("backup_jobs.avro", "rb") as data:
            blob_client = blob_service_client.get_blob_client(container="sources",blob="backup/backup_jobs.avro")
            blob_client.upload_blob(data, overwrite=True)

        print("Jobs backup file uploaded to blob")

        ################################
        ###### Backup Departments ######
        ################################
        cursor.execute("SELECT * FROM [dbo].[Departments]")
        results = cursor.fetchall()
            
        with open('backup_departments.avro', 'ab+') as avro_file:
            for row in results:
                data = {
                    "id": row[0],
                    "department": row[1]
                }

                writer(avro_file, departments_avro_schema, [data])

        
        with open("backup_departments.avro", "rb") as data:
            blob_client = blob_service_client.get_blob_client(container="sources",blob="backup/backup_departments.avro")
            blob_client.upload_blob(data, overwrite=True)

        print("Departments backup file uploaded to blob")

        ####################################
        ###### Backup Hired Employees ######
        ####################################
        cursor.execute("SELECT * FROM [dbo].[HiredEmployees]")
        results = cursor.fetchall()
            
        with open('backup_hiredemployees.avro', 'ab+') as avro_file:
            for row in results:
                data = {
                    "id": row[0],
                    "name": row[1],
                    "datetime": row[2],
                    "department_id": row[3],
                    "job_id": row[4]
                }

                writer(avro_file, hiredemployees_avro_schema, [data])

        
        with open("backup_hiredemployees.avro", "rb") as data:
            blob_client = blob_service_client.get_blob_client(container="sources",blob="backup/backup_hiredemployees.avro")
            blob_client.upload_blob(data, overwrite=True)

        print("Hired employees backup file uploaded to blob")

        cursor.close()
        conn.close()  

        return {"message": "Tables were backed up successfully"}


@app.post("/restore/{target_table}")
async def restore_table_from_backup(target_table: str):
    
    #Run only if target table is jobs, departments, hired employees
    if target_table.upper() == "JOBS" or target_table.upper() == "DEPARTMENTS" or target_table.upper() == "HIREDEMPLOYEES":
        #Set blob_name
        if target_table.upper() == "JOBS":
            blob_name = "backup/backup_jobs.avro"
            # os.remove("backup_jobs.csv")
        if target_table.upper() == "DEPARTMENTS":
            blob_name = "backup/backup_departments.avro"
            # os.remove("backup_departments.csv")
        if target_table.upper() == "HIREDEMPLOYEES":
            blob_name = "backup/backup_hiredemployees.avro"
            # os.remove("backup_hiredemployees.csv")
        
        try:
            # Get blob client
            blob_client = blob_service_client.get_blob_client(container="sources",blob=blob_name)
            
            # Download blob content into memory
            avro_bytes = BytesIO()
            download_stream = blob_client.download_blob()
            avro_bytes.write(download_stream.readall())
            avro_bytes.seek(0)

            # Read Avro and convert to pandas DataFrame
            records = []
            avro_reader = reader(avro_bytes)
            for record in avro_reader:
                records.append(record)

            df = pd.DataFrame(records)
            df.replace(r'\r', '', regex=True, inplace=True)
            
            # Convert to CSV in-memory
            csv_buffer = BytesIO()
            df.to_csv(csv_buffer, index=False, header=False, lineterminator="")
            csv_buffer.seek(0)

            # Upload CSV to Blob Storage
            blob_client = blob_service_client.get_blob_client("sources",blob="backup/backup_"+target_table.upper()+".csv")
            blob_client.upload_blob(csv_buffer, overwrite=True)

            # Truncate and insert to target table
            with get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(f"TRUNCATE TABLE [dbo].["+target_table.upper()+"];")
                sql = "BULK INSERT [dbo].["+target_table.upper()+"] FROM 'sources/backup/backup_"+target_table.upper()+".csv' WITH ( DATA_SOURCE = 'SourceAzureBlobStorage', FORMAT = 'CSV', CODEPAGE = 65001, FIRSTROW = 1, ROWTERMINATOR = '0x0a', BATCHSIZE = 1000, ERRORFILE = 'sources/loaderrors/"+target_table.upper()+"_load_errors_{}', ERRORFILE_DATA_SOURCE = 'SourceAzureBlobStorage', MAXERRORS = 999, TABLOCK);"
                now = datetime.now()
                formatted_datetime = now.strftime("%Y-%m-%d %H:%M:%S")
                new_sql = sql.format(formatted_datetime)
                cursor.execute(new_sql)
                conn.commit()


            return {"message": f"Table " + target_table.upper() + " was successfully restored from backup"}
        
        except Exception as e:
            print(e)
    else:
        return {"message": "No valid table to restore provided"}


def get_conn():
    credential = identity.DefaultAzureCredential(exclude_interactive_browser_credential = False)
    token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    SQL_COPT_SS_ACCESS_TOKEN = 1256   # This connection option is defined by microsoft in msodbcsql.h
    conn = pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
    return conn
