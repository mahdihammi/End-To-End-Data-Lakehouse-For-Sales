from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from dotenv import load_dotenv 
from datetime import datetime
import logging

load_dotenv()
import os



def attach_ducklake_and_set_secrets(DBNAME,SUPABASE_HOST,SUPABASE_PORT,SUPABASE_USER,SUPABASE_PWD,MINIO_ENDPOINT
                                    ,MINIO_ACCESS_KEY,MINIO_SECRET_KEY, conn, secret_name):
    """
        This function is for purpose of attaching the ducklake instance
    """


    try :
        exists = conn.execute(f"""
            SELECT COUNT(*) 
            FROM duckdb_secrets()
            WHERE name = '{secret_name}'
        """).fetchone()[0] > 0

        if not exists:
            conn.execute(f"""
                    
                    INSTALL ducklake ;
                    INSTALL postgres;
                
                    CREATE OR REPLACE PERSISTENT SECRET(
                        TYPE postgres,
                        HOST '{SUPABASE_HOST}',
                        PORT '{SUPABASE_PORT}',
                        DATABASE 'postgres',
                        USER '{SUPABASE_USER}',
                        PASSWORD '{SUPABASE_PWD}'
                        )
            """)

            logging.info("Creating secret successfully")
        else:
             logging.info("secret already exist")

        conn.execute(f'''
                    INSTALL httpfs;
                    LOAD httpfs;
                    SET s3_endpoint='{MINIO_ENDPOINT}';
                    SET s3_access_key_id='{MINIO_ACCESS_KEY}';
                    SET s3_secret_access_key='{MINIO_SECRET_KEY}';
                    SET s3_use_ssl=false;
                    SET s3_url_style='path';
            ''')

        conn.execute(
                    f"""
                    ATTACH IF NOT EXISTS 'ducklake:postgres:dbname=postgres' AS mahdi_ducklake (DATA_PATH 's3://lakehouse-project/')
                    """
                )
        logging.info("Success , the ducklake has been attached succesfully")
           
            
    except Exception as e:
            logging.error(f"Cannot attach the ducklake instance {e} ")

