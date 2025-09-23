from snowflake.snowpark import Session
import logging
import os
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(override=True)

# Snowflake connection parameters
connection_params = {
    "account": os.getenv("SNOW_ACCOUNT"),
    "user": os.getenv("SNOW_USER"),
    "password": os.getenv("SNOW_PASS"),
    "role": os.getenv("SNOW_ROLE"),
    "warehouse": os.getenv("SNOW_WH"),
    "database": os.getenv("SNOW_DB"),
    "schema": os.getenv("SNOW_SCHEMA"),
    "client_session_keep_alive": "false",
}
def singleton(class_instance):
    instances = {}

    def get_instance(*args, **kwargs):
        if class_instance not in instances:
            instances[class_instance] = class_instance(*args, **kwargs)
        return instances[class_instance]

    return get_instance

@singleton
class SnowConnect:
    def __init__(self) -> None:
        try:
            session = Session.builder.configs(connection_params).create()
            self.__session = session
        except Exception as e:
            logging.info(e)

    def getsession(self):
        return self.__session

    def delInstance(self):
        del self

def main():
    try:
        # Initialize connection
        snow_conn = SnowConnect()
        session = snow_conn.getsession()
        # Run a test query
        df = session.sql("SELECT pi();").collect()
        print(type(df))
        print("Connected successfully! Current Role:", df[0][0])

    except Exception as e:
        print(f"Error during connection test: {e}")

if __name__ == "__main__":
    print(connection_params)
    main()