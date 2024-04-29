from dotenv import load_dotenv
from dotenv import dotenv_values
import logging
from logging import getLogger
import snowflake
import snowflake.connector
from snowflake.snowpark import Session
from snowflake.ingest import SimpleIngestManager, StagedFile


config = dotenv_values(".env") 

connection_parameters = {
    "account": 'eyzqllm-cnb81860',
    "user": config['snowflake_user'],
    "password": config['snowflake_password'],
    "role": "ACCOUNTADMIN",
    "database": "MY_DB_10",
    "warehouse": "COMPUTE_WH",
    "schema": "MY_SCHEMA_10",
}

print(connection_parameters)

logging.basicConfig(
        filename='/tmp/ingest.log',
        level=logging.DEBUG)
logger = getLogger(__name__)


session = Session.builder.configs(connection_parameters).create()
ingest_manager = SimpleIngestManager()

file_list = ['data/customer_101.csv']
staged_file_list = []

for file_name in file_list:
    staged_file_list.append(StagedFile(file_name, None))



