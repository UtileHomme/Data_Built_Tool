# https://www.youtube.com/watch?v=PNK49SJvXjE&list=PLba2xJ7yxHB73xHFsyu0YViu3Hi6Ckxzj&index=10

# https://data-engineering-simplified.medium.com/continuous-data-loading-in-snowflake-bec729ec0e53

# https://github.com/snowflakedb/snowflake-ingest-python/blob/master/README.rst

from logging import getLogger
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile
from snowflake.ingest.utils.uris import DEFAULT_SCHEME
from datetime import timedelta
from requests import HTTPError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.hazmat.primitives.serialization import PrivateFormat
from cryptography.hazmat.primitives.serialization import NoEncryption
import time
import datetime
import os
import logging

paraphrase = "Votreami@123"


# logging.basicConfig(
        # filename=r"C:\Users\jsharma029\OneDrive - pwc\Data_Built_Tool\Snow_Flake_DataEngineering_Simplied_Beginner\ingest.log",
        # level=logging.DEBUG)
    
logging.basicConfig(    
filename=r"C:\Users\pksk6\OneDrive_Latest\OneDrive\Study_Related\Data_Build_Tool(DBT)_and_Snowflake\Snow_Flake_DataEngineering_Simplied_Beginner\ingest.log",
        level=logging.DEBUG)
logger = getLogger(__name__)

# https://streamsets.com/blog/snowflake-key-pair-authentication/
with open(r"C:\Users\pksk6\OneDrive_Latest\OneDrive\Study_Related\Data_Build_Tool(DBT)_and_Snowflake\rsa_key.p8", 'rb') as pem_in:
  pemlines = pem_in.read()
  private_key_obj = load_pem_private_key(pemlines,
  paraphrase.encode(),
  default_backend())


private_key_text = private_key_obj.private_bytes(
  Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()).decode('utf-8')

file_list=[r"my_customer_data.csv",r"my_customer_data_2.csv"]
ingest_manager = SimpleIngestManager(account='QFOMQCZ-DVB99010',
                                     host='QFOMQCZ-DVB99010.snowflakecomputing.com',
                                     user='UTILEHOMME123',
                                     pipe='MY_DB.CH10.my_pipe_10',
                                     private_key=private_key_text)
staged_file_list = []
for file_name in file_list:
    staged_file_list.append(StagedFile(file_name, None))

try:
    resp = ingest_manager.ingest_files(staged_file_list)
except HTTPError as e:
    logger.error(e)
    exit(1)

print("Section: Assert")
assert(resp['responseCode'] == 'SUCCESS')

while True:
    history_resp = ingest_manager.get_history()

    if len(history_resp['files']) > 0:
        print('Ingest Report:\n')
        print(history_resp)
        break
    else:
        # wait for 20 seconds
        time.sleep(20)

    hour = timedelta(hours=1)
    date = datetime.datetime.utcnow() - hour
    history_range_resp = ingest_manager.get_history_range(date.isoformat() + 'Z')

    print('\nHistory scan report: \n')
    print(history_range_resp)  