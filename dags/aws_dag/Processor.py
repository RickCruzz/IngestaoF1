import pandas as pd
import datetime
import os
from os import chdir,listdir, getcwd, getenv
from os.path import isfile, isdir

import logging
from abc import ABC

from dotenv import load_dotenv
import boto3
from botocore import exceptions
from botocore.exceptions import ClientError



logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
load_dotenv('/opt/airflow/cfgs/.env')


class AWS_Processor(ABC):
    def __init__(self, caminho:str, bNome: str) -> None:
        self.filename = f"{datetime.datetime.now()}/"
        self.bNome = bNome
        self.s3_client = boto3.client(
            'athena',
            region_name='us-east-1',
            aws_access_key_id = getenv('AWS_ID'),
            aws_secret_access_key = getenv('AWS_KEY')
            )
        