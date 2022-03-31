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


class AWS_Ingestor(ABC):
    def __init__(self, caminho:str, bNome: str) -> None:
        self.filename = f"{datetime.datetime.now()}/"
        self.bNome = bNome
        self.s3_client = boto3.client(
            's3',
            region_name='us-east-1',
            aws_access_key_id = getenv('AWS_ID'),
            aws_secret_access_key = getenv('AWS_KEY')
            )
        self.s3_resource = boto3.resource(
            's3',
            region_name='us-east-1',
            aws_access_key_id = getenv('AWS_ID'),
            aws_secret_access_key = getenv('AWS_KEY')
            )
        self.Ignite(caminho, bNome)
  
    def Flash(self, caminho: str):
        caminhodir= f"{getcwd()}/executado_{self.filename}"
        f = open(caminho, "r")
        os.makedirs(os.path.dirname(caminhodir), exist_ok=True)
        caminhodir+=caminho
        s = open(caminhodir, "x")
        for x in f:
            s.write(x)
                
        
    def Cleanse(self, caminho:str):
        os.remove(caminho)


    def Ignite(self, caminho: str, bNome: str) -> None:
        if isdir(caminho):
            print(caminho)
            chdir(caminho)
        for c in listdir():
            if isfile(c):
                print(c)
                self.Burn(c)
                self.Cleanse(c)


    def Burn(self, arquivo: str) -> None:
        data_proc=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        file_name = arquivo
        index = file_name.index('.')
        file_name = file_name[:index]
        self.s3_resource.Bucket(self.bNome).upload_file(
            arquivo,
            f"f1/data/{file_name}/extracted_at={data_proc}/{arquivo}"
        )
                

class AWS_Firestarter():
    def firestarter(*op_args):
        print(*op_args)
        caminho = op_args[1]
        bNome = op_args[2]
        AWS_Ingestor(caminho, bNome)

