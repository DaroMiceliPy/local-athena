from sqlalchemy import text
from sqlalchemy.engine import create_engine
from io import BytesIO
from datetime import date, datetime
import boto3
import botocore
import pandas as pd



mock_presto_engine = create_engine('presto://localhost:8080/hive')
mock_hive_engine = create_engine('hive://localhost:10000/default')
mock_s3_client = boto3.client("s3", region_name="us-east-1", endpoint_url="http://localhost:5000")


class Athena:

    def read(self, **kwargs):
        fields = ', '.join([f'"{column}"' for column in self._schema.keys()])
        limit = kwargs.get('limit', '')
        query = kwargs.get('query', '')

        if query == '':
            limit = f"limit {limit}" if limit != '' else ''
            query = f"""SELECT { fields } FROM { self._settings['table'] } {limit}"""

        # 3. Create table
        self.df = pd.read_sql_query(query, mock_presto_engine)

    def save(self, **kwargs):
        buffer = BytesIO()
        self.df.to_parquet(buffer, engine = "pyarrow", compression='snappy')
        buffer.seek(0)
        mock_s3_client.put_object(
                Bucket=self.url.get('bucket'),
                Key=f"{self.url.get('path')}/{self.url.get('filename')}",
                Body=buffer.read(),
        )
        if self._settings.get("partitions", False):
            url = f"s3a://{self.url.get('bucket')}/{self.url.get('path')}"
            partitions = ''
            for partition in self._settings['partitions']:
                partitions += f'{partition} = "{self.filters[partition]}",'
            partitions = partitions[:-1]
            query = f"""ALTER TABLE {self._settings['table']} ADD IF NOT EXISTS
                PARTITION ({partitions}) LOCATION "{url}"
            """
            with mock_hive_engine.connect() as connection:
                connection.execute(text(query))

    def create_table(self, **kwargs):
        partitions = ''
        for partition in self._settings['partitions']:
            partition_data_type = ''
            if type(self.filters[partition]) == str:
                partition_data_type = "string"
            if type(self.filters[partition]) == float:
                partition_data_type = "double" 
            if type(self.filters[partition]) == int:
                partition_data_type = "int"
            if type(self.filters[partition]) == date:
                partition_data_type = "date"
            if type(self.filters[partition]) == datetime:
                partition_data_type = "timestamp"
            partitions += f'{partition} {partition_data_type},'
        partitions = partitions[:-1]
        columns = ", ".join([f'{key} {self._schema[key]["dtype"]}' for key in self._schema.keys()])
        columns = columns.replace("datetime", "timestamp")
        with mock_hive_engine.connect() as connection:
            connection.execute(text(f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS { self._settings['table'] }({columns})
                PARTITIONED BY ({partitions})
                STORED AS PARQUET LOCATION 's3a://{self.url.get('bucket')}/{self._settings['table']}'
            """))