import os
from typing import List, Dict
import json
from datetime import datetime

from google.cloud import bigquery
from google.oauth2 import service_account

import pandas as pd
from pandas_gbq import to_gbq


class BigqueryProcessor:
    """
    빅쿼리 데이터 적재에 필요한 기능들을 정리했습니다.
    """

    def __init__(self, env_name: str, database_id: str):
        self.credentials = service_account.Credentials.from_service_account_file(
            os.environ.get(env_name)
        )
        self.project_id = self.credentials.project_id
        self.database_id = database_id
        self.client = bigquery.Client(credentials=self.credentials, project=self.project_id)

    def create_table(
        self, table_name: str, schema: List, partition: bool = False, partition_key: str = None
    ) -> None:
        table_path = f"{self.project_id}.{self.database_id}.{table_name}"
        table = bigquery.Table(table_path, schema=schema)

        if partition is True:
            partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_key,  # 분할하려는 필드
            )
            table.time_partitioning = partitioning

        self.client.create_table(table)
        print(f"{table_path}가 정상적으로 생성 됐습니다.")

    def read_table(
        self,
        table_name: str,
        where_clause: str = None,
    ) -> pd.DataFrame:
        table_path = f"{self.project_id}.{self.database_id}.{table_name}"
        table = self.client.get_table(table_path)

        if not table.time_partitioning:
            qr = f"""
            SELECT *
            FROM `{table_path}`
            """
        else:
            if not where_clause:
                raise ValueError("where_caluse에 파티션 조건절을 추가하세요")
            else:
                qr = f"""
                SELECT *
                FROM `{table_path}`
                WHERE {where_clause}                
                """

        return self.client.query(qr).to_dataframe()

    def update_table(
        self, df: pd.DataFrame, table_name: str, if_exists: str, schema: List[Dict]
    ) -> None:
        table_path = f"{self.project_id}.{self.database_id}.{table_name}"
        to_gbq(
            dataframe=df,
            destination_table=table_path,
            credentials=self.credentials,
            if_exists=if_exists,  # fail, replace, append
            table_schema=schema,
        )
        print(f"{table_path}가 정상적으로 적재 됐습니다.")

    def delete_table(self, table_name: str, where_clause: str) -> None:
        table_path = f"{self.project_id}.{self.database_id}.{table_name}"
        qr = f"""
        DELETE FROM `{table_path}`
        {where_clause}
        """
        query_job = self.client.query(qr)
        query_job.result()

        print(f"{table_path}의 {where_clause}가 정상적으로 삭제 됐습니다.")

    @staticmethod
    def read_schema(file_path: str) -> List[Dict]:
        with open(file_path, "r", encoding="utf-8") as json_file:
            data = json.load(json_file)
        return data
