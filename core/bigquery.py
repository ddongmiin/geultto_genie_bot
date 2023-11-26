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
        self.client = bigquery.Client(
            credentials=self.credentials, project=self.project_id
        )

    def create_table(
        self,
        table_name: str,
        schema: List,
        partition: bool = False,
        partition_key: str = None,
    ) -> None:
        """
        파이썬에서 빅쿼리 테이블을 생성합니다.

        Parameters
        ----------
        table_name : str
            테이블 이름
        schema : List
            스키마 구조
        partition : bool, optional
            파티션 키를 만들것인지 체크, by default False
        partition_key : str, optional
            파티션 키를 만든다면 어떤 컬럼을 사용할 것인지 명시, by default None
        """
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

    def run_query_to_dataframe(self, query: str) -> pd.DataFrame:
        """
        빅쿼리에 직접 쿼리를 날립니다

        Parameters
        ----------

        """
        return self.client.query(query=query).result().to_dataframe()

    def read_table(self, table_name: str, where_clause: str = None) -> pd.DataFrame:
        """
        빅쿼리 테이블을 판다스로 읽어옵니다.

        Parameters
        ----------
        table_name : str
            테이블명
        where_clause : str
            조건절을 입력합니다. 만약 파티션 키가 있는데 조건절을 입력하지 않으면 에러가 발생합니다, by default None
        """
        table_path = f"{self.project_id}.{self.database_id}.{table_name}"
        table = self.client.get_table(table_path)

        if not table.time_partitioning:
            qr = f"""
            SELECT *
            FROM `{table_path}`
            """
        else:
            if not where_clause:
                raise ValueError("where_clause 파티션 조건절을 추가하세요")
            qr = f"""
                SELECT *
                FROM `{table_path}`
                WHERE {where_clause}                
                """

        return self.client.query(qr).to_dataframe()

    def update_table(
        self, df: pd.DataFrame, table_name: str, if_exists: str, schema: List[Dict]
    ) -> None:
        """
        파이썬에서 빅쿼리 테이블을 업데이트 합니다.

        Parameters
        ----------
        df : pd.DataFrame
            판다스 데이터프레임
        table_name : str
            테이블명
        if_exists : str
            테이블이 만약에 존재한다면 어떤 조건을 사용할 것인지 3가지 사용가능 - fail, replace, append
        schema : List[Dict]
            스키마
        """
        table_path = f"{self.project_id}.{self.database_id}.{table_name}"
        to_gbq(
            dataframe=df,
            destination_table=table_path,
            credentials=self.credentials,
            if_exists=if_exists,
            table_schema=schema,
        )
        print(f"{table_path}가 정상적으로 적재 됐습니다.")

    def delete_table(self, table_name: str, where_clause: str) -> None:
        """
        파이썬에서 빅쿼리 내에서 특정 조건에 해당하는 데이터를 삭재합니다.
        데이터 업데이트 시 중복 적재를 방지하는 용도로 사용합니다.

        Parameters
        ----------
        table_name : str
            테이블명
        where_clause : str
            삭제 조건을 입력합니다.
        """
        table_path = f"{self.project_id}.{self.database_id}.{table_name}"
        qr = f"""
        DELETE FROM `{table_path}`
        {where_clause}
        """
        query_job = self.client.query(qr)
        query_job.result()

        print(f"{table_path}의 {where_clause}가 정상적으로 삭제 됐습니다.")

    def upsert_table(
        self,
        *,
        target_table: str = "slack_conversation_master",
        source_table: str = "temp_upsert_table",
    ) -> None:
        """
        아래와 같이 값이 존재하는 경우 update, 값이 없는 경우 insert하는 로직을 진행
        MERGE merge_example.table_data T
        USING merge_example.table_changes S
        ON T.id = S.id
        WHEN MATCHED THEN
        UPDATE SET value = s.value
        WHEN NOT MATCHED THEN
        INSERT (id, value) VALUES(id, value)

        Parameters
        ----------
        target_table : str
            변경 대상 테이블명
        source_table : str
            소스 테이블명
        """
        target_path = f"{self.project_id}.{self.database_id}.{target_table}"
        source_path = f"{self.project_id}.{self.database_id}.{source_table}"

        qr = f"""
        MERGE {target_path} T
        USING {source_path} S
        ON T.post_id = S.post_id AND T.createtime = S.createtime
        WHEN MATCHED THEN
        UPDATE SET
            channel_id = S.channel_id,
            message_type = S.message_type,
            user_id = S.user_id,
            createtime = S.createtime,
            tddate = S.tddate,
            text = S.text,
            reactions = S.reactions
        WHEN NOT MATCHED THEN
            INSERT (channel_id, message_type, post_id, user_id, createtime, tddate, text, reactions)
            VALUES (channel_id, message_type, post_id, user_id, createtime, tddate, text, reactions)            
        """

        query_job = self.client.query(qr)
        query_job.result()

        print(f"{target_path}의 upsert가 정상적으로 진행 됐습니다.")

    @staticmethod
    def read_schema(file_path: str) -> List[Dict]:
        """
        스키마를 읽어옵니다.

        Parameters
        ----------
        file_path : str
            json 스키마가 저장된 주소

        Returns
        -------
        List[Dict]
            스키마 파일
        """
        with open(file_path, "r", encoding="utf-8") as json_file:
            data = json.load(json_file)
        return data
