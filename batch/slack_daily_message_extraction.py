import sys
import os
import time
from typing import List, Dict
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import pandas as pd

from core.bigquery import BigqueryProcessor
from core.slack import SlackMessageRetriever
from core.config import config


class MessageExtractor:
    def __init__(self) -> None:
        self.bigquery_client = BigqueryProcessor(
            env_name="GOOGLE_APPLICATION_CREDENTIALS", database_id="geultto_9th"
        )
        self.slack_app = SlackMessageRetriever(env_name="SLACK_TOKEN")

    def update_users(
        self, table_name: str = "users", if_exists: str = "replace"
    ) -> None:
        users = self.slack_app.read_users_from_slack()
        user_list = []
        for user in users:
            user_list.append(SlackMessageRetriever.convert_users_to_dict(user=user))
        user_df = pd.DataFrame(user_list)
        self.bigquery_client.update_table(
            df=user_df,
            table_name=table_name,
            if_exists=if_exists,
            schema=config.SCHEMA_USERS,
        )

    def update_channels(
        self, table_name: str = "channels", if_exists: str = "replace"
    ) -> None:
        channels = self.slack_app.read_channels_from_slack()
        channel_list = []
        for channel in channels:
            channel_list.append(
                SlackMessageRetriever.convert_channels_to_dict(channel=channel)
            )
        channel_df = pd.DataFrame(channel_list)
        self.bigquery_client.update_table(
            df=channel_df,
            table_name=table_name,
            if_exists=if_exists,
            schema=config.SCHEMA_CHANNELS,
        )

    def get_conversations_list(self, sdatetime: datetime) -> List[Dict]:
        sdatetime_minus1 = sdatetime + relativedelta(days=-1)
        start_unixtime = time.mktime((sdatetime_minus1).timetuple())
        end_unixtime = time.mktime(sdatetime.timetuple()) - 1e-6  # 23:59:59 99999 까지

        channel_id_list = (
            self.bigquery_client.read_table(table_name="channels")
            .loc[:, "channel_id"]
            .tolist()
        )
        message_list = []
        for channel_id in channel_id_list:
            posts = self.slack_app.read_post_from_slack(
                start_unixtime=start_unixtime,
                end_unixtime=end_unixtime,
                channel_id=channel_id,
            )
            message_list.extend(
                self.slack_app.fetch_and_process_posts(
                    channel_id=channel_id, posts=posts
                )
            )
            time.sleep(1)
        print(sdatetime_minus1)
        return message_list

    def update_conversations(self, message_list: List[Dict]):
        temp_df = self.slack_app.convert_message_to_dataframe(message_list=message_list)
        self.bigquery_client.update_table(
            df=temp_df,
            table_name="temp_upsert_table",
            if_exists="replace",
            schema=config.SCHEMA_CONVERSATION,
        )
        self.bigquery_client.upsert_table()
