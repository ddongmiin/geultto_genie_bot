import sys
import os
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd

# 모듈 참조 경로 추가
sys.path.append("/Users/cho/git/geultto_genie_bot")

from module.bigquery import BigqueryProcessor
from module.slack import SlackMessageRetriever
from module.date_operation import get_daily_datelist


# 인스턴스 생성
bigquery_client = BigqueryProcessor(
    env_name="GOOGLE_APPLICATION_CREDENTIALS", database_id="geultto_8th"
)
slack_app = SlackMessageRetriever(env_name="SLACK_TOKEN")

# 스키마 불러오기
schema_users = bigquery_client.read_schema(file_path="schema/users.json")
schema_channels = bigquery_client.read_schema(file_path="schema/channels.json")
schema_conversation = bigquery_client.read_schema(file_path="schema/slack_conversation.json")

# 유저 리스트 적재
users = slack_app.read_users_from_slack()
user_list = []
for user in users:
    user_list.append(SlackMessageRetriever.convert_users_to_dict(user=user))
user_df = pd.DataFrame(user_list)
bigquery_client.update_table(
    df=user_df, table_name="users", if_exists="replace", schema=schema_users
)

# 채널 리스트 적재
channels = slack_app.read_channels_from_slack()
channel_list = []
for channel in channels:
    channel_list.append(SlackMessageRetriever.convert_channels_to_dict(channel=channel))
channel_df = pd.DataFrame(channel_list)
bigquery_client.update_table(
    df=channel_df, table_name="channels", if_exists="replace", schema=schema_channels
)

channel_id_list = channel_df["channel_id"].tolist()

# 대화내용 적재
start_date = sys.argv[1]
end_date = sys.argv[2]
d_range = get_daily_datelist(start_date=start_date, end_date=end_date)
begin_unixtime = time.mktime((d_range[0] + relativedelta(days=-1)).timetuple())
last_unixtime = time.mktime((d_range[-1]).timetuple())

for sdatetime in d_range:
    sdatetime_minus1 = sdatetime + relativedelta(days=-1)
    sdate = sdatetime_minus1.strftime("%Y-%m-%d")
    start_unixtime = time.mktime((sdatetime_minus1).timetuple())
    end_unixtime = time.mktime(sdatetime.timetuple()) - 1e-6  # 23:59:59 99999 까지
    print(
        f"{datetime.fromtimestamp(start_unixtime)} ~ {datetime.fromtimestamp(end_unixtime)} 기간의 데이터 처리를 시작합니다."
    )
    all_message_list = []
    for channel_id in channel_id_list:
        posts = slack_app.read_post_from_slack(
            channel_id=channel_id,
            start_unixtime=start_unixtime,
            end_unixtime=end_unixtime,
        )
        message_list = slack_app.fetch_and_process_posts(
            channel_id=channel_id,
            posts=posts,
            start_unixtime=begin_unixtime,
            end_unixtime=last_unixtime,
        )
        all_message_list.extend(message_list)
        time.sleep(1.5)
    message_df = slack_app.convert_message_to_dataframe(message_list=all_message_list)
    if message_df is not None:
        where_clause = f"""where tddate = '{sdate}'"""
        bigquery_client.delete_table(table_name="slack_conversation", where_clause=where_clause)
        bigquery_client.update_table(
            df=message_df,
            table_name="slack_conversation",
            if_exists="append",
            schema=schema_conversation,
        )
        print(
            f"{datetime.fromtimestamp(start_unixtime)} ~ {datetime.fromtimestamp(end_unixtime)} 기간의 데이터 처리를 완료했습니다."
        )
    else:
        print(
            f"{datetime.fromtimestamp(start_unixtime)} ~ {datetime.fromtimestamp(end_unixtime)} 기간의 데이터가 비어있습니다.."
        )
