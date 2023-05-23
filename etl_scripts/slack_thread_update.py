"""
아직 기능 추가중입니다. 
"""

import sys
import time
from datetime import datetime

# 모듈 참조 경로 추가
sys.path.append("/Users/cho/git/geultto_genie_bot")

from module.bigquery import BigqueryProcessor
from module.slack import SlackMessageRetriever

# 인스턴스 생성
bigquery_client = BigqueryProcessor(
    env_name="GOOGLE_APPLICATION_CREDENTIALS", database_id="geultto_8th"
)
schema_conversation = bigquery_client.read_schema(file_path="schema/slack_conversation.json")
slack_app = SlackMessageRetriever(env_name="SLACK_TOKEN")

# post_id 읽어오기
sdate = datetime.today().strftime("%Y-%m-%d")
where_clause = f"tddate <= '{sdate}'"
df_conversation = bigquery_client.read_table(
    table_name="slack_conversation_copy_lates", where_clause=where_clause
)
# 게시글만 추출
df_conversation = df_conversation.loc[(df_conversation.message_type == "post"),]

# kst -> utc변환 후 쓰레드 API 요청 파라미터로 사용할 unixtime 변수 생성
df_conversation["createtime_minus_9hour"] = (
    df_conversation["createtime"].dt.tz_localize("Asia/Seoul").dt.tz_convert("UTC")
)
# 게시글에 대한 댓글을 요청하기 위해서는 게시글 작성시간을 unixtime으로 넣어줘야 함.
df_conversation["unixtime"] = df_conversation["createtime_minus_9hour"].astype("int64") / 1e9

# 채널리스트
df_channel = bigquery_client.read_table(table_name="channels")
channel_id_list = df_channel["channel_id"].tolist()

for channel_id in channel_id_list:
    print(channel_id)
    thread_list = []
    thread_ts_list = df_conversation.loc[
        df_conversation.channel_id == channel_id, "unixtime"
    ].tolist()
    print(thread_ts_list)
    for thread_ts in thread_ts_list:
        try:
            # 게시글에 대한 쓰레드가 없을 수도 있음
            threads = slack_app.read_thread_from_slack(channel_id=channel_id, thread_ts=thread_ts)
            time.sleep(1)
            for thread in threads:
                thread_list.append(
                    SlackMessageRetriever.convert_thread_to_dict(
                        channel_id=channel_id, thread=thread
                    )
                )
            print(threads)
            print("\n")
        except Exception as e:
            print(e, thread_ts)

    temp_df = SlackMessageRetriever.convert_message_to_dataframe(message_list=thread_list)
    print(type(temp_df))
    if temp_df is not None:
        bigquery_client.update_table(
            df=temp_df,
            table_name="temp_upsert_table",
            if_exists="replace",
            schema=schema_conversation,
        )
        print(4)

        bigquery_client.upsert_table(
            target_table="upsert_test",
            source_table="temp_upsert_table",
            tddate=sdate,
            condition_clause="""
            ON T.post_id = S.post_id
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
            """,
        )
    print(f"{channel_id}의 업데이트가 완료됐습니다.")
