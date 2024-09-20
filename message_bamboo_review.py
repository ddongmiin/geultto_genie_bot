import os
from core.slack import SlackMessageRetriever
from core.bigquery import BigqueryProcessor


slack_app = SlackMessageRetriever(env_name="SLACK_TOKEN_BOT")
bigquery_client = BigqueryProcessor(
    env_name="GOOGLE_APPLICATION_CREDENTIALS", database_id="geultto_9th"
)

user_pd = bigquery_client.run_query_to_dataframe(
    query="""
    (
    SELECT '이모지수' AS group_name
        , permalink
        , SUM(CAST(JSON_EXTRACT_SCALAR(reaction, '$.count') AS INT64)) AS total_cnt
    FROM `geultto.geultto_9th.slack_conversation_master`
        , UNNEST(JSON_EXTRACT_ARRAY(reactions)) AS reaction
    WHERE message_type = 'post'
        AND channel_id = '' -- bamboo 채널id
        AND tddate >= '2024-03-04' 
        AND tddate <= '2024-03-17'
    GROUP BY text, permalink
    ORDER BY total_cnt DESC
    LIMIT 10
    )
    UNION ALL
    (
    SELECT '댓글수' AS group_name
        , permalink
        , total_cnt
    FROM (
        SELECT post_id
            , permalink
            , message_type
            , COUNT(*) OVER(PARTITION BY post_id) AS total_cnt
        FROM `geultto.geultto_9th.slack_conversation_master`
        WHERE channel_id = '' -- bamboo 채널id
        AND tddate >= '2024-03-04' 
        AND tddate <= '2024-03-17'  
        ) A
    WHERE message_type = 'post'
    ORDER BY total_cnt DESC
    LIMIT 10
    )
    """
)


def process_row(row):
    emoji_cnt = f"{row['group_name']}: {row['total_cnt']}"

    return f"""게시글링크: {row["permalink"]}
{emoji_cnt}"""


slack_messages = [process_row(row) for index, row in user_pd.iterrows()]
slack_message = "\n\n".join(slack_messages)

# slack_app.message_for_private(users=user_list, text=text)
text = f""":point_right::point_right: 데달부가 공유해드리는 !!대숲 인기글!! :point_left::point_left: 
종윤님 체크해주세요 :pray:

{slack_message}
"""
channel_id = ""
slack_app.message_for_channel(channel_id=channel_id, text=text)
