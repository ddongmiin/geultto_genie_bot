import os
from core.slack import SlackMessageRetriever
from core.bigquery import BigqueryProcessor


slack_app = SlackMessageRetriever(env_name="SLACK_TOKEN_BOT")
bigquery_client = BigqueryProcessor(
    env_name="GOOGLE_APPLICATION_CREDENTIALS", database_id="geultto_9th"
)

user_pd = bigquery_client.run_query_to_dataframe(
    query="""
    SELECT B.userid, A.name
    FROM `geultto.geultto_9th.username_writinglecture_season1` A
    LEFT JOIN `geultto.geultto_9th.user_db_master` B
    ON A.name = B.name
    WHERE B.userid IS NOT NULL
    """
)

for _, row in user_pd.iterrows():
    text = f"""안녕하세요~ 다들 주말 잘 보내고 계신가요! :smile:
    설문을 남겨주신 분들께 데달부가 글쓰기 세미나 리마인드 드립니다 :pencil:
    내일 1/14(일) 오후 9시 온라인 글쓰기 세미나가 있습니다!

    세미나는 정성들여 남겨주신 설문을 바탕으로 진행됩니다!

    그럼 주말 푹쉬시고 내일 뵙도록 하겠습니다 :smile:
    """
    print(row[1])
    slack_app.message_for_private(user=row[0], text=text)
