import os
from core.slack import SlackMessageRetriever
from core.bigquery import BigqueryProcessor

# 슬랙 메시지 전송을 위한 인스턴스 생성
slack_app = SlackMessageRetriever(env_name="SLACK_TOKEN_BOT")
# 빅쿼리 쿼리를 실행하기 위한 인스턴스 생성
bigquery_client = BigqueryProcessor(
    env_name="GOOGLE_APPLICATION_CREDENTIALS", database_id="geultto_9th"
)

# 유데미 강의를 수강한 유저들의 정보를 가져옴
user_pd = bigquery_client.run_query_to_dataframe(
    query="""
    SELECT DISTINCT ifnull(E.userid, F.user_id) AS userid, E.name, D.first
         , CASE WHEN D.second = '강의를 하나만 신청하셨습니다.' THEN '' ELSE D.second END AS second
    FROM (
    SELECT A.*
    FROM (
        SELECT *
        FROM `geultto.geultto_9th.udemy_lecture_real_final` 
        WHERE name is not null
      ) A
    ) D
    LEFT JOIN `geultto.geultto_9th.user_db_master` E
    ON D.name = E.name
    LEFT JOIN `geultto.geultto_9th.users` F
    ON D.name = F.real_name    
    """
)

# 유데미 강의를 수강한 유저들에게 메시지 전송
for _, row in user_pd.iterrows():
    text = f"""안녕하세요 {row[1]}님 이전에 유데미 쿠폰을 전달드렸던 데달부입니다. 
오랜만에 인사드리네요..! :blob-wave:

{row[1]}님은 아래 강의를 신청하였어요!
{row[2]} 
{row[3]} 

그간 강의는 잘 수강하셨나요..? 혹시, 아직 못들으셨더라도 걱정하지 마세요..!
글또가 2회나 연장 되었답니다 :cool-doge:
(글 쓰시면 회당 5,000원씩 추가 환급:bangbang:)

유데미 후기로 글도 쓰고 돈도 벌어보시는건 어떨까요!

그럼 {row[1]}님의 강의 후기를 기다리며 데달부는 이만 물러가 보겠습니다 :woman-bowing:
(이 메시지는 유데미 강의를 신청하신 모든 분들께 전달되는 메시지에요 만약에 후기를 작성해주셨다면 이 메시지는 패스해주셔도 됩니다:pray:)
    """
    print(row[1])
    slack_app.message_for_private(user=row[0], text=text)
