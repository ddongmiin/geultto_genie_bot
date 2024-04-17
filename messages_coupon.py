import os
from core.slack import SlackMessageRetriever
from core.bigquery import BigqueryProcessor


slack_app = SlackMessageRetriever(env_name="SLACK_TOKEN_BOT")
bigquery_client = BigqueryProcessor(
    env_name="GOOGLE_APPLICATION_CREDENTIALS", database_id="geultto_9th"
)

user_pd = bigquery_client.run_query_to_dataframe(
    query="""
    SELECT DISTINCT ifnull(E.userid, F.user_id) AS userid, D.*
    FROM (
    SELECT A.*
        , ifnull(B.code, '') AS code1
        , ifnull(C.code, '') AS code2
    FROM (
        SELECT *
        FROM `geultto.geultto_9th.udemy_lecture_real_final` 
        WHERE name is not null
    ) A
    LEFT JOIN (
    SELECT *
    FROM `geultto.geultto_9th.lecture_code_v2`
    WHERE round = 3
    ) B
    ON A.first like B.lecture
    LEFT JOIN (
    SELECT * 
    FROM `geultto.geultto_9th.lecture_code_v2`
    WHERE round = 3
    ) C
    ON A.second like C.lecture
    ) D
    LEFT JOIN `geultto.geultto_9th.user_db_master` E
    ON D.name = E.name
    LEFT JOIN `geultto.geultto_9th.users` F
    ON D.name = F.real_name
    WHERE code1 != '' OR code2 != ''
    """
)

# user_list = user_pd["user_id"].tolist()

# user_list = ["U0666UTKF8W", "U066AQSBRPF"]

# text = """데달부가 000님의 유데미 쿠폰번호를 배달해드립니다.:pencil:
# 000님이 신청하신 강의와 쿠폰번호는 아래와 같습니다

# 강의명: 쿠폰번호
# 강의명: 쿠폰번호

# 쿠폰 사용 방법은 아래 링크를 참조해주세요!
# <사용 링크>

# TDD로 배우는 웹 프론트엔드
# 프로젝트로 배우는 React.js & Next.js 마스터리 클래스
# 시스템 소프트웨어 개발을 위한 Arm 아키텍처의 구조와 원리 1부- 저자직강
# 시스템 소프트웨어 개발을 위한 Arm 아키텍처의 구조와 원리 2부- 저자직강
# Flutter Redux Essential 코스 (Korean)
# ---------

# 위 5개의 강의 쿠폰이 추가 발급되어
# 데달부가 해당 강의를 신청하신 {row[1]}님의 유데미 쿠폰번호를 배달해드립니다.:pencil:
# {row[1]}님이 신청하신 강의와 쿠폰번호는 아래와 같습니다.

# 첫 번째 강의:
# 강의명 - {row[2]}
# 쿠폰코드 - {row[4]}

# 두 번째 강의:
# 강의명 - {row[3]}
# 쿠폰코드 - {row[5]}

# :point_right::point_right: 꼭 읽어주세요!! :point_left::point_left:
# ㅡ 전달받으신 링크의 유효기간은 12/18일 까지입니다. 그 안에 꼭 신청해주셔야 합니다.
# ㅡ 쿠폰 코드를 클릭하시면 강의료에 "무료"라고 적혀져 있는 것을 확인하실 수 있습니다.
# ㅡ 해당 링크 안에서 수강신청 해주시면 됩니다.
# ㅡ 본인이 어떤 강의를 신청하신건지 헷갈리신다면 아래 링크에서 확인해주세요.
# https://docs.google.com/spreadsheets/d/1sVdozuaYX89cn5n0PxyY6aPbGMZQz2cdXjpsCw4vU5w/edit?usp=sharing
# ㅡ 잘못 기입되어 있는 것 같다면, 0_글또운영질문 채널에 꼭 문의를 남겨주세요
# ㅡ 본 강의는 유데미의 지원을 받아 무료로 제공해드리는 강의입니다. 꼭 수강하시고 후기를 남겨주셔야 합니다. 나중에 후기도 모두 체크할 예정입니다.

# Jump into the AI World - AI Product Lifecycle
# 위 강의의 쿠폰 코드는 12/18에 보내드릴 예정입니다.

# 그럼 {row[1]}님의 완강을 기원하면서 이만 물러가보겠습니다 :man-bowing:

# !!!쿠폰은 꼭 12/00일까지 사용해주셔야 합니다!!!

# 그럼 000님의 완강을 기원하면서 이만 물러가보겠습니다 :man-bowing:
# """

# slack_app.message_for_private(users=user_list, text=text)

for _, row in user_pd.iterrows():
    text = f"""안녕하세요 Jump into the AI World - AI Product Lifecycle 강의 쿠폰 코드가 발급되어
    해당 강의를 신청하신 분들게 강의 쿠폰 코드를 발급드립니다.

    {row[1]}님이 신청하신 강의와 쿠폰번호는 아래와 같습니다.

    첫 번째 강의:
    강의명 - {row[2]}
    쿠폰코드 - {row[4]}

    두 번째 강의:
    강의명 - {row[3]}
    쿠폰코드 - {row[5]}

    :point_right::point_right: 꼭 읽어주세요!! :point_left::point_left:
    ㅡ 전달받으신 링크의 유효기간은 12/22일 까지입니다. 그 안에 꼭 신청해주셔야 합니다.
    ㅡ 쿠폰 코드를 클릭하시면 강의료에 "무료"라고 적혀져 있는 것을 확인하실 수 있습니다.
    ㅡ 해당 링크 안에서 수강신청 해주시면 됩니다.
    ㅡ 본인이 어떤 강의를 신청하신건지 헷갈리신다면 아래 링크에서 확인해주세요.
    https://docs.google.com/spreadsheets/d/1sVdozuaYX89cn5n0PxyY6aPbGMZQz2cdXjpsCw4vU5w/edit?usp=sharing
    ㅡ 잘못 기입되어 있는 것 같다면, 0_글또운영질문 채널에 꼭 문의를 남겨주세요
    ㅡ 본 강의는 유데미의 지원을 받아 무료로 제공해드리는 강의입니다. 꼭 수강하시고 후기를 남겨주셔야 합니다. 나중에 후기도 모두 체크할 예정입니다.

    그럼 {row[1]}님의 완강을 기원하면서 이만 물러가보겠습니다 :man-bowing:
    """
    print(row[1])
    slack_app.message_for_private(user=row[0], text=text)
