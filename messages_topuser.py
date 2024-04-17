import os
from core.slack import SlackMessageRetriever
from core.bigquery import BigqueryProcessor


slack_app = SlackMessageRetriever(env_name="SLACK_TOKEN_BOT")
bigquery_client = BigqueryProcessor(
    env_name="GOOGLE_APPLICATION_CREDENTIALS", database_id="geultto_9th"
)

total_cnt = bigquery_client.run_query_to_dataframe(
    query="""
    SELECT SUM(cnt_emoji) AS total_emoji
        , SUM(cnt_post) AS total_post
        , SUM(cnt_thread) AS total_thread     
    FROM (
    SELECT users.*
        , cnt_emoji 
        , cnt_post
        , cnt_thread
    FROM `geultto.geultto_9th.users` AS users
    LEFT JOIN (
        SELECT '이모지수' AS group_name
            , user_id
            , COUNT(*) cnt_emoji
        FROM (
        SELECT JSON_EXTRACT_SCALAR(json, '$.name') AS name
            , REPLACE(user_id, '"', "") AS user_id
        FROM `geultto.geultto_9th.slack_conversation_master`,
        UNNEST(JSON_EXTRACT_ARRAY(reactions)) AS json,
        UNNEST(JSON_EXTRACT_ARRAY(json, '$.user_id')) AS user_id 
        WHERE tddate <= '2023-12-24'
        ) A
        GROUP BY user_id
    ) emoji_cnt
    ON users.user_id = emoji_cnt.user_id
    LEFT JOIN (
        SELECT user_id
            , COUNT(*) cnt_post
        FROM `geultto.geultto_9th.slack_conversation_master`
        WHERE tddate <= '2023-12-24'
        AND message_type = 'post'
        AND (text not like ('%님이 채널에 참여함%') AND text not like ('%integration to this channel%'))
        GROUP BY user_id
    ) post_cnt
    ON users.user_id = post_cnt.user_id
    LEFT JOIN (
        SELECT user_id
            , COUNT(*) cnt_thread
        FROM `geultto.geultto_9th.slack_conversation_master`
        WHERE tddate <= '2023-12-24'
        AND message_type = 'thread'
        GROUP BY user_id  
    ) thread_cnt
    ON users.user_id = thread_cnt.user_id
    ) A
    """
)

top_user = bigquery_client.run_query_to_dataframe(
    query="""
    SELECT *
    FROM (
    SELECT conversation_cnt.*
        , user_db_master.name
        , ROW_NUMBER() OVER(PARTITION BY group_name ORDER BY cnt DESC) AS row_nums
    FROM (
        SELECT '이모지' AS group_name
            , user_id
            , COUNT(*) cnt
        FROM (
        SELECT JSON_EXTRACT_SCALAR(json, '$.name') AS name
            , REPLACE(user_id, '"', "") AS user_id
        FROM `geultto.geultto_9th.slack_conversation_master`,
        UNNEST(JSON_EXTRACT_ARRAY(reactions)) AS json,
        UNNEST(JSON_EXTRACT_ARRAY(json, '$.user_id')) AS user_id 
        WHERE tddate <= '2023-12-24'
        ) A
        GROUP BY user_id

        UNION ALL

        SELECT '게시글' AS group_name
            , user_id
            , COUNT(*) cnt
        FROM `geultto.geultto_9th.slack_conversation_master`
        WHERE tddate <= '2023-12-24'
        AND message_type = 'post'
        AND (text not like ('%님이 채널에 참여함%') AND text not like ('%integration to this channel%'))
        GROUP BY user_id

        UNION ALL

        SELECT '댓글' AS group_name
            , user_id
            , COUNT(*) cnt
        FROM `geultto.geultto_9th.slack_conversation_master`
        WHERE tddate <= '2023-12-24'
        AND message_type = 'thread'
        GROUP BY user_id  
        ) conversation_cnt
    LEFT JOIN `geultto.geultto_9th.users` AS users
    ON conversation_cnt.user_id = users.user_id
    LEFT JOIN `geultto.geultto_9th.user_db_master` AS user_db_master
    ON conversation_cnt.user_id = user_db_master.userid
    WHERE real_name NOT IN ('또봇', 'Bamboo Forest', '강나영', '변성윤', '강승현', '김은찬', '김재휘', '김정희', '김준홍', '류지환', '박수민', '배지훈', '성연찬', '송경근', '송민혜', '신해나라', '이찬주', '임정', '임지훈', '정이태', '정종윤', '정현수', '조재우', '지정수', '채정현', '최현구', '조동민')
    AND display_name NOT IN ('강나영', '변성윤', '강승현', '김은찬', '김재휘', '김정희', '김준홍', '류지환', '박수민', '배지훈', '성연찬', '송경근', '송민혜', '신해나라', '이찬주', '임정', '임지훈', '정이태', '정종윤',
    '정현수', '조재우', '지정수', '채정현', '최현구', '조동민')
    AND name NOT IN ('강나영', '변성윤', '강승현', '김은찬', '김재휘', '김정희', '김준홍', '류지환', '박수민', '배지훈', '성연찬', '송경근', '송민혜', '신해나라', '이찬주', '임정', '임지훈', '정이태', '정종윤', '정현수', '조재우', '지정수', '채정현', '최현구', '조동민')
    ) with_row_nums
    WHERE row_nums <= 3
    ORDER BY group_name, row_nums 
    """
)


# 1, 2, 3이런 숫자를 받아서 one two three로 바꿔주는 함수
def number_to_string(num):
    if num == 1:
        return "one"
    elif num == 2:
        return "two"
    elif num == 3:
        return "three"
    else:
        return "four"


# 오른쪽이 내꺼
# user_list = ["U0666UTKF8W", "U066AQSBRPF"]

# 전체 카운트 수
total_emoji = total_cnt["total_emoji"][0]
total_post = total_cnt["total_post"][0]
total_thread = total_cnt["total_thread"][0]


def process_row(row):
    return f"""{row['group_name']} :{number_to_string(row['row_nums'])}:짱: :hearts:<@{row['user_id']}>님!:hearts:  :amaze:{row['cnt']}회:amaze:"""


slack_messages = [process_row(row) for index, row in top_user.iterrows()]
slack_message = "\n".join(slack_messages)

# slack_app.message_for_private(users=user_list, text=text)
# text = f""":point_right::point_right: test_message :point_left::point_left:
# <@U066AQSBRPF>
# """

text = f""":point_right::point_right: 데달부가 1회차, 2회차 활동 기록을 공유드려요!! :point_left::point_left:
*총 게시글 수:* `{total_post}개`, *총 댓글 수:* `{total_thread}개`, *총 이모지 수:* `{total_emoji}개`

어느 정도 수치인지 감이 안오신다구요? :thinking_face:
`8기(1~11회차) 대비, 벌써 게시글/댓글은 절반 수준, 이모지수는 3,000개 가량 더 많습니다..!`

이제 한달가량 지났는데, 벌써 정말 많은 활동들을 해주셨네요 :amaze:
_(혹시나 데이터 수집 & 8기 활동 분석 내용을 보고 싶으신 분들은 해당 <https://github.com/ddongmiin/geultto_genie_bot?tab=readme-ov-file|링크>를 참조해주세요!!!)_
다들 열심히 활동해주셔서 정말정말 감사드립니다 :man-bowing::man-bowing::man-bowing:

가장 많은 활동을 해주신 명예의 전당 7분을 소개해드리며 이만 데달부는 물러가겠습니다 :wave:
올 한해 마무리 잘 하시고, 다들 새해 복 많이 받으세요:pray::pray::pray:

{slack_message}
"""

# C066AQH75EZ C06BQR0C8G3 이건 테스트 채널 C0672HTT36C 자유로운 담소
slack_app.message_for_channel(channel_id="C0672HTT36C", text=text)
