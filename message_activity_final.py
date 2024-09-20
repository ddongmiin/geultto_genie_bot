import os
import time
from core.slack import SlackMessageRetriever
from core.bigquery import BigqueryProcessor

slack_app = SlackMessageRetriever(env_name="SLACK_TOKEN_BOT")
bigquery_client = BigqueryProcessor(
    env_name="GOOGLE_APPLICATION_CREDENTIALS", database_id="geultto_9th"
)

user_pd = bigquery_client.run_query_to_dataframe(
    query="""
    select distinct *
    from geultto.geultto_9th.send_message_final_view_test_v3
    """
).fillna(
    0
)  # 예치금 시트에 기록이 없는 경우 0으로 처리

for _, row in user_pd.iterrows():
    temp_pd = submit_count_pd.loc[(submit_count_pd.name == row[1])]
    temp_pd = temp_pd.sort_values("issues")
    submit_text = ""
    for _, row1 in temp_pd.iterrows():
        if row1[3] == "제출완료":
            submit_text = submit_text + "📬  "
        elif row1[3] == "패스완료":
            submit_text = submit_text + "📭  "
        else:
            submit_text = submit_text + "💸  "
    print(submit_text)

    if row[15] <= 50000:
        final_ment = "앗 이번 기수동안에 글을 제출하실 여유가 없으셨군요:) 남은 1달동안이라도 2번의 글 제출 기회를 잡아보시는건 어떠실까요~?!"
    else:
        final_ment = ""

    # 커피챗 횟수에 따른 추가 문구 작성
    if row[17] == 0:
        coffee_text = "앗 이번 기수동안에 커피챗을 진행할 여유가 없으셨군요:) 남은 1달동안이라도 주요 활동 채널 혹은 속하신 채널에서 커피챗을 진행해보시는건 어떠실까요~?!"
    elif row[17] == 1:
        coffee_text = "한번의 커피챗을 진행하셨군요. 남은 기간동안 한번의 커피챗을 더 진행해서 2번 채워보시는걸 어떠실까요~?!"
    elif row[17] == 2:
        coffee_text = "2번의 커피챗을 모두 진행하셨다니 멋있습니다! 글도 커피챗도 열심히 활동하시느라 고생많으셨어요~"
    else:
        coffee_text = f"이번 9기동안 {row[17]}번의 커피챗을 진행하셨다니 멋있습니다~ 다음 기수에서도 열심히 참여해주실거죠~?!"

    # 패스권에 따른 문구 작성
    if row[18] == 2:
        pass_cnt = 0
        pass_text = "패스권을 하나도 사용하지 않으셨다니 멋있습니다! 부지런히 글을 쓰신 지난 5개월동안 고생많으셨어요~"
    elif row[18] == 1:
        pass_cnt = 1
    else:
        pass_cnt = 2

    text = f"""
안녕하세요! {row[1]}님! 좋은 하루입니다☀️
그간 활동은 어떠셨나요? 400명가량이나 되는 커뮤니티에 참여 해주신 것만으로도 큰 용기를 내주신 것 같아 너무나 감사드립니다. 
글또 9기를 마무리하며 11월 27일부터 4월 14일까지 약 5개월간, 어떤 활동을 하셨는지 보실 수 있도록 데이터를 정리해 보았습니다.👏
같이 데이터를 보며 처음 참여했던 마음가짐과 앞으로의 목표에 대해서 생각해 봐요:) 참여해 주셔서 다시 한번 감사드립니다. 🙇🏻‍ 
\n
> 🔎 *{row[14]}님의 활동 요약* 
    • {row[14]}님은 총 `{row[3]}`개의 포스트와 `{row[4]}`개의 쓰레드 `{row[5]}`개의 이모지를 남겨주셨어요. 
    • 글또에서 활동하시면서 어떤 점을 느끼셨나요? 최초에 다짐했던 내용을 잘 지켰는지 혹은 그 외에 어떤 경험을 했을지 궁금하네요. 이런 내용들을 담아 {row[14]}님의 회고 글을 작성해보시면 어떨까요? 다음 기수를 하고 싶다면 다음 기수를 위한 다짐을 미리 작성해도 좋을 것 같아요.🙏
\n
> 📝 *{row[14]}님의 글 제출 내역*
    • {submit_text} ___ 📬(제출완료), 📭(패스), 💸(차감)
    • {row[14]}님은 총 `{row[19]}`회 제출 완료, `{pass_cnt}`회 패스를 사용하셨어요. 
    • OT 때 들은것 처럼 5달이 정말 빠르게 지나갔어요! 연장된 기간에 글을 제출하셔서 추가 환급금 받아가시고, 자신의 생각을 정리해보세요. 시작만큼이나 끝맺음을 잘하는 것이 중요한 것 같아요. 남은 기간 저희 열심히 해보아요.💪🏻💪🏻
\n
> 🐾 *{row[14]}님의 주요 활동 채널*
        {row[8]}
   • 지금까지 해당 채널에 여러분들의 발자취를 남겨주셔서 감사드립니다.🙇🏻‍♀️ 
   • 남은 한달동안 {row[14]}님이 속한 `{row[11]}`에서 인사를 나눠보면 어떨까요?
\n
> 👫 *{row[14]}님의 환상의 짝궁*
    • 글또 9기동안 나와 함께해준 환상의 짝꿍을 소개해드릴게요. 감사의 표현을 나눠보세요:)
        💝내가 많이 관심을 보였던 짝꿍💝 : `{row[13]}`
        💘나에게 많은 관심을 보내준 짝꿍💘 : `{row[12]}`
\n 
> ☕️ *{row[14]}님의 커피챗 내역*
    • {row[14]}님의 커피챗 횟수는 `{row[17]}`번 입니다.
    • {coffee_text} 우리의 인연은 지금부터 시작이기에 앞으로도 커피챗을 하며 인연을 이어나가요.
\n 
{final_ment} 
글또 9기동안 {row[9]}로서 활동하시며 좋은 글 남겨주셔서 감사드립니다.
남은 연장전도 열심히 참여 부탁드려요. 그럼 저 데달부는 {row[14]}님의 남은 글또 활동도 응원하며 물러가보겠습니다. 
데달부를 맞춤 Push 시스템으로 만들고 있는데, 여러분들의 의견이 궁금해요. 간단한 설문이니 여러분들의 관심과 참여를 부탁드립니다~🙇🏻‍♀️ 
그럼 저희는 글또 10기에서 또 만나요!!
\n
데달부 의견 청취함 : https://bit.ly/3w9spg2
    """
    print(row[1])
    print(text)

    # 혹시 있을 에러로 인해 메시지가 중간에 멈출 경우, 중단 지점부터 다시 발송 할 수 있도록. 해당 셀 실행 시, 중단지점부터 다시 발송
    user_pd = user_pd.drop(user_pd[user_pd["name"] == row[1]].index)
    # slack_app.message_for_private(user=row[0], text=text)
