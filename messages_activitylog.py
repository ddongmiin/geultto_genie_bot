import os
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
     where userid IN ('U066SBYNGE5' , 'U066AQSBRPF', 'U066QLYFNKX')
    """
)

submit_count_pd = bigquery_client.run_query_to_dataframe(
    """
    select *
    from geultto.geultto_9th.post_submit_status_v3
    where userid IN ('U066SBYNGE5' , 'U066AQSBRPF')
    order by userid, issues
    """
)

for _, row in user_pd.iterrows():
    temp_pd = submit_count_pd.loc[(submit_count_pd.name == row[1])] 
    temp_pd = temp_pd.sort_values('issues')
    submit_text = ''
    for _, row1 in temp_pd.iterrows():
        if row1[3] == '제출완료': 
            submit_text = submit_text + "📬  "
        elif row1[3] == '패스완료':
            submit_text = submit_text + "📭  "
        else: 
            submit_text = submit_text + "💸  "
    print(submit_text)
    
    #활동 채널
    channel_1 = row[8].split(',')[0]
    channel_2 = row[8].split(',')[1]
    channel_3 = row[8].split(',')[2]

    #남은 예산에 따른 추가 문구 작성
    if row[15] <= 50000:
        final_ment = '앗 이번 기수동안에 글을 제출하실 여유가 없으셨군요:) 남은 1달동안이라도 2번의 글 제출 기회를 잡아보시는건 어떠실까요~?!'
    else:
        final_ment = ''

    #커피챗 횟수에 따른 추가 문구 작성
    if row[17] == 0:
        coffee_text = '앗 이번 기수동안에 커피챗을 진행할 여유가 없으셨군요:) 남은 1달동안이라도 주요 활동 채널 혹은 속하신 채널에서 커피챗을 진행해보시는건 어떠실까요~?!'
    elif row[17] == 1:
        coffee_text = '한번의 커피챗을 진행하셨군요. 남은 기간동안 한번의 커피챗을 더 진행해서 2번 채워보시는걸 어떠실까요~?!'
    elif row[17] == 2:
        coffee_text = '2번의 커피챗을 모두 진행하셨다니 멋있습니다! 글도 커피챗도 열심히 활동하시느라 고생많으셨어요~'
    else:
        coffee_text = f'이번 9기동안 {row[17]}번의 커피챗을 진행하셨다니 멋있습니다~ 다음 기수에서도 열심히 참여해주실거죠~?!'
    

    #패스권에 따른 문구 작성
    if row[18] == 2:
        pass_cnt=0 
        pass_text = '패스권을 하나도 사용하지 않으셨다니 멋있습니다! 부지런히 글을 쓰신 지난 5개월동안 고생많으셨어요~'
    elif row[18] == 1:
        pass_cnt=1
    else:
        pass_cnt=2
        

    text = f"""
안녕하세요! {row[1]}님! 좋은 하루입니다☀️
한달동안 연장전을 달릴 여러분들을 위해 지난 5개월 동안의 활동내역을 정리해보았습니다.👏
\n
> 🔎 *{row[14]}님의 활동 요약* 
    • {row[14]}님은 총 `{row[3]}`개의 글과 `{row[4]}`개의 쓰레드 `{row[5]}`개의 이모지를 남겨주셨어요. 
    • 지금까지 활동한 양을 보니 어떤 마음이 드시나요? 연장된 한달동안 {row[14]}님의 글과 생각을 글또에 남겨보시는건 어떠실까요?!🙏
\n
> 📝 *{row[14]}님의 글 제출 내역*
    • {submit_text} ___ 📬(제출완료), 📭(패스), 💸(차감)
    • {row[14]}님은 총 `{row[19]}`회 제출 완료, `{pass_cnt}`회 패스를 사용하셨어요. 
    • 이렇게 보니 5달이란 시간이 엄청 빨리 간것 같아요! 이렇게 끝내기 아쉽지 않나요? 추가로 글을 제출하셔서 환급금을 받아가세용~! 가보자고~~💪🏻💪🏻
\n
> 🐾 *{row[14]}님의 주요 활동 채널*
        1위: {channel_1}
        2위: {channel_2}
        3위: {channel_3}
   • 지금까지 해당 채널에 여러분들의 발자취를 남겨주셔서 감사드립니다🙇🏻‍♀️ 
   • 남은 한달동안 {row[14]}님이 속한 {row[11]}에서 마지막으로 안부인사들을 나눠보시는건 어떨까요~?
\n
> 👫 *{row[14]}님의 환상의 짝궁*
    • 글또 9기동안 나와 함께해준 환상의 짝꿍을 소개해드릴게요. 
        💝내가 많이 관심을 보였던 짝꿍💝 : `{row[13]}`
        💘나에게 많은 관심을 보내준 짝꿍💘 : `{row[12]}`
\n 
> ☕️ *{row[14]}님의 커피챗 내역*
    • {row[14]}님의 커피챗 횟수는 `{row[17]}`입니다.
    • {coffee_text}
    • 글또가 끝나기전에 환상의 짝궁으로 선정된 분들과 커피챗을 해보시면 어떠실까요?☕️
\n 
{final_ment} 
글또 9기동안 {row[9]}로서 활동하시며 좋은 글 남겨주셔서 감사드립니다.
남은 연장전도 열심히 참여해 주실거죠?! 그럼 {row[14]}님의 남은 글또 활동도 응원하며 물러가보겠습니다. 저희는 글또 10기에서 더 발전된 모습으로 만나요!
    """
    print(row[1])

    slack_app.message_for_private(user=row[0], text=text)
