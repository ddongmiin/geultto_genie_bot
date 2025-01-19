# %%
'''
250119에 사용된 운영진 34명 대상 CRM 메세지 스크립트입니다. 
테스트하려면
query,query2 부분 모두 where 조건으로 사람을 지정하여 보내면 즉시 지니봇으로 보내집니다.
34명 기준 23초 소요되었으며, 로컬csv에 발송한 메세지와 해당 글의 ts가 저장됩니다.

crm_korean_newyear_01_agg: geultto_10th.super_user_mart 을 기반으로하는 집계 테이블
korean_newyear_crm_02_submit: 개별 인원의 글제출/패스/차감이 history
'''

import warnings
warnings.filterwarnings('ignore')
from message_basic import *
import pandas as pd
import time

start = time.time()
query = 'SELECT * FROM geultto.geultto_10th.crm_korean_newyear_01_agg limit 10'
user_pd = get_bigquery(query).fillna(0)            

#실행 정보 저장
cnt = 0 #발송한 횟수
df_sended = pd.DataFrame() #발송된 메세지 저장하기위한 데이터프레임 초기화
text_list = [] # 발송한 메세지
ts_list = [] #발송한 메세지의 ts 식별자

query2 = 'SELECT * FROM geultto.geultto_10th.korean_newyear_crm_02_submit limit 10'
submit_count_pd = get_bigquery(query2)

for _, row in user_pd.iterrows():
    temp_pd = submit_count_pd.loc[(submit_count_pd.name == row[1])]
    temp_pd = temp_pd.sort_values("step_number") # 제출이력 오름차순으로 정렬
    submit_text = ""
    for _, row1 in temp_pd.iterrows():
        if row1[4] == "제출완료":
            submit_text = submit_text + "📬  "
        elif row1[4] == "패스완료":
            submit_text = submit_text + "📭  "
        else:
            submit_text = submit_text + "💸  "
    
    #관심 레벨에 따른 추가 문구 작성(1~4)
    #interest level
    ## 지난 한 달 동안 [총 댓글 수/이모지 반응 수]로 커뮤니티에 활기를 불어넣어주셨어요. 덕분에 많은 분들이 유익한 시간을 보냈습니다. 설날을 맞아 더 많은 멋진 이야기를 기다리고 있을게요! 
    if row[16] == 1:
        interest_text = '지난 3개월동안 관심지수가 조금 낮군요..! 하지만, 설날을 맞아 새로운 마음으로 시작 해보시는 건 어떨까요? 설 연휴동안 코어에서 제출된 글들을 읽고 부담없이 쓰레드를 남겨보시는건 어떠신가요? 커뮤니티에서 활동하고 계실 여러분들의 모습을 기대하고 있을게요.'
    elif row[16] == 2:
        interest_text = '지금까지 활동 하셨던걸 숫자로 보니 감회가 새롭지 않나요? 설날을 맞아 관심있던 채널 혹은 코어채널에 잠시 들러서 새해 인사를 나눠보는 건 어떠세요? 앞으로도 풍성한 글또를 함께 만들어 가요! 🌱'
    elif row[16] == 3:
    #나름 열심히 활동
        interest_text = '지금까지 활동 하셨던걸 숫자로 보니 감회가 새롭지 않나요? 덕분에 매주 커뮤니티가 더 풍성해지고 있어요. 주기적으로 글또에 관심을 가지고 활동해 주셔서 감사해요. 앞으로 풍성한 글또를 함께 만들어 가요! 🌱'
    else:
    #완전 열심히 활동
        interest_text = '덕분에 커뮤니티가 더 풍성해지고 있어요. 매주 커뮤니티에 관심을 가지고 활동해 주셔서 정말 감사해요.⚡ 항상 응원합니다! 남은 기간도 지금처럼만~ 아시죠?'
    
    #제출 레벨에 따른 추가 문구 작성(1~4)
    #submit level :    #설날을 맞아 또 다른 멋진 글을 기다리고 있을게요. 
    if row[17] == 1:
    #절반 이내 재출
        submit_add_text = '저희는 항상 여러분들의 글을 기다리고 있어요. 함께 나누는 글들이 더 많아지길 기대하며, 설 연휴동안 글감을 미리 적어보시는건 어떨까요~?'
    elif row[17] == 2:
    #조금 아쉽게 제출 4,5
        submit_add_text = '혹시 놓친 글들이 아쉬우셨나요? 😊 아쉬운만큼 더 뽜이이팅 넘치게 설 연휴동안 글감을 미리 적어보시는건 어떠세요~?'
    elif row[17] == 3:
    #6,7 제출 : 보통
        submit_add_text = '그럼에도 항상 꾸준히 참여해 주셔서 감사해요! 남은기간도 이렇게 계속 가보자고~~'
    else:
    #7번 이상 제출
        submit_add_text = f'이번 10기동안 7개 이상의 글을 제출하며 보여주신 열정과 글들에 깊이 감사드립니다. 당신의 활동이 다른 이들에게도 큰 영감이 되고 있다는 사실, 이미 아시죠? 앞으로도 계속 좋은 글 기대할게요! '
        
        
    #커피챗 레벨에 따른 추가 문구 작성(0~3)
    if row[18] == 0:
    #커피챗 한번도 안한거
        coffee_text = '앗 이번 기수동안에 커피챗을 진행할 여유가 없으셨군요:) 그런데 여러분 커피챗을 한번만 진행해도 커피챗 레벨이 +1 된다는 사실 아시나요? 관심있던 4채널 혹은 코어 채널에서 커피챗을 진행해보시는건 어떠실까요~?'
    elif row[18] == 1:
    #커피챗 한번만 한거
        coffee_text = '첫 커피챗은 어떠셨나요? 해당 경험이 좋은 기억으로 남았으면 좋겠어요:) 다음 커피챗에서도 새로운 사람들과 이야기를 나누며 또 다른 영감을 얻어보세요. 누구와 하실지 막막하시다구요? 그렇다면 아래 짝궁분들과 커피챗 해보시는 건 어떠세요?'
    elif row[18] == 2:
    #2,3번 커피챗 진행
        coffee_text = '벌써 여러 차례 커피챗을 진행하셨다니, 정말 멋있어요!🌟 이렇게 멋진 참여로 커뮤니티에 활력을 더해주셔서 감사합니다. 앞으로도 여러분의 이야기를 나누고, 다른 멤버들과 더욱 깊이 연결되는 시간을 만들어보세요.  누구와 하실지 막막하시다구요? 그렇다면 아래 짝궁분들과 커피챗 해보시는 건 어떠세요?'
    elif row[18] == 3:
    #상위 15%안에 든
        coffee_text = '커피챗 채널에서 활약해 주셔서 정말 감사드려요!💬 덕분에 커뮤니티가 더 따뜻하고 풍성해지고 있다는거 이미 아시죠~?! 이미 많은 멤버들과 커피챗을 하셨지만, 앞으로도 기대할게요. 계속해서 활발한 교류와 관계를 만들어 나가주세요. 🌟'
           
    #전체 레벨에 따른 추가 문구 작성
    #group rank(0~3)
    if row[20] == 1:
    #상위 10%	상위 10%인거 말씀드리면서 활발한 활동 유지 부탁
        final_ment  = '마지막으로 여러분들에게만 말씀드리는건데..(속닥속닥) 글또 활동지수 상위 10%안에 드셨어요~ 늘 활발하게 활동해 주셔서 감사합니다. 남은 기간도 응원할게요! 새해 복 많이 받으세요!'
    elif row[20] == 2:
        final_ment  = '마지막으로 글또에서 여러분들은 어떤 활동들을 더 하고 싶으신가요? 앞으로 활발한 활동을 응원하며 물러가보겠습니다. 저희는 글또 10기가 끝날즈음에 더 발전된 모습으로 만나요!새해 복 많이 받으세요! '
    elif row[20] == 3:
    #하위 10%	조금씩 활동을 독려하는 메세지
        final_ment  = '마지막으로 글또 글 제출 및 커뮤니티 활동이 쉽지 않으시죠~?! 혹여나 저희가 도울 수 부분이 있다면 언제든 대나무숲을 통해 알려주세요. 남은 기간동안에도 조금씩 관심을 가져주실거라 믿을게요. 저희는 글또 10기가 끝날즈음에 더 발전된 모습으로 만나요! 새해 복 많이 받으세요!'
    else:
        final_ment  = f'마지막으로 여러분들에게만 말씀드리는건데..(속닥속닥) 글또 활동지수 탑랭커세요!! 이번 10기동안 커피챗, 제출, 활동 그 어느것 하나 놓치지않고 최고레벨을 달성하셨다니 진짜 멋있아요~  남은 기간도 활발한 활동을 응원하며 물러가보겠습니다. 새해 복 많이 받으세요!'
    
    active_imoji = '❤️‍🔥' * int(row[16])
    submit_imoji = '✍🏻' * int(row[17])
    coffee_imoji = '☕️' * int(row[18])

    text = f'''
    안녕하세요 {row[12]}님! 글또도 이제 절반이 넘는 시간을 함께 해왔는데요~ 남은 시간들도 함께 해주실 여러분들을 위해 지난 4개월 동안의 활동내역을 정리해보았습니다.👏

    > 🚩 *{row[12]}님의 글또 레벨*
        • 활동 레벨(1~4): {active_imoji}
        • 글 제출 레벨(1~4): {submit_imoji}
        • 커피챗 레벨(0~3): {coffee_imoji} 
        

    > 🔎 *{row[12]}님의 활동 요약* 
        • {row[12]}님은 총 {row[6]} 개의 글과 {row[7]} 개의 쓰레드 {row[8]} 개의 이모지를 남겨주셨어요.
        • {interest_text}

    > 📝 *{row[12]}님의 글 제출 내역*
        • {submit_text} 범례: 📬(제출완료), 📭(패스), 💸(차감)
        • 총 {row[15]}회 제출 완료, {row[14]}회 패스를 사용하셨어요. 
        • {submit_add_text}💪🏻💪🏻
    
    > ☕️ *{row[12]}님의 커피챗 내역*
        • {row[12]}님의 커피챗 횟수는 {row[13]} 회 입니다.
        • {coffee_text}
    
    > 👫 *{row[12]}님의 환상의 짝궁*
        • 지금까지 나에게 관심을 보여준 팔로워 top 5명을 소개해드릴게요. 혹시 친해지고 싶으신 분이 있다면 커피챗을 신청해보는 건 어떠세요? 사이도 돈독해지고 포인트도 얻는 1석 2조의 기회
        • 💘나에게 많은 관심을 보내준 팔로워💘: {row[11]}

    글또 10기동안 {row[2]} 코어 채널에 활동하시며 관심가져주셔서 감사드립니다. 
    설 연휴를 맞아 계획하고 계신게 있으신가요? 다 좋지만 이번 긴 연휴동안 푹 쉬는 것도 잊지마셔요~🙇🏻‍♀️
    {final_ment}
    '''
    result = send_message_to_user(row[0],text)
    text_list.append(text)
    ts_list.append(result)
    
    cnt += 1
    df_temp = pd.DataFrame({'text':text_list,'ts':ts_list})    
    if cnt % 50 == 0:
        time.sleep(1)
        print(f'{cnt}명 전달완료')
pd.concat([df_sended,df_temp]).to_csv('./message_sended.csv', encoding = 'utf-8-sig')
print(f'{cnt}명 전달완료')
end = time.time()
print(round(end - start))