# %%
'''
- Description
250125에 사용된 전체 대상 CRM 메세지 스크립트입니다. 
./history 경로에 발송한 대상, 메세지, ts가 저장됩니다.

- 사용된 bigquery
crm_korean_newyear_01_agg: geultto_10th.super_user_mart 을 기반으로하는 집계 테이블
crm_korean_newyear_02_submit: 개별 인원의 글제출/패스/차감 history

- 개선필요사항
비동기 -> 동기식 변환
속도향상(현재 361초)
'''

import warnings
warnings.filterwarnings('ignore')
from message_basic import *
import pandas as pd
import time

start = time.time()
query = 'SELECT * FROM geultto.geultto_10th.crm_korean_newyear_01_agg limit 1'
user_pd = get_bigquery(query).fillna(0)            

#실행 정보 저장
cnt = 0 #발송한 횟수
df_sended = pd.DataFrame() #발송된 메세지 저장하기위한 데이터프레임 초기화
name_list = []
text_list = [] # 발송한 메세지
ts_list = [] #발송한 메세지의 ts 식별자

query = 'SELECT * FROM geultto.geultto_10th.crm_korean_newyear_02_submit limit 1'
submit_count_pd = get_bigquery(query)
for _, row in user_pd.iterrows():
    temp_pd = submit_count_pd.loc[(submit_count_pd.name == row[1])]
    temp_pd = temp_pd.sort_values("step_number")# 제출이력 오름차순으로 정렬
    submit_text = ""
    for _, row1 in temp_pd.iterrows():
        if row1[4] == "제출완료":
            submit_text = submit_text + "📬  "
        elif row1[4] == "패스완료":
            submit_text = submit_text + "🌴  "
        else:
            submit_text = submit_text + "💸  "

    #관심 레벨에 따른 추가 문구 작성(1~4)
    #interest level
    ## 지난 한 달 동안 [총 댓글 수/이모지 반응 수]로 커뮤니티에 활기를 불어넣어주셨어요. 덕분에 많은 분들이 유익한 시간을 보냈습니다. 설날을 맞아 더 많은 멋진 이야기를 기다리고 있을게요! 
    if row[16] == 1:
        interest_text = f'설날을 맞아 새로운 마음으로 관심있는 글들을 읽고 쓰레드를 남겨보시는 건 어떠신가요? 커뮤니티에서 활동하고 계실 {row[12]}님의 모습을 기대하고 있을게요. 🌱'
    elif row[16] == 2:
        interest_text = f'지금까지 활동하셨던 것을 숫자로 보니 감회가 새롭지 않나요? 설날을 맞아 관심 있던 채널 혹은 코어 채널에 잠시 들러 새해 인사를 나눠보는 건 어떠세요? 앞으로도 풍성한 글또를 함께 만들어 가요! 🌱'
    elif row[16] == 3:
        interest_text = f'지금까지 활동하셨던 것을 숫자로 보니 감회가 새롭지 않나요? 덕분에 매주 커뮤니티가 더 풍성해지고 있어요. 주기적으로 글또에 관심을 가지고 활동해 주셔서 감사해요. 앞으로도 풍성한 글또를 함께 만들어 가요! 🌱'
    else:
        interest_text = f'덕분에 커뮤니티가 더 풍성해지고 있어요. 매주 글또에 관심을 가지고 활동해 주셔서 정말 감사해요. 항상 응원합니다! 남은 기간도 지금처럼만~ 아시죠? 😉'

    
    #제출 레벨에 따른 추가 문구 작성(1~4)
    #submit level :    #설날을 맞아 또 다른 멋진 글을 기다리고 있을게요. 
    if row[17] == 1:
        submit_add_text = f'저희는 항상 {row[12]}님의 글을 기다리고 있어요. 함께 나누는 글들이 더 많아지길 기대하며, 설 연휴 동안 글감을 미리 적어보시는 건 어떨까요~?'
    elif row[17] == 2:
        submit_add_text = f'포기하지 않고 꾸준히 글을 제출하려 노력해 주셔서 감동이에요 😊 더 파이팅 넘치게 설 연휴 동안 글감을 미리 적어보시는 건 어떠세요~?'
    elif row[17] == 3:
        submit_add_text = f'꾸준히 글을 잘 제출해 주셔서 감사해요! 남은 기간도 이렇게 계속 가보자고~~'
    else:
        submit_add_text = f'이번 10기 동안 7개 이상의 글을 제출하며 보여주신 열정과 글들에 깊이 감사드려요. {row[12]}님의 활동이 다른 이들에게도 큰 영감이 되고 있다는 사실, 이미 아시죠? 앞으로도 계속 좋은 글 기대할게요!'
        
        
    #커피챗 레벨에 따른 추가 문구 작성(0~3)    
    if row[18] == 0:
        coffee_text = f'앗, {row[12]}님! 커피챗을 한 번만 진행해도 커피챗 레벨이 +1 된다는 사실 아시나요? 관심 있던 4채널 혹은 코어 채널에서 커피챗을 진행해 보시는 건 어떠실까요~?'
    elif row[18] == 1:
        coffee_text = f'첫 커피챗은 어떠셨나요? 해당 경험이 좋은 기억으로 남았으면 좋겠어요 :) 다음 커피챗에서도 새로운 사람들과 이야기를 나누며 또 다른 영감을 얻어보세요~'
    elif row[18] == 2:
        coffee_text = f'벌써 여러 차례 커피챗을 진행하셨다니, 정말 멋있어요! 🌟 이렇게 멋진 참여로 커뮤니티에 활력을 더해주셔서 감사합니다. 앞으로도 여러분의 이야기를 나누고, 다른 멤버들과 더욱 깊이 연결되는 시간을 만들어보세요~'
    else: # row[18] == 3
        coffee_text = f'커피챗 채널에서 활약해 주셔서 정말 감사드려요! 💬 덕분에 커뮤니티가 더 따뜻하고 풍성해지고 있다는 거 아시죠~?! 이미 많은 멤버들과 커피챗을 하셨지만, 앞으로도 기대할게요. 계속해서 활발한 교류와 관계를 만들어 나가주세요. 🌟'
           
    #전체 레벨에 따른 추가 문구 작성
    #group rank(0~3)
    if row[20] == 1:
        final_ment = f'마지막으로 {row[12]}님에게만 말씀드리는 건데..(속닥속닥) 글또 활동 지수 상위 10% 안에 드셨어요~ 늘 활발하게 활동해 주셔서 감사합니다. 남은 기간도 응원할게요! 새해 복 많이 받으세요!🙇🏻‍♀️'
    elif row[20] == 2:
        final_ment = f'마지막으로 {row[12]}님은 글또에서 어떤 활동들을 더 하고 싶으신가요? 앞으로 활발한 활동을 응원할게요. 저희는 글또 10기가 끝날 즈음에 더 발전된 모습으로 만나요! 새해 복 많이 받으세요!🙇🏻‍♀️'
    elif row[20] == 3:
        final_ment = f'마지막으로 {row[12]}님! 성윤님이 공지사항에 올린 회고 설문을 남겨보시면 어떨까요? 저희가 도울 수 있는 부분이 있다면 돕고 싶어요. 남은 기간 동안에도 조금씩 관심을 가져주실 거라 믿을게요. 새해 복 많이 받으세요!🙇🏻‍♀️'
    else:
        final_ment = f'마지막으로 {row[12]}님에게만 말씀드리는 건데..(속닥속닥) 글또 활동 지수 탑 랭커세요!! 이번 10기 동안 커피챗, 제출, 활동 그 어느 것 하나 놓치지 않고 최고 레벨을 달성하셨다니 진짜 멋있어요~ 남은 기간도 활발한 활동을 응원할게요. 새해 복 많이 받으세요!🙇🏻‍♀️'

    if row[1] == '변성윤':
        final_ment = '''
    
    🌟 성윤님에게 CRM팀의 깜짝 메세지! 🌟
    *동민*: 존경합니다 필승🫡
    *정현*: 항상 뒤에서 슬낏슬낏 보고 배우는게 많아요~ 포인트 많이 모아서 넥스트 글또 모임에도 함께해보겠습니닷!🫡
    *이태*: 늘 따뜻하고 베푸는 모습에 많은 영감과 귀감을 받습니다. 충성충성
    *연찬*: 글또라는 소중한 공간을 마련해주셔서 너무 감사합니다! 정말 많은 기회와 생각을 얻게되었어요. 다른사람에게 선사한 것보다 더 많이 돌려받으시길!
    *임정*: 성윤님 덕분에 돈 대신 가치로 사람을 이끄는 방법을 배웠습니다. 앞으로도 함께해요~
        '''
    active_imoji = '❤️‍🔥' * int(row[16])
    submit_imoji = '✍🏻' * int(row[17])
    coffee_imoji = '☕️' * int(row[18])

    text = f'''
🧧{row[12]}님! 복주머니가 도착했어요🧧

 🚩 *{row[12]}님의 글또 레벨*
  • 활동 레벨(1~4): {active_imoji}
  • 글 제출 레벨(1~4): {submit_imoji}
  • 커피챗 레벨(0~3): {coffee_imoji} 
    
    
 🔎 *{row[12]}님의 활동 요약* 
  • 메시지 {row[6]} 개/ 댓글 {row[7]} 개 / 이모지 {row[8]} 개 

> {interest_text}🌱
    
    
 📝 *{row[12]}님의 글 제출 내역*
  • 총 {row[15]}회 제출 / {row[14]}회 패스
  • {submit_text} 
  • 범례: 📬(제출완료), 🌴(패스), 💸(차감)

> {submit_add_text}💪🏻💪🏻
    
    
 ☕️ *{row[12]}님의 커피챗 내역*
  • 총 {row[13]} 번의 커피챗 진행

> {coffee_text}
    
 👫 *{row[12]}님의 팔로워*

> 지금까지 나에게 관심을 보여준 팔로워 TOP 5명을 소개해드릴게요. 혹시 친해지고 싶으신 분이 있다면 커피챗을 신청해보는 건 어떠세요? 사이도 돈독해지고 포인트도 얻는 1석 2조의 기회!✌🏻

  • 💘나에게 많은 관심을 보내준 팔로워💘: {row[11]}


🧧 나가는 말

> 설 연휴를 맞아 계획하고 계신 일이 있으신가요? 무엇을 하시든 좋지만, 이번 긴 연휴 동안 푹 쉬는 것도 잊지 마세요~
> {final_ment}
    '''
    result = send_message_to_user(row[0],text)
    text_list.append(text)
    ts_list.append(result)
    name_list.append(row[1])
    
    #댓글로 채널 리스트 전달하기
    # message_to_thread = f'''
    # 참여하신 채널리스트는 {row[10]} 입니다! 
    # '''
    # send_reply_to_thread('D088BCH43V2',result,message_to_thread)
    cnt += 1
    df_temp = pd.DataFrame({'name':name_list,'text':text_list,'ts':ts_list})    
    if cnt % 100 == 0:
        time.sleep(1)
        print(f'{cnt}명, {row[1]}까지 전달완료')
time_record = time.strftime("%Y%m%d_%H%M%S", time.localtime(time.time()))
pd.concat([df_sended,df_temp]).to_csv(f'./history/message_sended_{time_record}.csv', encoding = 'utf-8-sig')
print(f'최종 {cnt}명 전달완료')
end = time.time()
print(round(end - start), '초 소요됨')