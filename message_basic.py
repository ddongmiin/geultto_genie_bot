'''
genie봇이 메세지를 보내는 기본기능을 정의합니다. token은 관리자 문의
해당 스크립트를 실행하면 번호로 선택하여 실행하며, 기능을 수행할 대상에 대한 정보(user_id 혹은 스레드정보)를 전달해야합니다.
'''
import sys
import os
print(sys.version)
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
#from core.slack import SlackMessageRetriever # SlackMessageRetriever 환경설정 후 메세지 보내는 
from core.bigquery import BigqueryProcessor
bigquery_client = BigqueryProcessor(
    env_name="GOOGLE_APPLICATION_CREDENTIALS", database_id="geultto_10th"
)
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN") # 윈도우 로컬 - 시스템 - 환경변수에 저장
USER_ID = ""  # 메시지를 보낼 대상의 Slack User ID


#하기 두개의 인자는 스레드의 링크복사 URL에 정보가 저장되어있음
# URL = ''# 개인정보 이슈 삭제
# URL_LIST = URL.split('/')
# CHANNEL_ID = URL_LIST[4] #  채널 ID
# THREAD_TS = URL_LIST[5][1:-7] + '.' + URL_LIST[5][-7:] #   스레드 메시지의 timestamp

# WebClient 초기화
client = WebClient(token=SLACK_BOT_TOKEN)

def send_message_to_user(user_id, message):
    '''
    Slack 메세지 전송 함수
    '''
    try:
        response = client.chat_postMessage(
            channel=user_id,
            text=message
        )
        # print(f"메시지가 성공적으로 전송되었습니다: {response['ts']}")
        return response['ts']
    except SlackApiError as e:
        print(f"오류 발생: {e.response['error']}")

def send_reply_to_thread(channel_id, thread_ts, message):
    '''
    스레드에 댓글 남기는 함수
    '''
    try:
        response = client.chat_postMessage(
            channel=channel_id,
            text=message,
            thread_ts=thread_ts
        )
        print(f"스레드에 댓글이 성공적으로 전송되었습니다: {response['ts']}")
    except SlackApiError as e:
        print(f"오류 발생: {e.response['error']}")

def get_bigquery(query):
    '''
    단순 빅쿼리 조회용
    '''
    try:
        result = bigquery_client.run_query_to_dataframe(query)
        print('Big query 조회 성공')
        # print(result)
        return result
    except Exception as e:
        print(f'Bigquery 오류 발생 {e}')

def send_bigquery_to_slack(user_id, query):
    data = get_bigquery(query)
    if data:
        for row in data:
            message = f'Bigquery 데이터: {row}'
            send_message_to_user(user_id,message)
    else:
        print('전송할 데이터가 없습니다.')


# 메뉴 기반 실행
if __name__ == "__main__":
    print("실행할 기능을 선택하세요:")
    print("1. 사용자에게 메시지 보내기")
    print("2. 스레드에 댓글 달기")
    print("3. Bigquery 데이터 조회")
    print("4. Bigquery 데이터 사용하여 사용자에게 전송")

    choice = input("번호를 입력하세요: ")

    if choice == "1":
        message = "안녕하세요! Slack Bot에서 보낸 메시지입니다."
        send_message_to_user(USER_ID, message)
    elif choice == "2":
        message = "스레드에 댓글 달기 테스트중입니다."
        send_reply_to_thread(CHANNEL_ID, THREAD_TS, message)
    elif choice == "3":
        query = 'SELECT * FROM geultto.geultto_10th.user_super_mart LIMIT 1;'
        get_bigquery(query)
    elif choice == "4":
        query = 'SELECT * FROM geultto.geultto_10th.user_super_mart LIMIT 1;'
        send_bigquery_to_slack(USER_ID, query)
    else:
        print("잘못된 입력입니다. 다시 실행해주세요.")