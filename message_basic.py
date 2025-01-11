'''
genie봇이 메세지를 보내는 기본기능을 정의합니다. token은 관리자 문의
해당 스크립트를 실행하면 번호로 선택하여 실행하며, 기능을 수행할 대상에 대한 정보(user_id 혹은 스레드정보)를 전달해야합니다.
'''

import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN") # 윈도우 로컬 - 시스템 - 환경변수에 저장
USER_ID = "U07NL2P380P"  # 메시지를 보낼 대상의 Slack User ID


#하기 두개의 인자는 스레드의 링크복사 URL에 정보가 저장되어있음

URL = 'https://geultto10.slack.com/archives/C07PD0N3D33/p1736409035694479'
URL_LIST = URL.split('/')
CHANNEL_ID = URL_LIST[4] # "C07PD0N3D33" 채널 ID
THREAD_TS = URL_LIST[5][1:-7] + '.' + URL_LIST[5][-7:] # "1736409035.694479"  스레드 메시지의 timestamp

# WebClient 초기화
client = WebClient(token=SLACK_BOT_TOKEN)

def send_message_to_user(user_id, message):
    try:
        response = client.chat_postMessage(
            channel=user_id,
            text=message
        )
        print(f"메시지가 성공적으로 전송되었습니다: {response['ts']}")
    except SlackApiError as e:
        print(f"오류 발생: {e.response['error']}")

def send_reply_to_thread(channel_id, thread_ts, message):
    try:
        response = client.chat_postMessage(
            channel=channel_id,
            text=message,
            thread_ts=thread_ts
        )
        print(f"스레드에 댓글이 성공적으로 전송되었습니다: {response['ts']}")
    except SlackApiError as e:
        print(f"오류 발생: {e.response['error']}")

# 메뉴 기반 실행1
if __name__ == "__main__":
    print("실행할 기능을 선택하세요:")
    print("1. 사용자에게 메시지 보내기")
    print("2. 스레드에 댓글 달기")

    choice = input("번호를 입력하세요: ")

    if choice == "1":
        message = "안녕하세요! Slack Bot에서 보낸 메시지입니다."
        send_message_to_user(USER_ID, message)
    elif choice == "2":
        message = "스레드에 댓글 달기 테스트중입니다."
        send_reply_to_thread(CHANNEL_ID, THREAD_TS, message)
    else:
        print("잘못된 입력입니다. 다시 실행해주세요.")