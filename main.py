import sys
from core.date_utils import get_daily_datelist
from batch.slack_daily_message_extraction import MessageExtractor

if __name__ == "__main__":
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    print(f"{start_date} ~ {end_date} 데이터 처리를 시작합니다.")
    message_client = MessageExtractor(token_type="SLACK_TOKEN_USER")
    message_client.update_users()
    message_client.update_channels()

    for sdatetime in get_daily_datelist(start_date=start_date, end_date=end_date):
        message_list = message_client.get_conversations_list(sdatetime=sdatetime)
        message_client.update_conversations(message_list=message_list)
    print(f"{start_date} ~ {end_date} 데이터 처리가 완료됐습니다.")
