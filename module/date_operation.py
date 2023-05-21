from datetime import datetime
from typing import List
from dateutil.relativedelta import relativedelta
import pandas as pd


def get_daily_datelist(start_date: str, end_date: str) -> List:
    """
    수집 일자 리스트입니다.
    회당 메시지 요청 제한량이 있기 때문에(최대1000회) 보수적으로 일 단위로 기간을 잡았습니다.
    EX)2023-04-01 00:00:00 ~ 2023-04-01 23:59:59
    Parameters
    ----------
    start_date : str
        시작일자를 문자열로 넣어줍니다.
    end_date : str
        종료일자를 문자열로 넣어줍니다.

    Returns
    -------
    List
        일자 리스트를 반환합니다.
    """
    start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    end_datetime = datetime.strptime(end_date, "%Y-%m-%d")
    date_list = pd.date_range(start=start_datetime, end=end_datetime).to_pydatetime().tolist()
    return date_list


def find_weekday_from_ranges(start_date: str, end_date: str, day: int) -> List:
    """
    start_date ~ end_date까지 기간 동안 주단위로 선택하 요일의 날짜를 datetime List로 반환합니다.

    Parameters
    ----------
    day : int
        요일을 의미합니다. 0(월) ~ 6(일)까지 입력할 수 있습니다.

    Returns
    -------
    List
        datetime List입니다.
    """
    date_list = []
    convert_startdate = datetime.strptime(start_date, "%Y-%m-%d").date()

    start = convert_startdate + relativedelta(days=-1)

    while start.strftime("%Y-%m-%d") < end_date:
        start = start + relativedelta(days=+1)
        if start.weekday() == day:
            date_list.append(start)

    return date_list
