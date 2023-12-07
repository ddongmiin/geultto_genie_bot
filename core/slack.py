import os
from typing import List, Dict
import json
from datetime import datetime
import ssl
import certifi
import time
from functools import wraps

import pandas as pd
from slack_sdk import WebClient
from slack_bolt import App


def retry(tries: int, delay: int, backoff: int):
    """
    재실행 데코레이터

    Parameters
    ----------
    tries : int
        최대 재시도 횟수
    delay : int
        재시도 사이의 대기시간(초)
    backoff : int
        재시도 시간을 점진적으로 증가시킴(초)
    """

    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while tries > 1:
                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    print(f"{str(e)}, {mdelay}초 후 재실행 예정")
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry

    return deco_retry


class SlackMessageRetriever:
    """
    슬랙 게시글 수집에 필요한 기능들을 정리했습니다.
    """

    def __init__(self, env_name: str):
        self.token = os.environ.get(env_name)
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())
        self.app = App(client=WebClient(token=self.token, ssl=self.ssl_context))

    def read_post_from_slack(
        self, start_unixtime: float, end_unixtime: float, channel_id: str
    ) -> List[Dict]:
        """
        conversations_history API를 이용해 슬랙 게시글을 불러옵니다.

        Parameters
        ----------
        start_unixtime : float
            게시글 시작일시입니다. Unixtime값을 넣어줘야 합니다.
        end_unixtime : float
            게시글 종료일시입니다. Unixtime값을 넣어줘야 합니다.
        channel_id : str
            수집할 채널명입니다.

        Returns
        -------
        List[Dict]
            list dict 형태로 게시글이 출력됩니다.
        """
        response = self.app.client.conversations_history(
            channel=channel_id,
            limit=1000,
            inclusive="true",
            oldest=start_unixtime,
            latest=end_unixtime,
        )
        return response["messages"]

    def read_permalink_from_slack(
        self,
        channel_id: str,
        message_ts: str,
    ) -> str:
        """
        슬랙의 게시글, 댓글 링크를 가져옵니다.

        Parameters
        ----------
        channel_id : str
            수집할 채널 명입니다.
        message_ts : str
            게시글 작성 시간(unixtime)을 넣어줘야 헤당 게시글의 링크를 확인할 수 있습니다.

        Returns
        -------
        str
            게시글 링크입니다.
        """
        return self.app.client.chat_getPermalink(
            channel=channel_id, message_ts=message_ts
        )

    def read_thread_from_slack(
        self,
        channel_id: str,
        thread_ts: float,
    ) -> List[Dict]:
        """
        conversations_replies API를 이용해 슬랙 게시글의 쓰레드를 불러옵니다.

        Parameters
        ----------
        channel_id : str
            수집할 채널 명입니다.
        thread_ts : float
            게시글 작성 시간(unixtime)을 넣어줘야 헤당 게시글의 쓰레드를 확인할 수 있습니다.

        Returns
        -------
        List[Dict]
            list dict 형태로 출력됩니다.
            0번째 인덱스는 게시글이므로 제외합니다.
        """
        thread = self.app.client.conversations_replies(
            channel=channel_id,
            ts=thread_ts,
        )
        return thread["messages"]

    def read_reactions_from_slack(self, channel_id: str, ts: str) -> List[Dict]:
        """
        슬랙 게시글/댓글의 이모지를 가져옵니다.

        Parameters
        ----------
        channel_id : str
            채널 Id입니다.
        ts : str
            이모지를 가져올 게시글/댓글의 Unixtime입니다.

        Returns
        -------
        List[Dict]
            이모지 이름, 유저, 카운트를 불러옵니다.
        """

        return self.app.client.reactions_get(
            channel=channel_id,
            timestamp=ts,
            full=True,
        )

    def read_users_from_slack(self) -> List[Dict]:
        """
        유저 리스트를 불러옵니다.

        Returns
        -------
        List[Dict]
            list dict 형태로 유저 리스트를 확인할 수 있습니다.
        """
        return self.app.client.users_list()["members"]

    def read_channels_from_slack(
        self,
    ) -> List[Dict]:
        """
        채널 리스트를 불러옵니다.

        Returns
        -------
        List[Dict]
            list dict 형태로 채널 리스트를 확인할 수 있습니다.
        """
        return self.app.client.conversations_list()["channels"]

    def message_for_private(self, users: List, text: str) -> None:
        """
        _summary_

        Returns
        -------
        None
            채널로 메시지를 보냅니다.
        """
        messaged_users = []
        for user in users:
            try:
                dm_channel_id = self.app.client.conversations_open(users=user)[
                    "channel"
                ]["id"]
                self.app.client.chat_postMessage(channel=dm_channel_id, text=text)
            except:
                time.sleep(5)
                dm_channel_id = self.app.client.conversations_open(users=user)[
                    "channel"
                ]["id"]
                self.app.client.chat_postMessage(channel=dm_channel_id, text=text)
            messaged_users.append(f"{user}")
        print(messaged_users)

    def convert_post_to_dict(self, channel_id: str, post: Dict) -> Dict:
        """
        post dict내에서 필요한 정보만 수집합니다.

        Parameters
        ----------
        channel_id : str
            채널명은 dict안에 없으므로 파라미터로 추가합니다.
        post : Dict
            dict 형태의 post입니다.
        Returns
        -------
        Dict
            채널id, 메시지타입(post), 게시글id, 유저id, 작성시간, 작성일자, 게시글, 리액션(종류, 사람, 숫자)을 수집합니다.
        """

        reaction_dict = self.read_reactions_from_slack(
            channel_id=channel_id, ts=post["ts"]
        )

        return {
            "channel_id": channel_id,
            "message_type": "post",
            "post_id": post["user"]
            + "-"
            + datetime.fromtimestamp(float(post["ts"])).strftime(
                "%Y-%m-%d-%H-%M-%S-%f"
            ),
            "user_id": post["user"],
            "ts": post["ts"],
            "createtime": datetime.fromtimestamp(float(post["ts"])).strftime(
                "%Y-%m-%dT%H:%M:%S.%f"
            ),
            "tddate": datetime.fromtimestamp(float(post["ts"])).strftime("%Y-%m-%d"),
            "text": post["text"],
            "permalink": self.read_permalink_from_slack(
                channel_id=channel_id, message_ts=post["ts"]
            ),
            "reactions": json.dumps(
                [
                    {
                        "name": reaction_dict["name"],
                        "user_id": reaction_dict["users"],
                        "count": reaction_dict["count"],
                    }
                    for reaction_dict in post.get("reactions", [])
                ],
                ensure_ascii=False,
            ),
        }

    def convert_thread_to_dict(self, channel_id: str, thread: Dict) -> Dict:
        """
        thread dict내에서 필요한 정보만 수집합니다.

        Parameters
        ----------
        channel_id : str
            채널명은 dict안에 없으므로 파라미터로 추가합니다.
        thread : Dict
            dict 형태의 thread입니다.

        Returns
        -------
        Dict
            채널id, 메시지타입(thread), 게시글id, 유저id, 작성시간, 작성일자, 게시글, 리액션(종류, 사람, 숫자)을 수집합니다.
        """
        parent_user_id = (
            thread["parent_user_id"]
            if "parent_user_id" in thread.keys()
            else thread["root"]["user"]
        )

        reaction_dict = self.read_reactions_from_slack(
            channel_id=channel_id, ts=thread["ts"]
        )

        return {
            "channel_id": channel_id,
            "message_type": "thread",
            "post_id": parent_user_id
            + "-"
            + datetime.fromtimestamp(float(thread["thread_ts"])).strftime(
                "%Y-%m-%d-%H-%M-%S-%f"
            ),
            "user_id": thread["user"],
            "ts": thread["ts"],
            "createtime": datetime.fromtimestamp(float(thread["ts"])).strftime(
                "%Y-%m-%dT%H:%M:%S.%f"
            ),
            "tddate": datetime.fromtimestamp(float(thread["ts"])).strftime("%Y-%m-%d"),
            "text": thread["text"],
            "permalink": self.read_permalink_from_slack(
                channel_id=channel_id, message_ts=thread["ts"]
            ),
            "reactions": json.dumps(
                [
                    {
                        "name": reaction_dict["name"],
                        "user_id": reaction_dict["users"],
                        "count": reaction_dict["count"],
                    }
                    for reaction_dict in thread.get("reactions", [])
                ],
                ensure_ascii=False,
            ),
        }

    @staticmethod
    def convert_users_to_dict(user: Dict) -> Dict:
        """
        user dict안에서 필요한 정보만 수집합니다.

        Parameters
        ----------
        user : Dict
            dict 형태의 user입니다.

        Returns
        -------
        Dict
            유저id, 성명, 표시이름을 수집합니다.
        """
        return {
            "user_id": user["id"],
            "real_name": user["profile"]["real_name"],
            "display_name": user["profile"]["display_name"],
        }

    @staticmethod
    def convert_channels_to_dict(channel: Dict) -> Dict:
        """
        channel dict안에서 필요한 정보만 수집합니다.

        Parameters
        ----------
        channel : Dict
            dict 형태의 channel입니다.

        Returns
        -------
        Dict
            채널id, 채널명, 멤버숫자를 수집합니다.
        """
        return {
            "channel_id": channel["id"],
            "channel_name": channel["name"],
            "num_member": int(channel["num_members"]),
        }

    @retry(tries=6, delay=10, backoff=10)
    def fetch_and_process_posts(
        self,
        channel_id: str,
        posts: List[Dict],
    ) -> List[Dict]:
        """
        포스트와 포스트에 해당하는 쓰레드들을 List[Dict] 형태로 변환하여
        message_list를 만듭니다.

        Parameters
        ----------
        channel_id : str
            채널ID
        posts : List[Dict]
            게시글

        Returns
        -------
        List[Dict]
            게시글과 쓰레드
        """
        message_list = []
        for post in posts:
            message_list.append(
                SlackMessageRetriever.convert_post_to_dict(
                    self, channel_id=channel_id, post=post
                )
            )
            if "subtype" not in list(post.keys()) and "thread_ts" in list(post.keys()):
                thread_ts = post["thread_ts"]
                # 0번째 값은 게시글
                threads = self.read_thread_from_slack(
                    channel_id=channel_id, thread_ts=thread_ts
                )[1:]
                time.sleep(1.5)
                for thread in threads:
                    message_list.append(
                        SlackMessageRetriever.convert_thread_to_dict(
                            self, channel_id=channel_id, thread=thread
                        )
                    )
        return message_list

    @staticmethod
    def convert_message_to_dataframe(message_list: List[Dict]) -> pd.DataFrame:
        """
        message_list를 판다스 데이터프레임으로 변환합니다.
        변환 후, datetime64[ns] 형태로 tddate와 createtime의 컬럼 타입을 변경해줍니다.
        아래 문서의 4.2절에서 추론 타입을 확인할 수 있습니다.
        https://pandas-gbq.readthedocs.io/_/downloads/en/latest/pdf/

        Parameters
        ----------
        message_list : List[Dict]
            메시지 리스트(게시글과 쓰레드)

        Returns
        -------
        pd.DataFrame
            메시지 리스트를 판다스 데이터프레임으로
        """
        message_df = pd.DataFrame(message_list)
        if message_df.empty:
            return None
        message_df["tddate"] = pd.to_datetime(message_df["tddate"], format="%Y-%m-%d")
        message_df["createtime"] = pd.to_datetime(
            message_df["createtime"], format="%Y-%m-%dT%H:%M:%S.%f"
        )
        return message_df
