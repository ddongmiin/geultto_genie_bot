import os
from typing import List, Dict
import json
from datetime import datetime
import ssl
import certifi
import time

import pandas as pd
from slack_sdk import WebClient
from slack_bolt import App


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

    def read_users_from_slack(self) -> List[Dict]:
        """
        유저 리스트를 불러옵니다.

        Returns
        -------
        List[Dict]
            list dict 형태로 유저 리스트를 확인할 수 있습니다.
        """
        return self.app.client.users_list()["members"]

    def read_channels_from_slack(self) -> List[Dict]:
        """
        채널 리스트를 불러옵니다.

        Returns
        -------
        List[Dict]
            list dict 형태로 채널 리스트를 확인할 수 있습니다.
        """
        return self.app.client.conversations_list()["channels"]

    @staticmethod
    def convert_post_to_dict(channel_id: str, post: Dict) -> Dict:
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
        return {
            "channel_id": channel_id,
            "message_type": "post",
            "post_id": post["user"]
            + "-"
            + datetime.fromtimestamp(float(post["ts"])).strftime("%Y-%m-%d-%H-%M-%S-%f"),
            "user_id": post["user"],
            "createtime": datetime.fromtimestamp(float(post["ts"])).strftime(
                "%Y-%m-%dT%H:%M:%S.%f"
            ),
            "tddate": datetime.fromtimestamp(float(post["ts"])).strftime("%Y-%m-%d"),
            "text": post["text"],
            "reactions": json.dumps(
                [
                    {
                        "name": react_dict["name"],
                        "user_id": react_dict["users"],
                        "count": react_dict["count"],
                    }
                    for react_dict in post.get("reactions", [])
                ],
                ensure_ascii=False,
            ),
        }

    @staticmethod
    def convert_thread_to_dict(channel_id: str, thread: Dict) -> Dict:
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
        return {
            "channel_id": channel_id,
            "message_type": "thread",
            "post_id": parent_user_id
            + "-"
            + datetime.fromtimestamp(float(thread["thread_ts"])).strftime("%Y-%m-%d-%H-%M-%S-%f"),
            "user_id": thread["user"],
            "createtime": datetime.fromtimestamp(float(thread["ts"])).strftime(
                "%Y-%m-%dT%H:%M:%S.%f"
            ),
            "tddate": datetime.fromtimestamp(float(thread["ts"])).strftime("%Y-%m-%d"),
            "text": thread["text"],
            "reactions": json.dumps(
                [
                    {
                        "name": react_dict["name"],
                        "user_id": react_dict["users"],
                        "count": react_dict["count"],
                    }
                    for react_dict in thread.get("reactions", [])
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
                SlackMessageRetriever.convert_post_to_dict(channel_id=channel_id, post=post)
            )
            if "subtype" not in list(post.keys()) and "thread_ts" in list(post.keys()):
                thread_ts = post["thread_ts"]
                # 0번째 값은 게시글
                threads = self.read_thread_from_slack(channel_id=channel_id, thread_ts=thread_ts)[
                    1:
                ]
                time.sleep(1)
                for thread in threads:
                    message_list.append(
                        SlackMessageRetriever.convert_thread_to_dict(
                            channel_id=channel_id, thread=thread
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
