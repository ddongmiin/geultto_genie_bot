# Genie Season2
- [우당탕탕 슬랙 메시지 저장기(1) - 슬랙 메시지 넌 누구냐?](https://gibles-deepmind.tistory.com/entry/%EC%9A%B0%EB%8B%B9%ED%83%95%ED%83%95-%EC%8A%AC%EB%9E%99-%EB%A9%94%EC%8B%9C%EC%A7%80-%EC%A0%80%EC%9E%A5%EA%B8%B01-%EC%8A%AC%EB%9E%99-%EB%A9%94%EC%8B%9C%EC%A7%80-%EB%84%8C-%EB%88%84%EA%B5%AC%EB%83%90)
- [우당탕탕 슬랙 메시지 저장기(2) - 게시글과 쓰레드 조회하기](https://gibles-deepmind.tistory.com/entry/%EC%9A%B0%EB%8B%B9%ED%83%95%ED%83%95-%EC%8A%AC%EB%9E%99-%EB%A9%94%EC%8B%9C%EC%A7%80-%EC%A0%80%EC%9E%A5%EA%B8%B02-%EA%B2%8C%EC%8B%9C%EA%B8%80%EA%B3%BC-%EC%93%B0%EB%A0%88%EB%93%9C-%EC%A1%B0%ED%9A%8C%ED%95%98%EA%B8%B0)
## 시작 계기
- [Genie Season1](https://github.com/geultto/genie)
    - 글 제출 / 패스권 / 상호피드백 체킹에 활용
- 수집 범위를 확장하여
    - 글또 내 사용자 활동 로그(게시글, 쓰레드, 이모지)를 수집
    - 커뮤니티 활성화에 이용
## ETL 프로세스
- 수집 범위
    - 공개 채널의 게시글, 쓰레드, 이모지
- 배치 단위
    - 일 단위 
    - 다만, 하루가 지나서 댓글이나 이모지가 달리는 경우가 있으므로 적절한 주기로 upsert할 예정
- 프로세스
    1. 유저 리스트 호출 후 덮어쓰기(새로운 유저가 추가될 수 있음)
    2. 채널 리스트 호출 후 덮어쓰기(새로운 채널이 추가될 수 있음)
    3. 수집할 일자 설정
    4. 채널 별로 게시글 / 쓰레드 / 이모지 전처리
    5. 빅쿼리 로드
## 사용 테이블
`users`

| 필드 이름| 유형 | 의미 |
| :----------: | :---------: | :----------: |
| user_id    | STRING       | 유저id             |
| real_name    | STRING       | 성명          |
| display_name    | STRING       | 표시이름          |

`channels`

| 필드 이름| 유형 | 의미 |
| :----------: | :---------: | :----------: |
| channel_id    | STRING       | 채널id             |
| channel_name    | STRING       | 채널명          |
| num_member    | INTEGER       | 채널 인원수          |


`slack_conversation`

| 필드 이름| 유형 | 의미 |
| :----------: | :---------: | :----------: |
| channel_id    | STRING       | 채널id             |
| message_type    | STRING       | post, thread          |
| post_id    | STRING       | 게시글id(post, thread 동일)          |
| user_id    | STRING       | 유저id          |
| createtime    | DATETIME       | 게시글, 포스트 작성시간          |
| tddate    | DATE       | 게시글, 포스트 작성일자(파티션키)          |
| text    | STRING       | 게시글, 포스트 텍스트          |
| reactions    | STRING       | 이모지이름/이모지체크유저id/체크수          |