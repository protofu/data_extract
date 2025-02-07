import os
import yaml
import datetime
import pymysql
import numpy as np
import logging
logging.basicConfig(level=logging.DEBUG)

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

class CollectionVolumeMonitor():
    def __init__(self, config_f):
        if not os.path.exists(config_f):
            raise IOError(f'Cannot read config file "{config_f}"')
        with open(config_f, encoding='utf-8') as ifp:
            self.config = yaml.load(ifp, yaml.SafeLoader)
        # 슬랙 변수 관련 설정
        self.slack_oauth_token = self.config["params"]['slack']["OAuth_token"]
        self.slack_mention_id = self.config["params"]['slack']["user_id"]
        self.slack_channel = self.config["params"]['slack']["channel"]
        self.analysis_date = ''
        self.site_number = ''
        self.site_group = ''
        self.site_name = ''
        self.keyword = ''
        self.collection_vol = ''
        self.threshold = ''
        self.day_collection_vol = ''
        self.col_cng_7 = ''
        self.col_cng_14 = ''
        self.col_cng_30 = ''
        self.col_cng_60 = ''
        self.cng_per_7 = ''
        self.cng_per_14 = ''
        self.cng_per_30 = ''
        self.cng_per_60 = ''


        # DB 관련 변수 설정
        self.db_host = self.config['database']['host']
        self.db_user = self.config['database']['user']
        self.db_password = self.config['database']['password']
        self.db_database = self.config['database']['database']

        # 분석 관련 변수 설정
        self.site_name_stats = self.config['database']['site_name_stats']
        self.standard_date = '2024-09-01'

        date_time = datetime.datetime.today()
        self.today = date_time.date()
        print(self.today)
        self.cond = self.config['params']['analysis']
        for days, value in self.cond.items():
            rlt = date_time - datetime.timedelta(days=value)
            setattr(self, days, rlt)

        one_day_ago = date_time - datetime.timedelta(days=1)
        print(date_time.strftime("%H:%M:%S"))
        self.end_date = (date_time - datetime.timedelta(days=420)).date()
        print(self.end_date)

    def __enter__(self):
        return self

    # MySQL로 부터 데이터 추출,
    def extract_data(self):
        try:
            # MySQL 연결
            conn = pymysql.connect(
                host=self.db_host,
                user=self.db_user,
                password=self.db_password,
                database=self.db_database,
                charset='utf8'
            )
            print("DB 연결 성공!")
            # 커서 생성
            cursor = conn.cursor()
            # 데이터 추출 쿼리
            excute_query = f'''
                SELECT
                    site_number_and_name, 
                    collection_end_date,
                    COUNT(*) AS total_count
                FROM 
                    merge
                WHERE
                    site_name_stats = %s AND
                    collection_end_date < %s
                GROUP BY
                    site_number_and_name,
                    collection_end_date
                ORDER BY
                    collection_end_date DESC;
            '''
            cursor.execute(excute_query, (
                self.site_name_stats,
                self.standard_date,
            ))

            rows = cursor.fetchall()
            for row in rows:
                print(row)
            # print('길이 => ', len(rows))
            cursor.close()
            conn.close()
            print("DB 연결 종료!")
            return rows

        except Exception as e:
            print(f"DB 연결 실패: {e}")
        finally:
            # 커서 종료
            if hasattr(self, 'cursor'):
                self.cursor.close()
            # 연결 종료
            if hasattr(self, 'conn'):
                self.conn.close()
                print("DB 연결 종료!")

    # 추출된 데이터 변수 초기화
    def data_preprocessing(self, datas):
        # 튜플 형태 -> 첫 요소 가져온 뒤 분해하여 변수 초기화
        num_group_name = str(datas[0][0]).split('_')
        print(num_group_name)
        self.site_number = num_group_name[0]
        self.site_group = num_group_name[1]
        self.site_name = num_group_name[2]
        self.day_collection_vol = datas[0][2]

    # 이동 평균 구하기
    def moving_average(self, datas):
        # 날짜와 값 추출
        dates = [row[1] for row in datas]
        values = [row[2] for row in datas]

        # numpy 배열로 변환
        dates_np = np.array(dates)
        values_np = np.array(values)

        # 이동평균 계산
        window_sizes = [1, 7, 14, 30, 60]
        moving_avgs = {window_size: [] for window_size in window_sizes}

        # 각 날짜를 기준으로 과거 N일 간의 평균을 계산
        for i in range(len(values_np)):
            for window_size in window_sizes:
                if i + window_size >= len(values_np):
                    moving_avgs[window_size].append(None)
                else:
                    # 기간을 넘지 않게 slice 하여 평균 계산
                    end_idx = max(0, i + window_size)
                    avg = np.mean(values_np[i:end_idx])
                    moving_avgs[window_size].append(int(np.round(avg)))

        # 변화량 변수 초기화
        self.amount_of_change(moving_avgs)

        for m_a, val in moving_avgs.items():
            # print(m_a, val)
            print(f'{m_a}일')
            # None을 제외한 값들만 추출
            val_clean = [v for v in val if v is not None]
            if m_a == 1:
                print(val_clean)
            # 중간값 계산
            median_val = np.round(np.median(val_clean))
            # 평균값 계산
            avg_val = np.round(np.average(val_clean))
            # 표준편차 계산
            std_val = np.round(np.std(val_clean))

            print('중간값 : ', median_val)
            print('평균값 : ', avg_val)
            print('표준편차 : ', std_val)

    # 변화량 계산
    def amount_of_change(self, moving_avgs):
        # 딕셔너리로 결과 저장
        col_cng_dict = {}
        cng_per_dict = {}

        # 각 이동 평균에 대해 반복
        for days in [7, 14, 30, 60]:
            col_cng_dict[days] = self.day_collection_vol - moving_avgs[days][0]
            cng_per_dict[days] = round((col_cng_dict[days] / moving_avgs[days][0]) * 100, 1)

        # 결과를 객체 속성에 할당
        self.col_cng_7 = col_cng_dict[7]
        self.col_cng_14 = col_cng_dict[14]
        self.col_cng_30 = col_cng_dict[30]
        self.col_cng_60 = col_cng_dict[60]

        self.cng_per_7 = cng_per_dict[7]
        self.cng_per_14 = cng_per_dict[14]
        self.cng_per_30 = cng_per_dict[30]
        self.cng_per_60 = cng_per_dict[60]

    # 표준편차 계산
    def avg_having_duration(self, datas):
        days_date = datetime.datetime.strptime(self.standard_date, "%Y-%m-%d").date()
        print(days_date)
        print(self.cond)

        for days, value in self.cond.items():
            while self.end_date < days_date - datetime.timedelta(days=value):
                vs = days_date - datetime.timedelta(days=value)
        # for days in self.cond:
        #     count = 0
        #     print(f'{getattr(self, days).date()}  --  {days_date}')
        #     for data in datas:
        #         if getattr(self, days).date() > data[1] >= days_date:
        #             count += 1
        #     print(days, ' = ', count)

    def slack_msg(self):
        # OAuth 토큰 설정
        slack_token = self.slack_oauth_token
        client = WebClient(token=slack_token)

        # 멘션할 사용자 ID
        mention_user_id = self.slack_mention_id

        # slack_msg = f'''
        # *분석 기준일*: {self.today}
        # *사이트 넘버*: {self.site_number}
        # *사이트 그룹*: {self.site_group}
        # *사이트 명*: {self.site_name_stats}
        # *검색 키워드*: 전체
        # *임계치*: + 12%
        # *분석 기준일 수집량*: {self.day_collection_vol}
        #
        # 2020년 O월 O일 OOO 사이트 수집량 변화 감지
        #
        # *사이트명*             *전일 대비*       *7일 대비*      *14일 대비*     *30일 대비*
        # =======================================================================
        # {self.site_name}    {self.col_cng_7}({self.cng_per_7}%)   {self.col_cng_14}({self.cng_per_14}%)   {self.col_cng_30}({self.cng_per_30}%)   {self.col_cng_60}({self.cng_per_60}%)
        #
        # *네이버 배달세상*
        # *전일 대비*: +100 (+10%) / 임계치 초과
        # *7일 평균 대비*: +20 (+OO%) / 임계치 초과
        # '''

        try:
            response = client.chat_postMessage(
                channel=self.slack_channel,  # 채널ID는 채널 세부정보 열기, 하단에서 확인가능
                blocks=[
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*2020년 O월 O일 OOO 사이트 수집량 변화 감지*"
                        }
                    },
                    {
                        "type": "divider"
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*사이트명*    *전일 대비*    *7일 대비*    *14일 대비*    *30일 대비*"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"{self.site_name}    {self.col_cng_7}({self.cng_per_7}%)    {self.col_cng_14}({self.cng_per_14}%)    {self.col_cng_30}({self.cng_per_30}%)    {self.col_cng_60}({self.cng_per_60}%)"
                        }
                    },
                    {
                        "type": "divider"
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*네이버 배달세상*\n*전일 대비*: +100 (+10%) / 임계치 초과\n*7일 평균 대비*: +20 (+OO%) / 임계치 초과"
                        }
                    }
                ]
            )
        except SlackApiError as e:
            assert e.response["error"]

    def start(self):
        db_datas = self.extract_data()
        if len(db_datas):
            self.data_preprocessing(db_datas)
            # 평균
            self.moving_average(db_datas)
            # self.avg_having_duration(db_datas)
        self.slack_msg()

    def __exit__(self, exc_type, exc_value, traceback):
        # 필요한 정리 작업 (예: DB 연결 종료)
        if hasattr(self, 'conn'):
            self.conn.close()
        if hasattr(self, 'cursor'):
            self.cursor.close()

def start_monitoring(**kwargs):
    with CollectionVolumeMonitor(kwargs['config_f']) as cvm:
        cvm.start()
        return 0

if __name__ == '__main__':
    _config_f = 'collection_volume_monitor.yaml'
    start_monitoring(config_f=_config_f)



