import os
import yaml
import datetime
import pymysql
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
                    site_number_and_name, collection_end_date 
                FROM 
                    merge 
                WHERE 
                    site_name_stats = %s AND collection_end_date BETWEEN %s AND %s
            '''
            cursor.execute(excute_query, (
                self.site_name_stats,
                self.end_date,
                self.standard_date,
            ))
            # cursor.execute("SELECT * FROM merge WHERE site_number_and_name = '001_네이버_배달세상';")
            rows = cursor.fetchall()
            # for row in rows:
            #     print(row)
            print('길이 => ', len(rows))
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

    # 추출된 데이터 전처리
    def data_preprocessing(self, datas):
        # 튜플 형태 -> 첫 요소 가져온 뒤 분해하여 변수 초기화
        num_group_name = str(datas[0][0]).split('_')

        self.site_number = num_group_name[0]
        self.site_group = num_group_name[1]
        self.site_name = num_group_name[2]
        self.day_collection_vol = len(datas)

    # 기간별 평균 구하기
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
        #     *분석 기준일* : 12월 11일\n*사이트 넘버* : 057\n*사이트 그룹* : 네이버\n*사이트 명* : 네이버\n*검색 키워드* : 전체\n*수집량* : 20140 건\n*임계치* : + 12%\n*분석 기준일 수집량* : 1002 건
        # '''
        slack_msg = f'''
        \t*분석 기준일*\t: {self.today}
        *사이트 넘버*\t: {self.site_number}
        *사이트 그룹*\t: {self.site_group}
        *사이트 명*\t: {self.site_name_stats}
        *검색 키워드*\t: 전체
        *수집량*\t: {self.day_collection_vol}
        *임계치*\t: + 12%
        *분석 기준일 수집량*\t: {self.day_collection_vol}
        '''

        try:
            response = client.chat_postMessage(
                channel=self.slack_channel,  # 채널ID는 채널 세부정보 열기, 하단에서 확인가능
                text=slack_msg,
            )
        except SlackApiError as e:
            assert e.response["error"]

    def start(self):
        db_datas = self.extract_data()
        if len(db_datas):
            self.data_preprocessing(db_datas)
            self.avg_having_duration(db_datas)
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
