import os
import yaml
from datetime import datetime, timedelta
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


        # DB 관련 변수 설정
        self.db_host = self.config['database']['host']
        self.db_user = self.config['database']['user']
        self.db_password = self.config['database']['password']
        self.db_database = self.config['database']['database']

        # 분석 관련 변수 설정
        self.site_name_stats = self.config['database']['site_name_stats']
        self.standard_date = self.config['params']['date']['standard_date']
        self.limit_day = self.config['params']['date']['limit']
        self.data_call_end_date = self.standard_date - timedelta(days=self.limit_day)

        # 알람 조건 관련 설정
        self.threshold = self.config['params']['alert']['threshold']
        self.min_collect = self.config['params']['alert']['min_collect']
        self.min_7_avg = self.config['params']['alert']['min_7_avg']

        date_time = datetime.today()
        self.today = date_time.date()
        self.cond = self.config['params']['analysis']
        for days, value in self.cond.items():
            rlt = date_time - timedelta(days=value)
            setattr(self, days, rlt)
        one_day_ago = date_time - timedelta(days=1)
        self.end_date = (date_time - timedelta(days=420)).date()

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
                    collection_end_date BETWEEN %s AND %s 
                GROUP BY
                    site_number_and_name,
                    collection_end_date
                ORDER BY
                    collection_end_date DESC;
            '''
            cursor.execute(excute_query, (
                self.data_call_end_date,
                self.standard_date,
            ))

            rows = cursor.fetchall()
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
        prepro_datas = {}
        start_date = self.standard_date
        end_date = self.end_date
        for site, date, collect in datas:
            prepro_datas.setdefault(site, []).append((date, collect))
        for key in prepro_datas:
            date_range = [start_date - timedelta(days=i) for i in range((start_date - end_date).days + 1)]
            date_dict = {date: 0 for date in date_range}  # 기본값은 0
            for date, value in prepro_datas[key]:
                if date in date_dict:
                    date_dict[date] = value
            result = [(date, date_dict[date]) for date in date_range]
            prepro_datas[key] = result
        return prepro_datas
    # 평균, 중간, 표준편차 구하기
    def calc_mid_avg_std(self, datas):
        avg_std_mid = {}
        for site, values in datas.items():
            # 날짜와 값 추출
            collect = [row[1] for row in values]

            # numpy 배열로 변환
            collect_np = np.array(collect)
            # 이동평균 범위 변수값
            window_sizes = [0, 1, 7, 14, 30, 60]
            # 계산
            for window_size in window_sizes:
                # 0일은 당일 수집량 담기
                if window_size == 0:
                    avg_std_mid.setdefault(site, {}).setdefault(window_size, collect_np[0])
                # 1일은 전날 수집량을 담기
                elif window_size == 1:
                    avg_std_mid.setdefault(site, {}).setdefault(window_size, collect_np[1])
                else:
                    # 기간을 넘지 않게 slice 하여 평균 계산
                    end_idx = window_size + 1
                    a_val = np.average(collect_np[0:end_idx])
                    m_val = np.mean(collect_np[0:end_idx])
                    s_val = np.std(collect_np[0:end_idx])
                    # 2개의 값이 다 NaN이 아니면 기록
                    if not np.isnan(a_val) and not np.isnan(m_val) and not np.isnan(s_val):
                        avg_std_mid.setdefault(site, {}).setdefault(window_size, []).append(int(np.round(a_val)))
                        avg_std_mid.setdefault(site, {}).setdefault(window_size, []).append(int(np.round(m_val)))
                        avg_std_mid.setdefault(site, {}).setdefault(window_size, []).append(int(np.round(s_val, 2)))
                    else:
                        avg_std_mid.setdefault(site, {}).setdefault(window_size, -1)
        return avg_std_mid
    # 알람 데이터 판별
    def is_alert(self, datas):
        alert_list = []
        for site, val in datas.items():
            alert = {
                'site_name': site,
                'today_collect': val[0],
                'day_delta': None,
                '7_day_delta': None,
                '14_day_delta': None,
                '30_day_delta': None,
                '60_day_delta': None,
                'day_per': None,
                '7_day_per': None,
                '14_day_per': None,
                '30_day_per': None,
                '60_day_per': None,
                '7_avg': None,
                'footer': None
            }
            com_alert = self.amount_of_change(val, alert)
            rlt = self.cond_of_alert(com_alert)
            if rlt is not None:
                alert_list.append(rlt)
        return alert_list
    # 변화량 계산
    def amount_of_change(self, values, alert):
        # 기준일 수집량
        today_col = values[0]
        # 전날 수집량과 비교
        day_1 = today_col - values[1]
        if values[1] != 0:
            day_1_per = np.round((day_1*100) / values[1], 1)
        else:
            day_1_per = '-'
        alert['day_delta'] = day_1
        alert['day_per'] = day_1_per
        rlt = {}
        # 평균 기준 계산
        for d in list(values.keys())[2:]:
            compari = values[d][1]
            if compari == 0:
                rlt.setdefault(d, (0, 0))
                continue
            day_d = today_col - compari
            day_p = np.round((day_d * 100) / compari, 1)
            rlt.setdefault(d, (day_d, day_p))

        # 7일 대비
        alert['7_day_delta'] = rlt[7][0]
        alert['7_day_per'] = rlt[7][1]
        alert['7_avg'] = values[7][1]
        # 14일 대비
        alert['14_day_delta'] = rlt[14][0]
        alert['14_day_per'] = rlt[14][1]
        # 30일 대비
        alert['30_day_delta'] = rlt[30][0]
        alert['30_day_per'] = rlt[30][1]
        # 60일 대비
        alert['60_day_delta'] = rlt[60][0]
        alert['60_day_per'] = rlt[60][1]

        return alert
    # 알람 조건
    def cond_of_alert(self, alert):
        if alert['7_avg'] == 0:
            percent = 0
        else:
            percent = np.round((alert['today_collect'] - alert['7_avg'])*100 / alert['7_avg'], 1)
        if abs(percent) >= self.threshold and abs(alert['7_avg']) >= self.min_7_avg:
            alert['footer'] = f"7일 이동평균 기준 *변화량* 이 *{alert['7_day_delta']}* 건, *변화율* 이 *{percent} %* 입니다."
            return alert

    # Slack 메시지 포맷을 만들기 위한 함수
    def create_slack_message(self, alert_list):
        header = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{self.standard_date} 사이트 수집량 변화 감지*"
                }
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*사이트명*    *전일 대비*    *7일 대비*    *14일 대비*    *30일 대비*    *60일 대비*"
                }
            },
        ]
        blocks = []
        footer = [
            {
                "type": "divider"
            },
            # {
            #     "type": "section",
            #     "text": {
            #         "type": "mrkdwn",
            #         "text": f"*네이버 배달세상*\n*전일 대비*: {site_data['day_delta']} ({site_data['day_per']}%) / 임계치 초과\n*7일 평균 대비*: {site_data['7_day_delta']} ({site_data['7_day_per']}%) / 임계치 초과"
            #     }
            # }
        ]
        for site_data in alert_list:
            block = {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{site_data['site_name']}*    {site_data['day_delta']}({site_data['day_per']}%)    {site_data['7_day_delta']}({site_data['7_day_per']}%)    {site_data['14_day_delta']}({site_data['14_day_per']}%)    {site_data['30_day_delta']}({site_data['30_day_per']}%)    {site_data['60_day_delta']}({site_data['60_day_per']}%)"
                }
            }
            fot = {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{site_data['site_name']}* 의 {site_data['footer']}"

                }
            }
            blocks.append(block)
            footer.append(fot)
        return header + blocks + footer

    def slack_msg(self, slack_msg):
        # OAuth 토큰 설정
        slack_token = self.slack_oauth_token
        client = WebClient(token=slack_token)

        # 멘션할 사용자 ID
        # mention_user_id = self.slack_mention_id

        try:
            response = client.chat_postMessage(
                channel=self.slack_channel,  # 채널ID는 채널 세부정보 열기, 하단에서 확인가능
                blocks=slack_msg
            )
        except SlackApiError as e:
            assert e.response["error"]

    def start(self):
        db_datas = self.extract_data()
        if len(db_datas):
            prepro_datas = self.data_preprocessing(db_datas)
            # 중간, 평균, 표준편차 데이터 추출
            mas_datas = self.calc_mid_avg_std(prepro_datas)
            alert_list = self.is_alert(mas_datas)
            slack_msg = self.create_slack_message(alert_list)
            self.slack_msg(slack_msg)

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



