import os
import yaml
from datetime import datetime, timedelta
import pymysql
import pandas as pd
import numpy as np
import re
import openpyxl
from openpyxl.styles import NamedStyle, Font, PatternFill, Alignment
from openpyxl.utils.dataframe import dataframe_to_rows
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
        self.window_sizes = self.config['params']['date']['window_sizes']

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

    def __exit__(self, exc_type, exc_value, traceback):
        # 필요한 정리 작업 (예: DB 연결 종료)
        if hasattr(self, 'conn'):
            self.conn.close()
        if hasattr(self, 'cursor'):
            self.cursor.close()

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
            window_sizes = self.window_sizes
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
        rlt = {}

        # 1일 대비 변화량 계산
        day_1 = today_col - values[1]
        day_1_per = '-' if values[1] == 0 else np.round((day_1 * 100) / values[1], 1)
        alert['day_delta'] = day_1
        alert['day_per'] = day_1_per

        # 평균 기준 계산
        for d in list(values.keys())[2:]:
            compari = values[d][1]
            change = today_col - compari
            if compari == 0:
                alert[f'{d}_day_delta'] = 0
                alert[f'{d}_day_per'] = 0
                alert[f'{d}_avg'] = 0
            else:
                percent = np.round((change * 100) / compari, 1)
                rlt.setdefault(d, (change, percent))
                alert[f'{d}_day_delta'] = change
                alert[f'{d}_day_per'] = percent
                alert[f'{d}_avg'] = compari
        return alert

    # 알람 조건
    def cond_of_alert(self, alert):
        if alert['7_avg'] == 0:
            percent = 0
        else:
            percent = np.round((alert['today_collect'] - alert['7_avg'])*100 / alert['7_avg'], 1)
        if abs(percent) >= self.threshold and abs(alert['7_avg']) > self.min_7_avg:
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

    def create_excel_style_slack_message(self, alert_list):
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
        ]
        blocks = []
        footer = [
            {
                "type": "divider"
            },
        ]

        for site_data in alert_list:
            block = {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{site_data['site_name']}* \n"
                            f" 전일 대비:\t{site_data['day_delta']}({site_data['day_per']}%) \n"
                            f" 7일 대비:\t{site_data['7_day_delta']}({site_data['7_day_per']}%) \n"
                            f" 14일 대비:\t{site_data['14_day_delta']}({site_data['14_day_per']}%) \n"
                            f" 30일 대비:\t{site_data['30_day_delta']}({site_data['30_day_per']}%) \n"
                            f" 60일 대비:\t{site_data['60_day_delta']}({site_data['60_day_per']}%)"
                },
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

    # 엑셀 저장 함수
    def excel_saver(self, datas):
        after_datas = {}
        for key, val in datas.items():
            after_datas.setdefault(key, {}).setdefault('당일', val[0])
            after_datas[key]['전일'] = val[1]
            # after_datas[key]['1일 대비 변화량'] = self.avg_calc(val[0], val[1])
            after_datas[key]['7일'] = val[7][1]
            after_datas[key]['14일'] = val[14][1]
            # after_datas[key]['14일 대비 변화량'] = self.avg_calc(val[0], val[14][1])
            after_datas[key]['30일'] = val[30][1]
            # after_datas[key]['30일 대비 변화량'] = self.avg_calc(val[0], val[30][1])
            after_datas[key]['60일'] = val[60][1]
            # after_datas[key]['60일 대비 변화량'] = self.avg_calc(val[0], val[60][1])
            after_datas[key]['7일 대비 변화량'] = self.avg_calc(val[0], val[7][1])

        df = pd.DataFrame(after_datas).T

        # 엑셀 저장
        with pd.ExcelWriter('sample.xlsx', engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='수집 데이터')

            # openpyxl로 엑셀 파일 접근
            workbook = writer.book
            worksheet = writer.sheets['수집 데이터']

            # 스타일 정의
            number_style = NamedStyle(name="number_style", number_format='#,##0')  # 숫자 형식 (천 단위 구분)
            date_style = NamedStyle(name="date_style", number_format='YYYY-MM-DD')  # 날짜 형식
            change_style = NamedStyle(name="change_style", number_format='#,##0.0%')  # 변화량 퍼센트 형식
            bold_font = Font(bold=True)  # Bold 글꼴 스타일
            align_center = Alignment(horizontal='center', vertical='center')  # 가운데 정렬
            align_right = Alignment(horizontal='right')
            align_left = Alignment(horizontal='left')

            # 음수 값에 파란색 배경 색상 적용
            blue_fill = PatternFill(start_color="ADD8E6", end_color="ADD8E6", fill_type="solid")
            blue_font = Font(color="0000FF")
            red_font = Font(color="FF0000")

            # 헤더 스타일 적용 (볼드 처리, 중앙 정렬)
            for cell in worksheet[1]:
                cell.font = bold_font
                cell.alignment = align_center

            # 스타일 적용: 각 열에 맞는 스타일 적용
            for row in worksheet.iter_rows(min_row=2, max_row=worksheet.max_row):
                for cell in row:
                    # 날짜 열 스타일 적용
                    if cell.column == 1:  # 첫 번째 열 (날짜 열)
                        cell.style = date_style
                        cell.font = bold_font
                        cell.alignment = align_left
                    # 변화량 열 스타일 적용
                    elif '변화량' in worksheet.cell(row=1, column=cell.column).value:  # 변화량 열
                        cell.style = change_style
                        cell.alignment = align_right
                        number = int(re.sub(r"\(.*\)", "", cell.value).strip())
                        # print(number, type(number))
                        if number < 0:
                            cell.font = blue_font
                        elif number > 0:
                            cell.font = red_font

                    else:
                        cell.style = number_style
                        cell.alignment = align_right



            # 1행의 헤더 열 폭을 설정
            for col in worksheet.iter_cols(min_row=1, max_row=1, min_col=1, max_col=worksheet.max_column):
                for cell in col:
                    # 각 열의 최대 길이를 계산
                    max_length = 0
                    column = cell.column_letter
                    for row in worksheet.iter_rows(min_row=2, max_row=worksheet.max_row):
                        for c in row:
                            if c.column == cell.column:  # 같은 열의 셀들에 대해 최대 길이 계산
                                try:
                                    if len(str(c.value)) > max_length:
                                        max_length = len(str(c.value))
                                except:
                                    pass
                    adjusted_width = (max_length + 2)  # 여유 공간을 2만큼 더 추가하여 너비 설정
                    worksheet.column_dimensions[column].width = adjusted_width

    def avg_calc(self, today, next):
        amount_of_change = today - next
        if next == 0:
            return f"{amount_of_change}({amount_of_change * 100}%)"
        return f"{amount_of_change}({np.round(amount_of_change * 100 / next, 1)}%)"

    def start(self):
        db_datas = self.extract_data()
        if len(db_datas):
            prepro_datas = self.data_preprocessing(db_datas)
            # 중간, 평균, 표준편차 데이터 추출
            mas_datas = self.calc_mid_avg_std(prepro_datas)
            self.excel_saver(mas_datas)
            alert_list = self.is_alert(mas_datas)
            # slack_msg = self.create_slack_message(alert_list)
            slack_msg = self.create_excel_style_slack_message(alert_list)
            self.slack_msg(slack_msg)

def start_monitoring(**kwargs):
    with CollectionVolumeMonitor(kwargs['config_f']) as cvm:
        cvm.start()
        return 0

if __name__ == '__main__':
    _config_f = 'collection_volume_monitor.yaml'
    start_monitoring(config_f=_config_f)



