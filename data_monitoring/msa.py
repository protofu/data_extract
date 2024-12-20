from dash import dash, html, dash_table, dcc, callback, Output, Input
import pandas as pd
import plotly.graph_objects as go
import pymysql
import yaml
import os
from datetime import datetime, timedelta

class MonitoringChart:
    def __init__(self, config_f):
        if not os.path.exists(config_f):
            raise IOError(f'Cannot read config file "{config_f}"')
        with open(config_f, encoding='utf-8') as ifp:
            self.config = yaml.load(ifp, yaml.SafeLoader)
        # Dash 인스턴스 생성
        self.app = dash.Dash(__name__)

        # DB 관련 변수 설정
        self.db_host = self.config['database']['host']
        self.db_user = self.config['database']['user']
        self.db_password = self.config['database']['password']
        self.db_database = self.config['database']['database']
        # 데이터 로드
        df = pd.read_excel('C:/work/voc/monitoring/사이트명.xlsx', sheet_name='이벤트')
        # 날짜를 datetime 형식으로 변환
        df['날짜'] = pd.to_datetime(df['날짜']).dt.date
        # 사이트 번호별로 날짜와 이슈를 매핑한 딕셔너리 생성
        self.site_names = df.groupby('사이트 번호').apply(lambda x: dict(zip(x['날짜'], x['이슈']))).to_dict()
        print(self.site_names)
        self.select_date = self.config['params']['select_date']
        # 오늘 날짜
        today = self.select_date if self.select_date else datetime.today()
        # 2주 전의 시작 날짜 (2주 전 월요일)
        start_date = (today - timedelta(days=today.weekday() + 14))  # 월요일 기준
        # 전주의 끝 날짜 (일요일)
        end_date = start_date + timedelta(days=13)  # 그 주의 일요일
        self.start_date, self.end_date = start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
        # 데이터 추출(사이트명, 날짜, 수집량)
        self.datas = self.extract_data()
        # 데이터 가공
        # self.pre_datas = self.data_preprocessing()

        # 레이아웃 설정
        self.setup_layout()
        # 콜백 설정
        self.setup_callbacks()

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

            # 커서 생성
            cursor = conn.cursor()
            site_list = self.site_names
            # 데이터 추출 쿼리
            excute_query = f'''
                SELECT
                    site_number_and_name,
                    collection_end_date,
                    COUNT(*) AS total_count
                FROM 
                    merge
                WHERE
                    site_number_and_name LIKE %s AND
                    collection_end_date BETWEEN %s AND %s
                GROUP BY
                    site_number_and_name,
                    collection_end_date
                ORDER BY
                    collection_end_date DESC;
            '''
            rlt_dict = {}

            for site in site_list:
                cursor.execute(excute_query, (
                    f"{site}%",
                    self.start_date,
                    self.end_date,
                ))
                sql_datas = cursor.fetchall()
                # 뽑은 데이터 1차 처리
                for site, date, collect in sql_datas:
                    if site not in rlt_dict:
                        rlt_dict[site] = []
                    rlt_dict[site].append((date, collect))

            cursor.close()
            conn.close()

            return rlt_dict

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

    def data_preprocessing(self):
        pass

    def setup_layout(self):
        # 애플리케이션의 레이아웃 정의
        self.app.layout = html.Div([
            html.Div(children='수집량 모니터링 대시보드', style={'fontSize': 24, 'fontWeight': 'bold'}),
            html.Hr(),
            dcc.RadioItems(
                options=[{'label': key, 'value': key} for key in self.datas.keys()],
                value='188_네이버_카페명',  # 기본값
                id='controls-and-radio-item',
                style={'marginBottom': '20px'}
            ),
            # dash_table.DataTable(
            #     data=self.df.to_dict('records'),
            #     page_size=6
            # ),
            dcc.Graph(
                figure={},
                id='controls-and-graph'
            )
        ])

    def setup_callbacks(self):
        # 콜백 함수 정의
        @self.app.callback(
            Output(component_id='controls-and-graph', component_property='figure'),
            Input(component_id='controls-and-radio-item', component_property='value')
        )
        def update_graph(cafe_name):
            # # 선택된 카페 데이터를 가져오기
            # cafe_data = self.datas[cafe_name]
            # dates = [entry[0] for entry in cafe_data]
            # values = [entry[1] for entry in cafe_data]
            #
            # # 꺾은선 그래프 생성
            # fig = go.Figure()
            # fig.add_trace(go.Scatter(x=dates, y=values, mode='lines+markers', name=cafe_name))
            #
            # fig.update_layout(title=f'{cafe_name} 수집량 추이',
            #                   xaxis_title='날짜',
            #                   yaxis_title='수집량',
            #                   template='plotly_white')

            # 모든 카페의 데이터를 그래프에 추가
            fig = go.Figure()

            for cafe_name, cafe_data in self.datas.items():
                site_n = int(cafe_name[:3])
                dates = [entry[0] for entry in cafe_data]
                values = [entry[1] for entry in cafe_data]

                # 특정 날짜의 포인트를 강조하기 위한 마커 설정
                highlight_dates = list(self.site_names.get(site_n, {}).keys())  # dict_keys를 list로 변환
                # 마커 설정 (강조 날짜와 나머지 날짜 구분)
                marker_sizes = [
                    15 if date in highlight_dates else 5  # 강조할 날짜일 경우 크기 15, 아니면 5
                    for date in dates
                ]
                marker_colors = [
                    'red' if date in highlight_dates else 'blue'  # 강조할 날짜일 경우 빨간색, 아니면 파란색
                    for date in dates
                ]

                # 각 카페 데이터를 꺾은선 그래프로 추가
                fig.add_trace(go.Scatter(
                    x=dates,
                    y=values,
                    mode='lines+markers',
                    name=cafe_name,
                    marker=dict(size=marker_sizes, color=marker_colors),
                    hovertemplate=[
                        # 강조된 날짜가 아닌 경우
                        f'<b>날짜:</b> {date.strftime("%Y-%m-%d")}<br><b>수집량:</b> {value}<extra></extra>'
                        if date not in highlight_dates
                        # 강조된 날짜인 경우
                        else f'<b>강조된 날짜:</b> {date.strftime("%Y-%m-%d")}<br><b>수집량:</b> {value}<br><b>이벤트:</b> {self.site_names[site_n].get(date)}<extra></extra>'
                        for date, value in zip(dates, values)
                    ],
                ))

            fig.update_layout(
                title=f'{self.select_date}\t모든 카페 수집량 추이',
                xaxis=dict(
                    title='날짜',
                    tickformat='%Y-%m-%d',  # 날짜 포맷을 설정
                    tickmode='linear',  # 날짜 간격을 선형으로 설정
                    tick0=self.start_date,  # 시작 날짜
                    dtick="D1",  # 1일 간격 (D1: Day 1),
                    tickangle=90,  # 각도 0도로 설정하여 중앙 정렬
                    ticks='inside',  # 축의 tick을 안쪽으로 배치
                ),
                yaxis_title='수집량',
                template='plotly_white'
            )

            return fig

    def run(self, debug=True):
        # 앱 실행
        self.app.run(debug=debug)

if __name__ == '__main__':
    # 클래스 인스턴스 생성 및 실행
    _config_f = 'msa.yaml'
    app_instance = MonitoringChart(config_f=_config_f)
    app_instance.run()