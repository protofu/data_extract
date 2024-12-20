from dash import dash, html, dash_table, dcc, callback, Output, Input
import pandas as pd
import plotly.graph_objects as go
import pymysql
import yaml
import os
from datetime import datetime, timedelta

class MonitoringChart:
    def __init__(self, config_f):
        # 설정 파일이 없으면 오류 발생
        if not os.path.exists(config_f):
            raise IOError(f'Cannot read config file "{config_f}"')

        # 설정 파일을 읽어서 self.config에 저장
        with open(config_f, encoding='utf-8') as ifp:
            self.config = yaml.load(ifp, yaml.SafeLoader)

        # Dash 애플리케이션 생성
        self.app = dash.Dash(__name__)

        # DB 설정 값 가져오기
        self.db_host = self.config['database']['host']
        self.db_user = self.config['database']['user']
        self.db_password = self.config['database']['password']
        self.db_database = self.config['database']['database']

        # 데이터 로드
        df = pd.read_excel('C:/work/voc/monitoring/사이트명.xlsx', sheet_name='이벤트')

        # 날짜를 datetime 형식으로 변환
        df['날짜'] = pd.to_datetime(df['날짜']).dt.date

        # 사이트 번호별로 날짜와 이슈 매핑
        self.site_names = df.groupby('사이트 번호').apply(lambda x: dict(zip(x['날짜'], x['이슈']))).to_dict()

        # 사이트 번호를 2자리로 맞추기
        self.site_names = {str(key).zfill(3): value for key, value in self.site_names.items()}

        # 날짜 설정
        self.select_date = self.config['params']['select_date']
        self.y_axis = self.config['params']['y_axis']

        # 오늘 날짜
        today = self.select_date if self.select_date else datetime.today()

        # 2주 전 월요일의 시작 날짜
        start_date = (today - timedelta(days=today.weekday() + 14))

        # 2주 전 일요일의 끝 날짜
        end_date = start_date + timedelta(days=13)

        # 시작 날짜와 종료 날짜 포맷
        self.start_date, self.end_date = start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

        # 데이터 추출
        self.datas = self.extract_data()

        # 이벤트(이슈) 테이블 생성에 사용할 데이터 전처리
        self.evnet_table_datas = self.data_preprocessing()
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

    # 테이블 데이터 전처리
    def data_preprocessing(self):
        event_table_datas = {}
        for keys in self.datas.keys():
            event_table_datas[keys] = self.site_names[keys[:3]]

        # DataTable에 표시할 형식으로 데이터 변환
        data_for_table = []

        # 키와 값을 순회하며 리스트로 변환
        for key, date_dict in event_table_datas.items():
            for date, issue in date_dict.items():
                data_for_table.append({
                    '이름': key,
                    '날짜': date.strftime('%Y-%m-%d'),  # 날짜를 문자열로 변환
                    '이슈': issue
                })

        # 날짜 기준 정렬(내림)
        data_for_table.sort(key=lambda x: datetime.strptime(x['날짜'], '%Y-%m-%d'))

        return data_for_table

    def setup_layout(self):
        # 애플리케이션의 레이아웃 정의
        self.app.layout = html.Div([
            html.Div(children='수집량 모니터링 대시보드', style={'fontSize': 24, 'fontWeight': 'bold'}),
            html.Hr(),
            dcc.Interval(id='interval-component', interval=60 * 1000, n_intervals=0),
            dcc.Graph(
                figure={},
                id='controls-and-graph'
            ),
            html.Hr(),
            html.Div(children=f'이벤트 차트'),
            html.Div([
                dash_table.DataTable(
                    data=self.evnet_table_datas,  # 데이터 삽입
                    columns=[  # 테이블 컬럼 정의
                        {'name': '이름', 'id': '이름'},
                        {'name': '날짜', 'id': '날짜'},
                        {'name': '이슈', 'id': '이슈'}
                    ],
                    page_size=5,  # 페이지 크기 설정

                    # 스타일 설정
                    style_table={'width': '800px', 'height': '400px', 'overflowY': 'auto'},  # 테이블의 높이와 스크롤 설정
                    style_cell={
                        'textAlign': 'left',  # 셀 텍스트 정렬
                        'padding': '10px',  # 셀 내 여백
                        'fontSize': '14px',  # 폰트 크기
                        'fontFamily': 'Arial',  # 폰트 설정
                        'border': '1px solid #ddd'  # 셀 경계 스타일
                    },
                    style_header={
                        'backgroundColor': '#f1f1f1',  # 헤더 배경색
                        'fontWeight': 'bold',  # 헤더 글꼴 굵기
                        'textAlign': 'center',  # 헤더 텍스트 정렬
                        'border': '1px solid #ddd'  # 헤더 경계 스타일
                    },
                    style_data={
                        'backgroundColor': '#fff',  # 데이터 셀 배경색
                        'color': 'black'  # 데이터 셀 텍스트 색상
                    },
                )
            ]),
        ])

    def setup_callbacks(self):
        # 콜백 함수 정의
        @self.app.callback(
            Output(component_id='controls-and-graph', component_property='figure'),
            [Input(component_id='interval-component', component_property='n_intervals')]  # Interval 트리거
        )
        def update_graph(_):
            # 모든 카페의 데이터를 그래프에 추가
            fig = go.Figure()

            for site_name, site_datas in self.datas.items():
                site_n = str(site_name[:3])
                dates = [entry[0] for entry in site_datas]
                values = [entry[1] for entry in site_datas]

                # 특정 날짜의 포인트를 강조하기 위한 마커 설정
                highlight_dates = list(self.site_names.get(site_n, {}).keys())  # dict_keys를 list로 변환
                marker_sizes = [
                    15 if date in highlight_dates else 5  # 강조할 날짜일 경우 크기 15, 아니면 5
                    for date in dates
                ]
                marker_colors = [
                    'red' if date in highlight_dates else 'blue'  # 강조할 날짜일 경우 빨간색, 아니면 파란색
                    for date in dates
                ]

                # 두 개의 Y축에 데이터를 분배
                if self.get_avg(site_datas) > self.y_axis:
                    yaxis_values = values

                    # 왼쪽 Y축 (수집량 > 2000)
                    fig.add_trace(go.Scatter(
                        x=dates,
                        y=yaxis_values,
                        mode='lines+markers',
                        name=f"{site_name}",
                        marker=dict(size=marker_sizes, color=marker_colors),
                        yaxis="y1",
                        hovertemplate=[
                            f'<b>날짜:</b> {date.strftime("%Y-%m-%d")}<br><b>수집량:</b> {value}<extra></extra>'
                            if date not in highlight_dates
                            else f'<b>강조된 날짜:</b> {date.strftime("%Y-%m-%d")}<br><b>수집량:</b> {value}<br><b>이벤트:</b> {self.site_names[site_n].get(date)}<extra></extra>'
                            for date, value in zip(dates, values)
                        ],
                    ))

                else:
                    yaxis2_values = values

                    # 오른쪽 Y축 (수집량 <= 2000)
                    fig.add_trace(go.Scatter(
                        x=dates,
                        y=yaxis2_values,
                        mode='lines+markers',
                        name=f"{site_name}",
                        marker=dict(size=marker_sizes, color=marker_colors),
                        yaxis="y2",
                        hovertemplate=[
                            f'<b>날짜:</b> {date.strftime("%Y-%m-%d")}<br><b>수집량:</b> {value}<extra></extra>'
                            if date not in highlight_dates
                            else f'<b>강조된 날짜:</b> {date.strftime("%Y-%m-%d")}<br><b>수집량:</b> {value}<br><b>이벤트:</b> {self.site_names[site_n].get(date)}<extra></extra>'
                            for date, value in zip(dates, values)
                        ],
                    ))

            # 레이아웃 설정 (두 개의 Y축)
            fig.update_layout(
                title=f'{self.select_date}\t수집량 추이',
                xaxis=dict(
                    title='날짜',
                    tickformat='%Y-%m-%d',
                    tickmode='linear',
                    tick0=self.start_date,
                    dtick="D1",
                    tickangle=90,
                    ticks='inside',
                ),
                yaxis=dict(
                    title='수집량',
                    side='left',
                    type='log'
                ),
                yaxis2=dict(
                    title='수집량',
                    overlaying='y',
                    side='right',
                    type='log',
                    tickmode='array',  # 눈금 값을 배열로 설정
                    tickvals=[1, 10, 50, 100, 300, 500, 700, 1000],  # Y2 축에 표시할 값
                    ticktext=['1', '10', '50', '100', '300', '500', '700', '1000'],  # Y2 축 눈금 텍스트
                ),
                template='plotly_white',
                legend=dict(
                    font=dict(size=12),  # 범례 글꼴 크기
                    itemwidth=40,  # 범례 항목의 너비
                    traceorder="normal",  # 범례 항목 순서 설정 (normal 또는 reversed)
                    itemsizing='constant',  # 범례 아이콘 크기 고정
                    x=1.05,  # 범례의 가로 위치 조정 (1보다 큰 값은 그래프 영역 밖으로 범례를 위치시킴)
                    y=1,  # 범례의 세로 위치 조정
                    xanchor='left',  # 범례의 기준점을 왼쪽으로 설정
                    yanchor='top',  # 범례의 기준점을 상단으로 설정
                ),
                margin=dict(r=100),  # 오른쪽 여백 추가 (범례와 그래프 사이 간격)
            )

            return fig

    # 전체 수집량의 평균 구하기
    def get_avg(self, site_datas):
        avg = sum(entry[1] for entry in site_datas) / len(site_datas)
        return avg

    def run(self, debug=True):
        # 앱 실행
        self.app.run(debug=debug)

if __name__ == '__main__':
    # 클래스 인스턴스 생성 및 실행
    _config_f = 'msa.yaml'
    app_instance = MonitoringChart(config_f=_config_f)
    app_instance.run()