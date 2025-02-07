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
        db_params = self.config.get('database', {})
        self.db_host = db_params.get('host', '')
        self.db_user = db_params.get('user', '')
        self.db_password = db_params.get('password', '')
        self.db_database = db_params.get('database', '')

        # 데이터 로드
        df = pd.read_excel('C:/work/voc/monitoring/사이트명.xlsx', sheet_name='이벤트')

        # 날짜를 datetime 형식으로 변환
        df['날짜'] = pd.to_datetime(df['날짜']).dt.date

        # 사이트 번호별로 날짜와 이슈 매핑
        self.site_names = df.groupby('사이트 번호').apply(lambda x: dict(zip(x['날짜'], x['이슈']))).to_dict()

        # 사이트 번호를 2자리로 맞추기
        self.site_names = {str(key).zfill(3): value for key, value in self.site_names.items()}

        # 이벤트 DB에 저장
        self.insert_event()

        # 날짜 설정
        date_time = datetime.today()
        today = date_time.date()
        params = self.config.get('params')
        self.select_date = params.get('select_date', today)
        self.y_axis = params.get('y_axis', 1000)

        # 해당 주의 월요일을 찾기
        start_of_week = self.select_date - timedelta(days=self.select_date.weekday())

        # 2주 전 월요일의 시작 날짜
        start_date = (start_of_week - timedelta(days=today.weekday() + 14))

        # 2주 전 일요일의 끝 날짜
        end_date = start_date + timedelta(days=13)

        # 시작 날짜와 종료 날짜 포맷
        self.start_date, self.end_date = start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

        # event 추출
        self.event_list = self.extract_event()

        # 데이터 추출
        self.data = self.extract_data()

        # 이벤트(이슈) 테이블 생성에 사용할 데이터 전처리
        self.event_table_data = self.data_preprocessing()

        # 레이아웃 설정
        self.setup_layout()

        # 콜백 설정
        self.setup_callbacks()

    # MySQL에서 수집 데이터 추출
    def extract_data(self):
        try:
            # MySQL 연결
            with pymysql.connect(
                    host=self.db_host,
                    user=self.db_user,
                    password=self.db_password,
                    database=self.db_database,
                    charset='utf8'
            ) as conn:

                # 커서 생성
                with conn.cursor() as cursor:
                    site_list = self.event_list

                    # SQL 쿼리 작성
                    execute_query = '''
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

                    # 각 사이트별로 데이터 추출
                    for site in site_list:
                        cursor.execute(execute_query, (
                            f"{site}%",
                            self.start_date,
                            self.end_date,
                        ))
                        sql_data = cursor.fetchall()

                        # 데이터를 딕셔너리에 매핑
                        for inner_site, date, collect in sql_data:
                            if inner_site not in rlt_dict:
                                rlt_dict[inner_site] = []
                            rlt_dict[inner_site].append((date, collect))

            return rlt_dict

        except Exception as e:
            print(f"DB 연결 실패: {e}")
        finally:
            print("DB 연결 종료!")

    # MySQL에서 이벤트 데이터 삽입
    def insert_event(self):
        try:
            # MySQL 연결
            with pymysql.connect(
                    host=self.db_host,
                    user=self.db_user,
                    password=self.db_password,
                    database=self.db_database,
                    charset='utf8'
            ) as conn:

                # 커서 생성
                with conn.cursor() as cursor:
                    # SQL 쿼리 작성
                    create_table_query = '''
                        CREATE TABLE IF NOT EXISTS events (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            site_number CHAR(3) NOT NULL,
                            event_date DATE NOT NULL,
                            description VARCHAR(2048) DEFAULT '',
                            UNIQUE (site_number, event_date),
                            INDEX idx_event_date (event_date)
                        );
                    '''
                    # 쿼리 실행
                    cursor.execute(create_table_query)

                    # 변경 사항 커밋
                    conn.commit()

                    insert_query = '''
                        INSERT INTO 
                            events
                        (event_date, description, site_number)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE event_date = event_date;
                    '''

                    # 삽입할 데이터 준비
                    data_to_insert = [
                        (date, issue, site_num)
                        for site_num, values in self.site_names.items()
                        for date, issue in values.items()
                    ]

                    # 여러 행 삽입
                    cursor.executemany(insert_query, data_to_insert)
                    conn.commit()

        except Exception as e:
            print(f"DB 연결 실패: {e}")
        finally:
            print("DB 연결 종료!")

    # MySQL에서 지정 기간 내 이벤트 데이터 추출
    def extract_event(self):
        try:
            # MySQL 연결
            with pymysql.connect(
                    host=self.db_host,
                    user=self.db_user,
                    password=self.db_password,
                    database=self.db_database,
                    charset='utf8'
            ) as conn:
                # 커서 생성
                with conn.cursor() as cursor:
                    # SQL 쿼리 작성
                    execute_query = '''
                        SELECT
                            site_number,
                            event_date,
                            description
                        FROM 
                            events
                        WHERE
                            event_date BETWEEN %s AND %s
                        GROUP BY
                            site_number,
                            event_date
                        ORDER BY
                            event_date DESC;
                            '''

                    # 각 사이트별로 데이터 추출
                    cursor.execute(execute_query, (
                        self.start_date,
                        self.end_date,
                    ))
                    sql_data = cursor.fetchall()

                    rlt_dict = {}

                    for site_number, event_date, description in sql_data:
                        # 키가 없으면 빈 딕셔너리로 초기화
                        if site_number not in rlt_dict:
                            rlt_dict[site_number] = {}
                        # 중첩 딕셔너리 구성
                        rlt_dict[site_number][event_date] = description

            return rlt_dict

        except Exception as e:
            print(f"DB 연결 실패: {e}")
        finally:
            print("DB 연결 종료!")

    # 테이블 데이터 전처리
    def data_preprocessing(self):
        event_table_datas = {}
        for keys in self.data.keys():
            event_table_datas[keys] = self.event_list[keys[:3]]

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

    # Dash 레이아웃 설정
    def setup_layout(self):
        self.app.layout = html.Div([
            html.Div(children=f'{self.select_date} 기준 이벤트 변화 모니터링 대시보드', style={'fontSize': 24, 'fontWeight': 'bold'}),
            html.Hr(),
            dcc.Interval(id='interval-component', interval=60 * 1000, n_intervals=0),
            dcc.Graph(
                id='controls-and-graph-1',
            ),
            dcc.Graph(
                id='controls-and-graph-2',
            ),
            html.Div(children=f'이벤트 차트'),
            html.Hr(),
            html.Div([
                dash_table.DataTable(
                    data=self.event_table_data,  # 데이터 삽입
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

    # 콜백 함수 설정
    def setup_callbacks(self):
        @self.app.callback(
            [Output(component_id='controls-and-graph-1', component_property='figure'),
             Output(component_id='controls-and-graph-2', component_property='figure')],
            [Input(component_id='interval-component', component_property='n_intervals')]  # Interval 트리거
        )
        def update_graph(n_intervals):
            fig1 = go.Figure()  # 수집량 기준치 초과 그래프
            fig2 = go.Figure()  # 수집량 기준치 이하 그래프

            # 데이터 처리 및 그래프 생성
            for site_name, site_data in self.data.items():
                site_n = str(site_name[:3])
                dates = [entry[0] for entry in site_data]
                values = [entry[1] for entry in site_data]

                # 특정 날짜 강조를 위한 마커 설정
                highlight_dates = list(self.event_list.get(site_n, {}).keys())
                marker_sizes = [15 if date in highlight_dates else 5 for date in dates]
                marker_colors = ['red' if date in highlight_dates else 'blue' for date in dates]

                # 수집량 > 기준치 경우 첫 번째 그래프에 추가
                if self.get_avg(site_data) > self.y_axis:
                    fig1.add_trace(go.Scatter(
                        x=dates,
                        y=values,
                        mode='lines+markers',
                        name=f"{site_name}",
                        marker=dict(size=marker_sizes, color=marker_colors),
                        hovertemplate=self.get_hover_template(dates, values, site_n, highlight_dates)
                    ))

                # 수집량 <= 기준치 경우 두 번째 그래프에 추가
                else:
                    fig2.add_trace(go.Scatter(
                        x=dates,
                        y=values,
                        mode='lines+markers',
                        name=f"{site_name}",
                        marker=dict(size=marker_sizes, color=marker_colors),
                        hovertemplate=self.get_hover_template(dates, values, site_n, highlight_dates)
                    ))

            if len(fig1.data) == 0 and len(fig2.data) == 0:
                # 데이터가 없는 경우 기본 그래프 생성
                fig1.add_trace(go.Scatter(x=[], y=[], mode='markers', name="No Data"))
                fig2.add_trace(go.Scatter(x=[], y=[], mode='markers', name="No Data"))
                fig1.update_layout(title="No Data Available")
                fig2.update_layout(title="No Data Available")
            else:
                if len(fig1.data) > 0:
                    fig1.update_layout(
                        self.get_common_layout(f'수집량 평균 {self.y_axis} 초과', 1)
                    )
                if len(fig2.data) > 0:
                    fig2.update_layout(
                        self.get_common_layout(f'수집량 평균 {self.y_axis} 이하', 0)
                    )

            return fig1, fig2

    # 공통 레이아웃 설정 함수
    def get_common_layout(self, title, mode):
        if mode:
            y_axis=dict(
                title='수집량',
                side='left',
                type='log',
                tickmode='auto'
            )
        else:
            y_axis = dict(
                title='수집량',
                side='left',
                type='log',
                tickmode='array',  # 눈금 값을 배열로 설정
                tickvals=[100, 300, 400, 500, 600, 700, 1000],  # Y2 축에 표시할 값
                ticktext=['100', '300', '400', '500', '600', '700', '1000'],  # Y2 축 눈금 텍스트
            )
        return dict(
            title=title,
            xaxis=dict(
                title='날짜',
                tickformat='%Y-%m-%d',
                tickmode='linear',
                tick0=self.start_date,
                dtick="D1",
                tickangle=90,
                ticks='inside',
            ),
            yaxis=y_axis,
            template='plotly_white',
            showlegend=True,
            legend=dict(
                font=dict(size=12),  # 범례 글꼴 크기
                itemwidth=40,  # 범례 항목의 너비
                traceorder="normal",  # 범례 항목 순서 설정 (normal 또는 reversed)
                itemsizing='constant'  # 범례 아이콘 크기 고정
            ),
            margin=dict(r=100),  # 오른쪽 여백 추가 (범례와 그래프 사이 간격)
        )

    # hover template을 간단하게 처리
    def get_hover_template(self, dates, values, site_n, highlight_dates):
        return [
            f'<b>날짜:</b> {date.strftime("%Y-%m-%d")}<br><b>수집량:</b> {value}<extra></extra>'
            if date not in highlight_dates
            else f'<b>날짜:</b> {date.strftime("%Y-%m-%d")}<br><b>수집량:</b> {value}<br><b>이벤트:</b> {self.event_list[site_n].get(date)}<extra></extra>'
            for date, value in zip(dates, values)
        ]

    # 전체 수집량 평균 구하기
    @staticmethod
    def get_avg(site_data):
        return sum(entry[1] for entry in site_data) / len(site_data)

    # Dash 앱 실행
    def run(self, debug=True):
        self.app.run(debug=debug)


# 애플리케이션 실행
if __name__ == '__main__':
    _config_f = 'msa.yaml'
    app_instance = MonitoringChart(config_f=_config_f)
    app_instance.run()
