from dash import dash, html, dash_table, dcc, callback, Output, Input
import pandas as pd
import plotly.express as px
import pymysql
import yaml
import os

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
        # 날짜를 키로, 사이트 번호를 리스트로 변환
        self.result = df.groupby('날짜')['사이트 번호'].apply(list).to_dict()
        # 데이터 추출(사이트명, 날짜, 수집량)
        self.datas = self.extract_data()
        # 데이터 가공
        self.pre_datas = self.data_preprocessing()

        # # 레이아웃 설정
        # self.setup_layout()
        # # 콜백 설정
        # self.setup_callbacks()

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
            # 데이터 추출 쿼리
            excute_query = f'''
                SELECT
                    site_number_and_name, 
                    collection_end_date,
                    COUNT(*) AS total_count
                FROM 
                    merge
                GROUP BY
                    site_number_and_name,
                    collection_end_date
                ORDER BY
                    collection_end_date DESC;
            '''
            cursor.execute(excute_query, (

            ))

            rows = cursor.fetchall()
            cursor.close()
            conn.close()

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

    def data_preprocessing(self):
        pass

    def setup_layout(self):
        # 애플리케이션의 레이아웃 정의
        self.app.layout = html.Div([
            html.Div(children='수집량 모니터링 대시보드'),
            html.Hr(),
            dcc.RadioItems(
                options=['pop', 'lifeExp', 'gdpPercap'],
                value='lifeExp',
                id='controls-and-radio-item'
            ),
            dash_table.DataTable(
                data=self.df.to_dict('records'),
                page_size=6
            ),
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
        def update_graph(col_chosen):
            fig = px.histogram(self.df, x='continent', y=col_chosen, histfunc='avg')
            return fig

    def run(self, debug=True):
        # 앱 실행
        self.app.run(debug=debug)

if __name__ == '__main__':
    # 클래스 인스턴스 생성 및 실행
    _config_f = 'msa.yaml'
    app_instance = MonitoringChart(config_f=_config_f)
    app_instance.run()