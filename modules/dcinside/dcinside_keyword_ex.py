import os
import yaml
import time
from alabs.common.util.vvlogger import get_logger
from alabslib.selenium import PySelenium

from bs4 import BeautifulSoup
from datetime import datetime, timedelta

class DcinsideKeywordMonitoring(PySelenium):

    def __init__(self, config_f):
        start_time = time.time()
        if not os.path.exists(config_f):
            raise IOError(f'Cannot read config file "{config_f}"')
        with open(config_f, encoding='utf-8') as ifp:
            self.config = yaml.load(ifp, yaml.SafeLoader)
        self.page_count = 1
        self.current_page = 1
        # log를 기록할 경로 저장
        log_d = self.config['target']['log_folder']
        # 경로가 존재하지 않을 시 경로에 대한 폴더 만들기
        if not os.path.exists(log_d):
            os.makedirs(log_d)
        logger = get_logger(self.get_safe_path(log_d, 'DCInside.log'), logsize=1024*1024*10)
        self.config['params']['kwargs']['logger'] = logger
        PySelenium.__init__(self, **self.config['params']['kwargs'])

        # 키워드
        self.keyword = self.config['params']['site']['keyword']

        # 현재 시간 가져오기
        current_time = datetime.now()

        # 1시간 전 시간 계산
        self.target_time = current_time - timedelta(hours=self.config['target']['search_time'])
        self.article_count = 0
        self.is_done = False
        elapsed_time = time.time() - start_time
        print(f"[init] 완료 - 경과 시간: {elapsed_time:.2f} 초")

    # =================================기타 함수======================================
    # 검색 관련
    def search(self):
        start_time = time.time()
        # 검색 어 입력
        e = self.get_by_xpath('.//*[@id="preSWord"]')
        self.send_keys(e, self.config['params']['site']['search'])
        # 검색 단추
        e = self.get_by_xpath('.//*[@id="searchSubmit"]',
                              cond='element_to_be_clickable')
        self.safe_click(e)

        # 게시물
        e = self.get_by_xpath('.//*[@id="top"]/div/nav/ul/li[5]', cond='element_to_be_clickable')
        self.safe_click(e)

        # print('최신순')
        # # 최신순 클릭 (기본이 최신순이지만 한번 더 클릭)
        # e = self.get_by_xpath('.//button[contains(text(), "최신순")]', timeout=1)  # 텍스트가 "최신순"인 버튼을 찾기
        # print('최신순 element 찾기 완료')
        # self.safe_click(e)
        # self.implicitly_wait(after_wait=0.5)
        # print('search 완료')
        elapsed_time = time.time() - start_time
        print(f"[search] 완료 - 경과 시간: {elapsed_time:.2f} 초")

    def get_article_count(self):
        # 페이지의 HTML 소스 가져오기
        html_source = self.driver.page_source
        # BeautifulSoup을 사용하여 HTML 파싱
        page_source = BeautifulSoup(html_source, 'html.parser')
        article_times = page_source.find('ul', class_='sch_result_list').find_all('span', class_='date_time')
        for a_t in article_times:
            # 각 기사 시간 문자열을 datetime 객체로 변환
            article_time_str = a_t.get_text().strip()
            try:
                # '2024-12-05 14:30' 형식
                article_time = datetime.strptime(article_time_str, '%Y.%m.%d %H:%M')

                # 기사 시간이 target_time 이후인 경우 카운트 증가
                if article_time >= self.target_time:
                    self.article_count += 1
                else:
                    self.is_done = True
                    return
            except ValueError:
                # 날짜 형식이 맞지 않으면 무시 (예외 처리)
                print(f"Date format error: {article_time_str}")  # 로그 출력 추가

    def next_page(self):
        # page_count => 1, 2, 3, 4, ..., 10, 11, 12, 13, ...
        # current_page => 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, ...
        # 1~10 -> 12(start with a[1] ~ a[9] \\ else 14(start with a[3] ~ a[11]) || both except em tag

        try:
            if self.current_page == 10:
                if self.page_count < 11:
                    e = self.get_by_xpath('.//ul[@id="dgn_btn_paging"]/a[10]')
                else:
                    e = self.get_by_xpath('.//ul[@id="dgn_btn_paging"]/a[12]')
                self.safe_click(e)
                self.page_count += 1
                self.current_page = self.page_count % 10
            else:
                self.page_count += 1
                self.current_page += 1
                if self.page_count <= 10:
                    e = self.get_by_xpath(f'.//ul[@id="dgn_btn_paging"]/a[{self.current_page - 1}]')
                else:
                    e = self.get_by_xpath(f'.//ul[@id="dgn_btn_paging"]/a[{self.current_page + 1}]')
                self.safe_click(e)
            self.implicitly_wait(after_wait=1)
            print(self.page_count)
            print(self.current_page)
            print('#-=====================')

        except Exception as e:
            print(f"Error navigating to next page: {e}")
            self.is_done = True

    def start(self):
        start_time = time.time()  # 시작 시간 기록
        try:
            print("search 입장")
            self.search()
            print("search 퇴장")
            while not self.is_done:
                print("while 입장")
                self.get_article_count()
                print("nextPage 입장")
                self.next_page()
            print(f"Article Count: {self.article_count}")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            end_time = time.time()  # 끝 시간 기록
            elapsed_time = end_time - start_time  # 실행 시간 계산
            print(f"Total execution time: {elapsed_time:.2f} seconds")

def do_start(**kwargs):
    with DcinsideKeywordMonitoring(kwargs['config_f']) as dkm:
        dkm.start()
        return

if __name__ == '__main__':
    _config_f = 'config.yaml'
    do_start(config_f=_config_f)