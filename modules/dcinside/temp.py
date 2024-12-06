import os
import time
import yaml

def print_separator(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        print('#=========================')
        return result
    return wrapper

class SelfPractice():

    def __init__(self, config_f):
        print('[__init__] 함수 실행')
        if not os.path.exists(config_f):
            raise IOError(f'Cannot read config file "{config_f}"')
        with open(config_f, encoding='utf-8') as ifp:
            self.config = yaml.load(ifp, yaml.SafeLoader)
        self.name = self.config['params']['person']['name']
        self.age = self.config['params']['person']['age']
        time.sleep(1)
        print('#=========================')

    @print_separator
    def __enter__(self):
        print('[__enter__]')
        self.print_instantce()
        self.insert_person_to_db()
        time.sleep(1)

        return self

    @print_separator
    def __exit__(self, exc_type, exc_value, traceback):
        print('[__exit__]')
        time.sleep(1)

    @print_separator
    def __call__(self):
        print('[__call__]')
        time.sleep(1)

    @print_separator
    def print_instantce(self):
        print('[print_instantce]')
        print(self.name)
        print(self.age)

    @print_separator
    def insert_person_to_db(self):
        print('[insert_person_to_db]')
        print(f'{self.age}세 {self.name}님의 정보를 저장합니다.')

    @print_separator
    def start(self):
        print('[start]')
        print(f'{self.age}세 {self.name}님의 정보를 사용합니다.')
        time.sleep(1)


def do_start(**kwargs):
    with SelfPractice(kwargs['config_f']) as sp:
        print('__init__, __enter__ 함수 종료')
        sp.start()
        # sp()
        return

if __name__ == '__main__':
    _config_f = 'temp.yaml'
    do_start(config_f=_config_f)
