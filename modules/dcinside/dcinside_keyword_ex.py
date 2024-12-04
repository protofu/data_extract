import os
import yaml
from selenium import webdriver

class DcinsideKeywordMonitoring():
    def __init__(self, config_f):
        print('init')
        if not os.path.exists(config_f):
            print('nooo')
        else:
            print('good')
            with open(config_f, encoding='utf-8') as ifp:
                self.config = yaml.load(ifp, yaml.SafeLoader)
        self.url = self.config['params']['kwargs']['url']
        driver = webdriver.Chrome()
        driver.get(self.url)
        return

    def __enter__(self):
        print("Entering context")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        print("Exiting context")
        print('Browser closed')

    def __call__(self):
        print("Called!")



    def start(self):
        print('start')
        print('browser Open')

def do_start(**kwargs):
    with DcinsideKeywordMonitoring(kwargs['config_f']) as dkm:
        dkm.start()
        return

if __name__ == '__main__':
    _config_f = 'config.yaml'
    do_start(config_f=_config_f)
