import requests
import threading
import time
import logging

import polars as pl
from bs4 import BeautifulSoup
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
from abc import ABC, abstractmethod

def setup_logger(name, log_file, formatter, level=logging.INFO):
    """To setup as many loggers as you want"""

    handler = logging.FileHandler(log_file, encoding='utf-8')        
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

info_logger = setup_logger('first_logger', 'loggs/parsing_error_info.log', formatter)
error_logger = setup_logger('second_logger', 'loggs/parsing_error_error.log', formatter)

class ABCParse(ABC):

    def __init__(self, list_of_links, root, n_threads=3, save_every=5, sleep=1, n_user_agent=1000, start_value=None, end_value=None, add_naming=None):
        '''list_of_links — список ссылок, которые мы будем парсить;
           root — путь, по которому будут сохранены результаты парсинга;
           n_threads — сколько потоков парсит;
           sleep — сколько времени спим перед;
           save_every — сохраняем n запросов;
           n_user_agent — после скольких запросов;
           start_value — с какого индекса парсить;
           end_value — до какого индекса парсить;
           add_naming — добавить дополнительный текст в название, чтобы, например, отмечать файлы, которые не распарсились с 1-ого раза
        '''
        self.list_of_links = list_of_links
        self.root = root
        self.n_threads = n_threads
        self.save_every = save_every
        self.sleep = sleep
        self.n_user_agent = n_user_agent
        self.current_value = start_value if start_value else 0
        self.end_value = end_value if end_value else len(list_of_links)
        self.add_naming = add_naming
        self.data = []

        software_names = [SoftwareName.CHROME.value, SoftwareName.SAFARI.value, SoftwareName.CHROMIUM.value, SoftwareName.FIREFOX.value]
        operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value, OperatingSystem.MACOS.value, OperatingSystem.IOS.value]   
        self.user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=10000)
    
    def parse(self, lock):
        '''Генерируем метаинформацию для запроса, Делаем запрос и преобразуем данные. 
        '''
        user_agent = self.user_agent_rotator.get_random_user_agent()

        while self.current_value < self.end_value:
            with lock:
                thread_value = self.current_value
                link = self.list_of_links[self.current_value]
                self.current_value += 1

            parsed = self.parse_from_link(link, user_agent, self.current_value)

            with lock:
                self.data.append(parsed)

                if thread_value % self.save_every == 0:
                    data = self.preprocess_data(self.data)
                    self.save(data, thread_value)
                    info_logger.info(f'Спаршено: {thread_value}')
                    self.data = [] 
            time.sleep(self.sleep)
        
        data = self.preprocess_data(self.data)
        if isinstance(data, pl.DataFrame):
            self.save(data, thread_value)
        info_logger.info(f'Спаршено всё!')
    
    @abstractmethod
    def parse_from_link(self, link, user_agent, current_value):

        if current_value % self.n_user_agent == 0:
            user_agent = self.user_agent_rotator.get_random_user_agent()

    def start_parsing(self):
        lock = threading.Lock()
        threads = []
        run_event = threading.Event()
        run_event.set()
        for _ in range(self.n_threads):
            t = threading.Thread(target=self.parse, args=(lock,))
            t.start()
            time.sleep(2)
            threads.append(t)
            
        for thread in threads:
            thread.join()
        

    @abstractmethod
    def preprocess_data(self, data):
        '''Метод для обработки данных для приведения их к стандартному виду. Надо, чтобы словарь стал Датафреймом'''
        assert isinstance(data, pl.DataFrame)
        return data
    
    def save(self, data: pl.DataFrame, thread_value):
        path = f'{self.root}/{thread_value}' + f'{self.add_naming if self.add_naming else ""}' + '.parquet'
        data.write_parquet(path)

class BlockedError(Exception):

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class ConnectionError(Exception):

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class PulseParse(ABCParse):

    def __init__(self, list_of_links, root, n_threads, save_every, sleep, n_user_agent, start_value=None, end_value=None, add_naming=None):
        super().__init__(list_of_links, root, n_threads, save_every, sleep, n_user_agent, start_value, end_value, add_naming)
        cols = ['author', 'id', 'timestamp', 'message', 'tickers', 'prices', 'n_comments', 'comments', 'reactions', 'n_reactions']
        self.col_rename = {f'column_{i}': cols[i] for i in range(len(cols))}
    
    def parse_from_link(self, link, user_agent, current_value):
        if current_value % 10 == 0:
            user_agent = self.user_agent_rotator.get_random_user_agent().replace('\t', '')

        author = link.split('/')[6]
        index = link.split('/')[7]

        try:
            r = requests.get(link, headers={'User-Agent': user_agent})

            if r.status_code == 403:
                raise BlockedError(f'{r.status_code}')
            elif r.status_code != 200:
                raise ConnectionError(f'{r.status_code}')

            soup = BeautifulSoup(r.text.encode('latin1').decode('utf-8'), 'lxml')

            data_body = soup.find(class_='social-post__aRLm--o')
            if not data_body:
                return author, index, None, 'empty', None, None, None, None, None, None
            
            message_body = data_body.find(class_='social-post__ffTK6Z social-post__ifTK6Z social-post__jfTK6Z')

            try:
                date = soup.find(class_='social-post__ciQY5O').text
            except:
                date = None
            
            try:
                prices = [el.text.replace('\xa0', '') for el in soup.find_all(class_='social-post__dDoSCf social-post__cDoSCf')]
            except:
                prices = None

            try:
                if not message_body:
                    head = data_body.find(class_='social-post__b-+eQ4b').text
                    main = data_body.find(class_='social-post__h-+eQ4b')
                    for d in main.find_all('div', {"style": True}):
                        d.find_parent("div").extract()
                    message = head + '. ' + main.text
                else: 
                    message = message_body.text
            except Exception as ex:
                message = 'error'
                error_logger.error(f'{link} - {ex}')

            try:
                tickers = [el.find(class_='social-post__gSSIko social-post__dSSIko')['href'].split('/')[-2] for el in data_body.find_all(class_='social-post__dDoSCf social-post__cDoSCf')]
            except:
                tickers = None

            try:
                n_comments = int(data_body.find(class_='social-post__rbf3Br').text)
            except:
                n_comments = None

            try:
                comments = [[el.find(class_='social-post__gr1HQG').text, el.find(class_='social-post__d2CwVH').text] for el in soup.find_all(class_='social-post__b2CwVH')]
            except:
                comments = None

            try:
                reactions = [el.find('img')['src'].split('/')[-1] for el in data_body.find_all(class_='social-post__babbMV')]
            except:
                reactions = None

            try:
                n_reactions = int(data_body.find(class_='social-post__ebf3Br').text)
            except:
                n_reactions = None

            return author, index, date, message, tickers, prices, n_comments, comments, reactions, n_reactions

        except Exception as ex:
            error_logger.error(f'{link} - {ex}')
            return author, index, None, 'error', None, None, None, None, None, None

        except KeyboardInterrupt:
            return
        
    def preprocess_data(self, data):
        try:
            data = pl.DataFrame(data, orient='row').rename(self.col_rename)
        except KeyboardInterrupt:
            raise
        except Exception as ex:
            error_logger.error(f'Ошибка при создании таблицы - {ex}')
            return None
        
        return data
    


if __name__ == '__main__':
    import gzip

    with gzip.open('mar_links.gz', 'r') as f:
        list_of_links = f.readlines()
        list_of_links = [el.decode().replace('\n', '') for el in list_of_links]
    print(len(list_of_links))
    
    parser = PulseParse(list_of_links, 'Article 3 Data/raw_mar', n_threads=3, save_every=5,
                        sleep=0, n_user_agent=10000, start_value=0)
    parser.start_parsing()
