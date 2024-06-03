import asyncio
import json
import os
from abc import ABC, abstractmethod
from random import randrange

from bs4 import BeautifulSoup
from fake_useragent import UserAgent


class BaseParser(ABC):
    """
    Abstract base class for parsers.
    """
    BASE_URL = ''
    BASE_URLS_CATEGORIES = ''
    DEFAULT_URL_PATH = "data/urls"
    DEFAULT_URL_PATH_ERRORS = "data/urls/errors"
    DEFAULT_URL_PATH_CONTINUES = "data/urls/continues"
    RESULT_CSV = []
    LINKS = []
    TASKS = []
    DATA_FOR_CSV = []
    ERRORS = {}
    ERRORS_URLS = set()
    URL_COUNTER = 0

    def _get_header(self):
        """
        Используется для рандомизации headers в запросах(рандомизирует User-Agent при каждом обращении к HEADERS через данный метод)
        :return: заголовки
        """
        self.HEADERS.update({'User-Agent': UserAgent().random})
        return self.HEADERS

    def create_header(self,
                      accept='text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                      user_agent=None):
        """
            для создания атрибута экземпляра HEADERS
        :param accept:
        :param user_agent:
        :return:
        """
        if user_agent is None:
            user_agent = UserAgent().random
        self.HEADERS = {
            'Accept': accept,
            'User-Agent': user_agent,
        }

    @staticmethod
    def check_dirs(path, check_file=False):
        """

        :param path: путь до файла или каталога
        :param check_file: True - то проверяет наличие файла по указанному пути
                           False - проверяет наличие только пути, если нету, то создает каталоги
        :return:
        """
        if check_file:
            return os.path.exists(path)
        if not os.path.exists(path):
            os.makedirs(path)

    @staticmethod
    def write_to_json(path: str, filename: str = None, data: dict = None, isadd: bool = False) -> None:
        """

        :param path: путь до файла
        :param filename: имя файла
        :param data: данные для сохранения в json формате
        :param isadd:   True - Если нужно записать в конец существующего файла данные.
                        False - Если нужно записать данные в новый файл
        :return: None
        """
        if not filename or not data:
            print(f'\t[INFO] File is not create!\t\tdata:"{data}"\n\t\tfilename: "{path}/{filename}"')
        else:
            __class__.check_dirs(path)

            if (ischeck_file := __class__.check_dirs(f"{path}/{filename}", check_file=True)):
                old_data = json.load(open(f"{path}/{filename}", 'r', encoding='utf-8-sig'))

            with open(f"{path}/{filename}", 'w', encoding='utf-8-sig') as f_json:
                if isadd and ischeck_file:  # если файл уже существует и isadd=True то обновляем старый файл
                    old_data.update(
                        data
                    )
                    json.dump(old_data, f_json, indent=4, ensure_ascii=False)
                else:
                    json.dump(data, f_json, indent=4, ensure_ascii=False)

            print(f'[INFO] File "{path}/{filename}" is create\n')

    @staticmethod
    def write_to_file(path, filename=None, data=None, workmode='r', iscsv=False):
        """
        Для записи файла на диск.
        :param path: путь к файлу
        :param filename: имя файла
        :param data: данные для записи в файл с расширением txt
        :param workmode: режим открытия файла, Default = 'r' - чтение
        :return:
        """
        __class__.check_dirs(path)

        if filename and data:
            with open(f"{path}/{filename}", mode=workmode,
                      encoding='utf-8-sig') as f:  # кодировка UTF-8-sig в ней MSexcel и другие(WPS, libre) распознают русские буквы
                if iscsv:
                    pass

                for row in data:
                    print(row, file=f)
            print(f'[INFO] File "{path}/{filename}" is create\n')
        else:
            print(f'\t[INFO] File is not create!\t\tdata:"{data}"\n\t\tfilename: "{path}/{filename}"')

    @staticmethod
    def read_file(path):
        """
        Чтение файла, в основном для чтения txt-файлов
        :param path: путь к файлу
        :return:
        """
        __class__.check_dirs(path, check_file=True)

        with open(path, 'r', encoding='utf-8-sig') as f:
            return (row.strip() for row in f.readlines())

    @staticmethod
    def get_chunks(obj, shift):
        """
        Если размер итерируемого объекта очень большой, то разбивает этот итерируемый объект
        на более маленькие с длинной в shift элементов.
        :param obj: итерируемый объект
        :param shift: смещение
        :return: generator objects
        """
        return (obj[i:i + shift] for i in range(0, len(obj), shift))

    def get_soup(self, response):
        """
        для получения "супа" из ответа
        :param response: объект типа requests или session
        :return: объект BeautifulSoup
        """
        soup = BeautifulSoup(response, 'html.parser')
        return soup

    async def get_delay(self, start, stop):
        """
         Вносит произвольную задержку отправки запроса к сайту. Выбор значений будет от start до stop
        :param start:
        :param stop:
        :return:
        """
        await asyncio.sleep(randrange(start, stop))

    @abstractmethod
    def get_urls_from_soup(self, soup):
        """
        :param soup:
        :return: list of urls from soup
        """
        pass

    async def _parsing_urls_from_soup(self, session, url, url_index):
        """

        :param session: ---> async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(0), trust_env=True) as session:
        :param url: ссылка данные по которой нужно получить
        :param url_index: индекс ссылки для вывода информационного сообщения
        :return: None
        """
        print(f"[INFO id {url_index}] Сбор данных по {url}")
        async with session.get(url, headers=self._get_header()) as response:
            soup = self.get_soup(await response.read())
            return self.get_urls_from_soup(soup)

    @abstractmethod
    def get_object_data(self):
        """
            Нужен для получения данных, но можно и свой метод создать
        :return:
        """
        pass

    @abstractmethod
    def create_tasks(self):
        """
        код примера ниже нужно добавить в цикл для обработки ссылок

        self.TASKS.append(
            asyncio.create_task(
                self.get_object_data(url...)
            )
        )
        :return:
        """
        pass

    @abstractmethod
    def run_tasks(self):
        """
        asyncio.run()
        :return:
        """

        pass


if __name__ == "__main__":
    pass
