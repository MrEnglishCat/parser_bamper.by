import asyncio
import csv
import json
import os
import sys
import time
import aiohttp
import requests
from pprint import pprint
from random import randrange
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from datetime import datetime


# sys.stdin.reconfigure(encoding='utf-8')  # если в терминале проблемы с кодировкой, то раскомитить 17 и 18 строчки, либо изменить кодировку в терминале
# sys.stdout.reconfigure(encoding='utf-8')


class ParserBamberBy:
    """
    Парсер всех найденных позиций на сайте bamper.by.
    Парсинг начинается со ссылки BASE_URLS_CATEGORIES = 'https://bamper.by/catalog/modeli/'
    """
    BASE_URL = 'https://www.bamper.by'
    BASE_URLS_CATEGORIES = 'https://bamper.by/catalog/modeli/'

    # DEFAULT_ ... пути для файлов
    DEFAULT_URL_PATH = "data/urls"
    DEFAULT_URL_PATH_CSV = "data/result"
    DEFAULT_URL_PATH_ERRORS = f"data/urls/errors"
    DEFAULT_URL_PATH_CONTINUES = f"data/urls/continues"
    DEFAULT_TEST_URL_PATH = "data/test"  # TODO TEST каталог для тестовых файлов

    HEADERS = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'User-Agent': UserAgent().random,
    }

    TASKS = []

    # для сбора данных
    URLS_WITH_ATTRS_GROUPS = {}
    ALL_GOODS_URLS = {}

    # для работы с CSV/json
    DATA_FOR_CSV = []
    RESULT_CSV = []
    CSV_FIELDNAMES = (
        'Группа',
        'Раздел',
        'Артикул',
        'Название',
        'Примечание',
        'Номер запчасти',
        'Цена',
        'Валюта',
        'Город',
        'Объем двигателя',
    )

    ERRORS = {}
    ERRORS_URLS = set()
    URL_COUNTER = 0

    @staticmethod
    def _get_length(obj: dict) -> int:
        """
        Получение общего количества ссылок. Работает с определенной вложенностью словаря dict
        """
        result = 0
        for chapter in obj.values():
            for row in chapter.values():
                result += len(row)

        return result

    @staticmethod
    def _get_header() -> dict:
        """
        Используется для рандомизации headers в запросах
        :return: заголовки/dict
        """
        return {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'User-Agent': UserAgent().random,
        }

    @staticmethod
    def _check_dirs(path: str, check_file: bool = False) -> bool | None:
        """
        Используется для проверки наличия пути. Или проверки наличия файла(при check_file=True)
        :path:  При check_file=False - проверяет, существуют ли каталоги, если каких-то нету, то их создает, метод возвращает None
                При check_file=True - проверяет, существует ли указанный файл, если существует возвращает True, иначе False
        :result: True|False|None
        """
        if check_file:
            return os.path.exists(path)
        if not os.path.exists(path):
            os.makedirs(path)

    @staticmethod
    def _delete_old_files(path: str) -> None:
        """
        Удаление файлов в каталоге data/urls:
            main_urls.txt;
            rls_with_attrs_groups.json;
            all_goods_urls.json;
            all_data_items.json
        """
        if os.path.exists(path):
            files_list = os.listdir(path)
            for file in files_list:
                if os.path.isfile(os.path.join(path, file)):
                    os.remove(os.path.join(path, file))

    @staticmethod
    def _write_to_json(path: str, filename: str = None, data: dict = None, isadd: bool = False) -> None:
        """
        Запись данных в json
        :path путь до файла
        :filename имя файла до которого указан путь в path
        :data Словарь с данными
        :isadd default=False - Если False - создается новый файл или перезаписывается предыдущий.
                                Если True - данные дозаписываются в уже существующий файл
        :return None
        """
        if filename and data:
            __class__._check_dirs(path)
            if (ischeck_file := __class__._check_dirs(f"{path}/{filename}", check_file=True)):
                old_data = json.load(open(f"{path}/{filename}", 'r', encoding='utf-8-sig'))
            with open(f"{path}/{filename}", 'w', encoding='utf-8-sig') as f_json:
                if isadd and ischeck_file and old_data:
                    if isinstance(old_data, list):
                        old_data.extend(
                            data
                        )
                    elif isinstance(old_data, dict):
                        old_data.update(
                            data
                        )
                    json.dump(old_data, f_json, indent=4, ensure_ascii=False)
                else:
                    json.dump(data, f_json, indent=4, ensure_ascii=False)

            print(f'[INFO] File "{path}/{filename}" is create\n')
        else:
            print(f'\t[INFO] File is not create!\t\tdata:"{data}"\n\t\tfilename: "{path}/{filename}"')

    @staticmethod
    def _write_to_file(path: str, filename: str = None, data: list | tuple = None, workmode: str = 'w') -> None:
        """
        Запись данных в txt
        :path путь до файла
        :filename имя файла до которого указан путь в path
        :data Словарь с данными
        :isadd default=False - Если False - создается новый файл или перезаписывается предыдущий.
                                Если True - данные дозаписываются в уже существующий файл
        :return None
        """
        if filename and data:
            __class__._check_dirs(path)
            with open(f"{path}/{filename}", mode=workmode,
                      encoding='utf-8-sig') as f:  # кодировка UTF-8-sig в ней MSexcel и другие(WPS, libre) распознают русские буквы
                for row in data:
                    print(row, file=f)
            print(f'[INFO] File "{path}/{filename}" is create\n')
        else:
            print(f'\t[INFO] File is not create!\t\tdata:"{data}"\n\t\tfilename: "{path}/{filename}"')

    @staticmethod
    def _write_to_csv(path:str, filename:str=None, data:dict=None)->None:
        """
        Запись данных в csv
        :path путь до файла
        :filename имя файла до которого указан путь в path
        :data Словарь с данными
        :return None
        """
        if filename and data:
            __class__._check_dirs(path)
            file_found = __class__._check_dirs(path, check_file=True)
            with open(f"{path}/{filename}", mode='a' if file_found else 'w', encoding='utf-8-sig', newline='') as f_csv:
                writer = csv.DictWriter(f_csv, fieldnames=__class__.CSV_FIELDNAMES, delimiter=';')
                if not file_found:
                    writer.writeheader()
                writer.writerows(data)
        else:
            print(f'\t[INFO] File is not create!\t\tdata:"{data}"\n\t\tfilename: "{path}/{filename}"')

    @staticmethod
    def _read_file(path:str, isjson:bool=False)->None:
        """
        Чтение данных из txt или json
        :path путь до файла
        :isjson Если False - чтение будет из txt. Если True, то чтение из json
        :return None
        """
        __class__._check_dirs(path, check_file=True)

        with open(path, 'r', encoding='utf-8-sig') as f:
            if isjson:
                return json.load(f)
            return (row.strip() for row in f.readlines())

    @staticmethod
    def _get_active_page(soup:BeautifulSoup)->str:
        """
        Метод для получения номера активной страницы
        """
        return soup.find('div', class_='pagination-bar').find('li', class_='active').text.strip()

    @staticmethod
    def _check_pagination(soup:BeautifulSoup)->str:
        """
        Проверка наличия пагинации
        soup: BeautifulSoup
        """
        # TODO добавлен try
        try:
            if (pagination_bar := soup.find('div', class_='pagination-bar').find('a', class_='modern-page-next')):
                return pagination_bar.get('href')
            else:
                return '1 страница'
        except:
            return '1 страница'

    @staticmethod
    def _get_datetime(split=False):
        """"""  # TODO продолжить отсюда
        result = datetime.now().strftime("%d.%m.%Y_%H.%M.%S")
        if split:
            return result.split('_')[0]
        return result

    def get_main_urls(self, url):
        """
         get urls list from main page:  self.BASE_URLS_CATEGORIES
        :param url:
        :return:
        """
        result = []
        response = requests.get(url, headers=self._get_header()).text
        soup = BeautifulSoup(response, 'html.parser')
        for row in soup.find('div', class_='inner-box relative').find_all('ul', class_='cat-list'):
            for row_row in row.find_all('li'):
                result.append(
                    self.BASE_URL + row_row.find('a').get('href')
                )
        return result

    def get_soup(self, response):
        soup = BeautifulSoup(response, 'html.parser')
        return soup

    # def get_urls_from_soup(self, soup):
    #     result_urls = []
    #     # for row in soup.find('div', class_='inner-box relative').find_all('ul', class_='cat-list'):
    #     for row in soup.find('div', class_='relative').find_all('a'):
    #         result_urls.append(
    #             # self.BASE_URL + link.get('href')
    #             self.BASE_URL + row.get('href')
    #         )
    #     return result_urls

    def get_urls_from_soup(self, soup):
        group = None
        # chapter = None
        for row in soup.find('div', class_='relative').find_all('li'):
            # print(row.text)
            row_text = row.text.strip()
            if 'list-header' in row.get('class', ''):
                group = row_text
                continue
            chapter = row_text
            type(self).URLS_WITH_ATTRS_GROUPS.setdefault(group, {}).setdefault(chapter, []).append(
                self.BASE_URL + row.find('a').get('href')
            )

    async def get_delay(self, start, stop):
        await asyncio.sleep(randrange(start, stop))

    def get_chunks(self, obj, chunk_length):
        return (obj[i:i + chunk_length] for i in range(0, len(obj), chunk_length))

    async def _parsing_urls_from_soup(self, session, url, url_index, is_list_headers=False):
        print(f"[INFO id {url_index}] Сбор данных по {url}")
        async with session.get(url, headers=self._get_header()) as response:
            soup = self.get_soup(await response.read())
            # if is_list_headers:
            self.get_urls_from_soup(soup)
            # return self.get_urls_from_soup(soup)

    async def get_list_attr_groups_url(self, session, url, url_index):  # url это ссылка с self.BASE_URLS_CATEGORIES
        await self.get_delay(1, 2)
        try:
            await self._parsing_urls_from_soup(session, url, url_index, is_list_headers=True)
            print(f"\t[SUCCESS id {url_index}] ДАННЫЕ СОБРАНЫ ПО: {url}")
        except Exception as e:
            type(self).ERRORS.setdefault('get_list_car_brands_url', {}).setdefault(f"{url}", tuple(e.args))
            type(self).ERRORS_URLS.add(url)
            print(f"\t[ERROR id {url_index}] ОШИБКА! ")

    async def get_tasks_attrs_groups(self, chunk_urls):
        type(self).TASKS.clear()
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(0), trust_env=True) as session:
            for url_index, url in enumerate(chunk_urls, 1):
                type(self).TASKS.append(
                    asyncio.create_task(self.get_list_attr_groups_url(session, url, url_index))
                )
            await asyncio.gather(*self.TASKS)

    def run_attrs_groups_tasks(self):
        print(f"{'=' * 50}\nНачат сбор урлов всех моделей авто:\n{'=' * 50}")
        type(self).GOODS_ALL_URL_LIST = self.get_main_urls(self.BASE_URLS_CATEGORIES)
        self._write_to_file(self.DEFAULT_URL_PATH, 'main_urls.txt',
                            data=self.GOODS_ALL_URL_LIST)
        chunks = self.get_chunks(self.GOODS_ALL_URL_LIST, 100)

        for chunk_id, chunk_urls in enumerate(chunks):
            print('-' * 100)
            print(f'{"\t" * 10} Chunk id: #{chunk_id}')
            print('-' * 100)
            asyncio.run(
                self.get_tasks_attrs_groups(chunk_urls)
            )
            self._write_to_json(self.DEFAULT_URL_PATH, 'urls_with_attrs_groups.json', self.URLS_WITH_ATTRS_GROUPS,
                                isadd=True)
            self._write_to_json(f"{self.DEFAULT_URL_PATH_ERRORS}/{self._get_datetime(True)}",
                                f'ERRORS_attrs_groups.json',
                                self.ERRORS, isadd=True)
            self._write_to_file(f"{self.DEFAULT_URL_PATH_ERRORS}/{self._get_datetime(True)}",
                                f'ERRORS_URLS_attrs_groups.txt', self.ERRORS_URLS, workmode='a')
            if chunk_id == 2:  # TODO TEST ограничение на количество обрабатываемых чанков при получении ссылок на модели авто
                break
        type(self).ERRORS.clear()
        type(self).ERRORS_URLS.clear()

    async def get_all_goods_from_page(self, session, url, url_index, group, chapter):
        await self.get_delay(1, 2)
        type(self).URL_COUNTER += 1
        PREVIOUS_ACTIVE_PAGE = ''  # переменная только для этой функции get_all_goods_from_page()
        print(f"[{self.URL_COUNTER}][ INFO id {url_index}] Сбор данных по {url}")
        start = True
        while start:
            await self.get_delay(1, 2)
            try:
                async with session.get(url, headers=self._get_header()) as response:
                    soup = self.get_soup(await response.read())
                    if PREVIOUS_ACTIVE_PAGE == self._get_active_page(soup):
                        start = False
                        print(f"ID {url_index}", PREVIOUS_ACTIVE_PAGE, url)
                        continue
                    for row_index, row in enumerate(
                            soup.find('div', class_='list-wrapper').find_all('div', class_='add-image'), 1):
                        type(self).ALL_GOODS_URLS.setdefault(group, {}).setdefault(chapter, []).append(
                            self.BASE_URL + row.find('a').get('href')
                        )
                    paginagions = self._check_pagination(soup)

                    print('\t\t\t\t', paginagions)
                    if paginagions != '1 страница':
                        url = self.BASE_URL + paginagions
                        PREVIOUS_ACTIVE_PAGE = self._get_active_page(soup)
                    else:
                        start = False
                    print(f"\t[SUCCESS id {url_index}] ДАННЫЕ СОБРАНЫ ПО: {url}")

            except Exception as e:
                type(self).ERRORS.setdefault('get_all_goods_from_page', {}).setdefault(f"{url}", tuple(e.args))
                type(self).ERRORS_URLS.add(url)
                print(f"\t[ERROR id {url_index}] ОШИБКА! ")

    async def get_tasks_car_goods(self, chunk, group, chapter):
        type(self).TASKS.clear()
        print(f'[INFO] Формирование задач для начала сбора url товаров...')
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(0), trust_env=True) as session:
            for url_index, url in enumerate(chunk, 1):
                type(self).TASKS.append(
                    asyncio.create_task(
                        self.get_all_goods_from_page(session, url, url_index, group, chapter)
                    )
                )

            await asyncio.gather(*self.TASKS)

    def run_car_item_tasks(self):
        if not type(self).URLS_WITH_ATTRS_GROUPS:
            type(self).URLS_WITH_ATTRS_GROUPS = self._read_file('data/urls/urls_with_attrs_groups.json', isjson=True)

        for group, group_values in self.URLS_WITH_ATTRS_GROUPS.items():
            for chapter, chapter_values in group_values.items():
                chunks = self.get_chunks(chapter_values, 50)
                for chunk_id, chunk_urls in enumerate(chunks):
                    print('-' * 100)
                    print(f'{"\t" * 10} Chunk id: #{chunk_id}')
                    print('-' * 100)
                    asyncio.run(
                        self.get_tasks_car_goods(chunk_urls, group, chapter)
                    )
                    self._write_to_json(self.DEFAULT_URL_PATH, 'all_goods_urls.json', self.ALL_GOODS_URLS, isadd=True)
                    self._write_to_json(f"{self.DEFAULT_URL_PATH_ERRORS}/{self._get_datetime(True)}",
                                        f'ERRORS_goods_urls.json', self.ERRORS, isadd=True)
                    self._write_to_file(f"{self.DEFAULT_URL_PATH_ERRORS}/{self._get_datetime(True)}",
                                        f'ERRORS_URLS_goods_urls.txt', self.ERRORS_URLS,
                                        workmode='a')
                    if chunk_id == 1:  # TODO TEST ограничение на количество обрабатываемых чанков при получении ссылок на сами объявдения
                        break
                break
            break

        type(self).URL_COUNTER = 0
        type(self).ERRORS.clear()
        type(self).ERRORS_URLS.clear()

    def get_data(self, soup, url, group, chapter):
        result = {}
        item_name = soup.find('h1', class_='auto-heading onestring').find('span').text.strip()
        try:
            price = soup.find('h1', class_='auto-heading onestring').find('meta', itemprop='price').get('content')
            units = soup.find('h1', class_='auto-heading onestring').find('meta', itemprop='priceCurrency').get(
                'content')
        except:
            price = 'цена не указана'
            units = 'N/A'

        item_comment = ''
        vendor_code = ''
        item_number = ''
        item_attributes = soup.find('div', class_='key-features').find_all('div', class_='media')
        for item in item_attributes:
            if not item_comment:
                try:
                    item_comment = item.find('span', class_='media-heading cut-h-375').text.strip()
                    continue
                except:
                    pass

            if not vendor_code:
                try:
                    vendor_code = item.find('span', class_='data-type f13').text
                    continue
                except:
                    pass
            if not item_number and 'Номер запчасти' in item.find('div', class_='media-body').text:
                try:
                    item_number = item.find('div', class_='media-body').find_all('span', class_='media-heading')[
                        -1].text.strip()
                    continue
                except:
                    pass

        city = soup.find('div', class_='panel sidebar-panel panel-contact-seller hidden-xs hidden-sm').find('div',
                                                                                                            class_='seller-info').find_all(
            'p')[0].text.split()[-1].strip()
        engine_v = 'не указан'
        if (a := soup.find('div', style="font-size: 17px;")):
            for r in a.text.split(','):
                r = r.strip()
                if ' л' in r:
                    engine_v = r
                    break

        result.update(
            {
                # 'url': url,
                'Группа': group,
                'Раздел': chapter,
                'Артикул': vendor_code,
                'Название': item_name,
                'Примечание': item_comment,
                'Номер запчасти': item_number,
                'Цена': price,
                'Валюта': units,
                'Город': city,
                'Объем двигателя': engine_v,
            }
        )
        return result

    async def get_data_from_page(self, session, url, url_index, group, chapter):
        await self.get_delay(1, 3)
        type(self).URL_COUNTER += 1
        try:
            async with session.get(url, headers=self._get_header()) as response:
                soup = self.get_soup(await response.read())
                if (a := soup.find('div', class_='row block404')):
                    raise ValueError(f'Error 404 - {a.text}')
                type(self).DATA_FOR_CSV.append(
                    self.get_data(soup, url, group, chapter)
                )
                print(f"\t[{self.URL_COUNTER}][SUCCESS id {url_index}] ДАННЫЕ СОБРАНЫ ПО: {url}")
        except Exception as e:
            type(self).ERRORS.setdefault('get_data_from_page', {}).setdefault(f"{url_index}. {url}", tuple(e.args))
            type(self).ERRORS_URLS.add(url)
            print(f"\t[ERROR id {url_index}] ОШИБКА! ")

    async def get_tasks_car_items(self, chunk_urls, group, chapter):
        type(self).TASKS.clear()
        print(f'[INFO] Формирование задач для начала сбора данных о товарах...')
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(0), trust_env=True) as session:
            for url_index, url in enumerate(chunk_urls, 1):
                type(self).TASKS.append(
                    asyncio.create_task(
                        self.get_data_from_page(session, url, url_index, group, chapter)
                    )
                )

            await asyncio.gather(*self.TASKS)

    def run_get_data_from_page_tasks(self):
        if not type(self).ALL_GOODS_URLS:
            type(self).ALL_GOODS_URLS = self._read_file(f'{self.DEFAULT_URL_PATH}/all_goods_urls.json', isjson=True)
            if self._check_dirs(f"{self.DEFAULT_URL_PATH_CSV}/RESULT.csv", check_file=True):
                os.remove(f"{self.DEFAULT_URL_PATH_CSV}/RESULT.csv")

        for group, group_values in self.ALL_GOODS_URLS.items():
            for chapter, chapter_values in group_values.items():
                chunks = self.get_chunks(chapter_values, 100)
                for chunk_id, chunk_urls in enumerate(chunks):
                    print('-' * 100)
                    print(f'{"\t" * 10} Chunk #{chunk_id}')
                    print('-' * 100)
                    asyncio.run(
                        self.get_tasks_car_items(chunk_urls, group, chapter)
                    )
                    self._write_to_json(f"{self.DEFAULT_URL_PATH_ERRORS}/{self._get_datetime(True)}",
                                        f'ERRORS_data_items.json', self.ERRORS, isadd=True)
                    self._write_to_file(f"{self.DEFAULT_URL_PATH_ERRORS}/{self._get_datetime(True)}",
                                        f'ERRORS_URLS_data_items.txt', self.ERRORS_URLS,
                                        workmode='a')
                    # self.write_to_file(self.DEFAULT_URL_PATH_CONTINUES, 'all_goods_urls.txt',
                    #                    self.ALL_GOODS_URLS[(chunk_id + 1) * 100:])
                    self._write_to_json(self.DEFAULT_URL_PATH, 'all_data_items.json', self.DATA_FOR_CSV, isadd=True)
                    self._write_to_csv(self.DEFAULT_URL_PATH_CSV, f'RESULT.csv', self.DATA_FOR_CSV)
                    # if chunk_id == 1:  # TODO TEST ограничение на количество обрабатываемых чанков при получении данных о товаре
                    #     break
                    type(self).DATA_FOR_CSV.clear()
                type(self).URL_COUNTER = 0

    def run_all_tasks(self):
        self._delete_old_files(self.DEFAULT_URL_PATH)

        # Эта часть ищет все ссылки брендов на каждую группу товара.
        start = time.monotonic()
        parser.run_attrs_groups_tasks()
        end = time.monotonic()
        print(
            f"Время работы скрипта получение списка ({len(self.URLS_WITH_ATTRS_GROUPS)}): {end - start} секунд. \n{'=' * 50}")

        print()
        self._write_to_file(self.DEFAULT_URL_PATH, 'timing.txt', (
            f"Время работы скрипта получение списка ({len(self.URLS_WITH_ATTRS_GROUPS)}): {end - start} секунд.",))

        # Эта часть ищет ссылка на сами товары.
        start = time.monotonic()
        self.run_car_item_tasks()
        end = time.monotonic()
        print(
            f"Время работы скрипта получение списка ссылок на товары({self._get_length(self.URLS_WITH_ATTRS_GROUPS)}): {end - start} секунд. \n{'=' * 50}")

        print()
        self._write_to_file(self.DEFAULT_URL_PATH, 'timing.txt', (
            f"Время работы скрипта получение списка ссылок на товары({self._get_length(self.URLS_WITH_ATTRS_GROUPS)}): {end - start} секунд.",))

        # Эта часть ищет данные по списку ссылок и затем сохраняет в csv
        start = time.monotonic()
        self.run_get_data_from_page_tasks()
        end = time.monotonic()
        print(
            f"Время работы скрипта получение данных о товарах({self._get_length(self.ALL_GOODS_URLS)}): {end - start} секунд. \n{'=' * 50}")
        self._write_to_file(self.DEFAULT_URL_PATH, 'timing.txt', (
            f"Время работы скрипта получение данных о товарах({self._get_length(self.ALL_GOODS_URLS)}): {end - start} секунд.",))


if __name__ == '__main__':
    # TODO continue рассмотреть возможность сделать сохранение оставшихся чанков
    # TODO continue в каталоге continues после того как файл будет пустым рассмотреть необходимость его удаления
    parser = ParserBamberBy()
    parser.run_all_tasks()
