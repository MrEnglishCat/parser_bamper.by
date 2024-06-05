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
    BASE_URL = 'https://www.bamper.by'
    # BASE_URLS_CATEGORIES = 'https://bamper.by/catalog/zapchasti/'
    BASE_URLS_CATEGORIES = 'https://bamper.by/catalog/modeli/'
    # PARSER_URL = 'https://bamper.by/zapchast_elektrogidrousilitel-rulya/8792-1084241/'
    DEFAULT_URL_PATH = "data/urls"
    DEFAULT_URL_PATH_ERRORS = f"data/urls/errors"
    DEFAULT_URL_PATH_CONTINUES = f"data/urls/continues"
    DEFAULT_TEST_URL_PATH = "data/test"  # TODO for test

    LINKS = []
    HEADERS = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'User-Agent': UserAgent().random,
    }
    TASKS = []
    URLS_WITH_ATTRS_GROUPS = {}
    ALL_GOODS_URLS = {}

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
    PREVIOUS_ACTIVE_PAGE = ''
    URL_COUNTER = 0

    @staticmethod
    def get_length(obj):
        result = 0
        for chapter in obj.values():
            for row in chapter.values():
                result += len(row)

        return result


    @staticmethod
    def _get_header():
        """
        Используется для рандомизации headers в запросах
        :return: заголовки
        """
        return {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'User-Agent': UserAgent().random,
        }

    @staticmethod
    def check_dirs(path, check_file=False):
        if check_file:
            return os.path.exists(path)
        if not os.path.exists(path):
            os.makedirs(path)

    @staticmethod
    def write_to_json(path, filename=None, data=None, isadd=False):
        if not filename or not data:
            print(f'\t[INFO] File is not create!\t\tdata:"{data}"\n\t\tfilename: "{path}/{filename}"')
        else:
            __class__.check_dirs(path)
            if (ischeck_file := __class__.check_dirs(f"{path}/{filename}", check_file=True)):
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

    @staticmethod
    def write_to_file(path, filename=None, data=None, workmode='w'):
        __class__.check_dirs(path)

        if filename and data:
            with open(f"{path}/{filename}", mode=workmode,
                      encoding='utf-8-sig') as f:  # кодировка UTF-8-sig в ней MSexcel и другие(WPS, libre) распознают русские буквы
                # ??? CSV
                for row in data:
                    print(row, file=f)
            print(f'[INFO] File "{path}/{filename}" is create\n')
        else:
            print(f'\t[INFO] File is not create!\t\tdata:"{data}"\n\t\tfilename: "{path}/{filename}"')

    @staticmethod
    def write_to_csv(path, filename=None, data=None):
        __class__.check_dirs(path)

        with open(f"{path}/{filename}", 'w', encoding='utf-8-sig') as f_json:
            if filename and data:

                with open(f"{path}/{filename}", mode='w', encoding='utf-8-sig', newline='') as f_csv:
                    writer = csv.DictWriter(f_csv, fieldnames=__class__.CSV_FIELDNAMES, delimiter=';')
                    writer.writeheader()
                    writer.writerows(data)
            else:
                print(f'\t[INFO] File is not create!\t\tdata:"{data}"\n\t\tfilename: "{path}/{filename}"')

    @staticmethod
    def read_file(path, isjson=False):
        __class__.check_dirs(path, check_file=True)

        with open(path, 'r', encoding='utf-8-sig') as f:
            if isjson:
                return json.load(f)
            return (row.strip() for row in f.readlines())

    @staticmethod
    def get_active_page(soup):
        return soup.find('div', class_='pagination-bar').find('li', class_='active').text.strip()

    @staticmethod
    def check_pagination(soup):
        #TODO добавлен try
        try:
            if (pagination_bar := soup.find('div', class_='pagination-bar').find('a', class_='modern-page-next')):
                return pagination_bar.get('href')
            else:
                return '1 страница'
        except:
            return '1 страница'

    @staticmethod
    def get_datetime(split=False):
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

    def get_urls_from_soup(self, soup):
        result_urls = []
        # for row in soup.find('div', class_='inner-box relative').find_all('ul', class_='cat-list'):
        for row in soup.find('div', class_='relative').find_all('a'):
            result_urls.append(
                # self.BASE_URL + link.get('href')
                self.BASE_URL + row.get('href')
            )
        return result_urls

    def get_urls_from_soup_list_header(self, soup):
        group = None
        chapter = None
        for row in soup.find('div', class_='relative').find_all('li'):
            # print(row.text)
            row_text = row.text.strip()
            if 'list-header' in row.get('class', ''):
                group = row_text
                continue
            chapter = row_text
            self.URLS_WITH_ATTRS_GROUPS.setdefault(group, {}).setdefault(chapter,[]).append(
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
            if is_list_headers:
                self.get_urls_from_soup_list_header(soup)
            return self.get_urls_from_soup(soup)

    async def get_list_attr_groups_url(self, session, url, url_index):  # url это ссылка с self.BASE_URLS_CATEGORIES
        await self.get_delay(1, 2)
        try:
            data = await self._parsing_urls_from_soup(session, url, url_index, is_list_headers=True)
            print(f"\t[SUCCESS id {url_index}] ДАННЫЕ СОБРАНЫ ПО: {url}")
        except Exception as e:
            data = ''
            self.ERRORS.setdefault('get_list_car_brands_url', {}).setdefault(f"{url}", tuple(e.args))
            self.ERRORS_URLS.add(url)
            print(f"\t[ERROR id {url_index}] ОШИБКА! ")
        # if data:
            # self.URLS_WITH_ATTRS_GROUPS.update(
            #     data
            # )



    async def get_tasks_attrs_groups(self, chunk_urls):
        self.TASKS.clear()
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(0), trust_env=True) as session:
            for url_index, url in enumerate(chunk_urls, 1):
                self.TASKS.append(
                    asyncio.create_task(self.get_list_attr_groups_url(session, url, url_index))
                )
            await asyncio.gather(*self.TASKS)

    def run_attrs_groups_tasks(self):
        print(f"{'=' * 50}\nНачат сбор урлов всех моделей авто:\n{'=' * 50}")
        self.GOODS_ALL_URL_LIST = self.get_main_urls(self.BASE_URLS_CATEGORIES)
        self.write_to_file(self.DEFAULT_URL_PATH, 'main_url.txt',
                           self.GOODS_ALL_URL_LIST)  # файл для теста(проверка урлов брендов)
        chunks = self.get_chunks(self.GOODS_ALL_URL_LIST, 100)

        for chunk_id, chunk_urls in enumerate(chunks):
            print('-' * 100)
            print(f'{"\t" * 10} Chunk id: #{chunk_id}')
            print('-' * 100)
            asyncio.run(
                self.get_tasks_attrs_groups(chunk_urls)
            )
            self.write_to_json(self.DEFAULT_URL_PATH, 'urls_with_attrs_groups.json', self.URLS_WITH_ATTRS_GROUPS,
                               isadd=True)
            self.write_to_file(self.DEFAULT_URL_PATH, 'urls_without_attrs_groups.txt', self.URLS_WITHOUT_ATTRS_GROUPS,
                               workmode='a')
            self.write_to_json(f"{self.DEFAULT_URL_PATH_ERRORS}/{self.get_datetime(True)}", f'ERRORS_attrs_groups.json',
                               self.ERRORS, isadd=True)
            self.write_to_file(f"{self.DEFAULT_URL_PATH_ERRORS}/{self.get_datetime(True)}",
                               f'ERRORS_URLS_attrs_groups.txt', self.ERRORS_URLS, workmode='a')
            if chunk_id == 2:
                break  # TODO test
        self.ERRORS.clear()
        self.ERRORS_URLS.clear()

    async def get_all_goods_from_page(self, session, url, url_index, group, chapter):
        await self.get_delay(1, 2)
        self.URL_COUNTER += 1
        PREVIOUS_ACTIVE_PAGE = ''
        print(f"[{self.URL_COUNTER}][ INFO id {url_index}] Сбор данных по {url}")
        start = True
        while start:
            await self.get_delay(1, 2)
            try:
                async with session.get(url, headers=self._get_header()) as response:
                    soup = self.get_soup(await response.read())
                    if PREVIOUS_ACTIVE_PAGE == self.get_active_page(soup):
                        start = False
                        # PREVIOUS_ACTIVE_PAGE = ''
                        print(f"ID {url_index}", PREVIOUS_ACTIVE_PAGE, url)
                        continue
                    for row_index, row in enumerate(
                            soup.find('div', class_='list-wrapper').find_all('div', class_='add-image'), 1):
                        self.ALL_GOODS_URLS.setdefault(group, {}).setdefault(chapter, []).append(
                            self.BASE_URL + row.find('a').get('href')
                        )
                    paginagions = self.check_pagination(soup)

                    print('\t\t\t\t', paginagions)
                    if paginagions != '1 страница':
                        url = self.BASE_URL + paginagions
                        PREVIOUS_ACTIVE_PAGE = self.get_active_page(soup)
                    else:
                        start = False
                        # PREVIOUS_ACTIVE_PAGE = ''
                    print(f"\t[SUCCESS id {url_index}] ДАННЫЕ СОБРАНЫ ПО: {url}")

            except Exception as e:
                self.ERRORS.setdefault('get_all_goods_from_page', {}).setdefault(f"{url}", tuple(e.args))
                self.ERRORS_URLS.add(url)
                print(f"\t[ERROR id {url_index}] ОШИБКА! ")
                # PREVIOUS_ACTIVE_PAGE = ''




    async def get_tasks_car_goods(self, chunk, group, chapter):
        self.TASKS.clear()
        print(f'[INFO] Формирование задач для начала сбора url товаров...')
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(0), trust_env=True) as session:
            for url_index, url in enumerate(chunk, 1):
                self.TASKS.append(
                    asyncio.create_task(
                        self.get_all_goods_from_page(session, url, url_index, group, chapter)
                    )
                )
                self.PREVIOUS_ACTIVE_PAGE = ''
            await asyncio.gather(*self.TASKS)

    def run_car_item_tasks(self):
        if not type(self).URLS_WITH_ATTRS_GROUPS:
            type(self).URLS_WITH_ATTRS_GROUPS = self.read_file('data/urls/urls_with_attrs_groups.json', isjson=True)

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
                    self.write_to_json(self.DEFAULT_URL_PATH, 'all_goods_urls.json', self.ALL_GOODS_URLS, isadd=True)
                    self.write_to_json(f"{self.DEFAULT_URL_PATH_ERRORS}/{self.get_datetime(True)}",
                                       f'[{self.get_datetime()}]_ERRORS_goods_urls.json', self.ERRORS, isadd=True)
                    self.write_to_file(f"{self.DEFAULT_URL_PATH_ERRORS}/{self.get_datetime(True)}",
                                       f'[{self.get_datetime()}]_ERRORS_URLS_goods_urls.txt', self.ERRORS_URLS,
                                       workmode='a')
                    # self.write_to_file(self.DEFAULT_URL_PATH_CONTINUES, 'urls_with_car_models.txt',
                    #                    self.URLS_WITH_ATTRS_GROUPS[(chunk_id + 1) * 100:], workmode='a')
                    if chunk_id == 1:
                        break
                break
            break

        type(self).URL_COUNTER = 0

        self.ERRORS.clear()
        self.ERRORS_URLS.clear()

    def get_data(self, soup, url):
        result = {}
        # soup = BeautifulSoup(response.content, 'lxml')
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
        chapter = ''
        group = soup.find('h1', class_='auto-heading onestring').find('span').find('b').text.strip()
        # group = (url.get('href') for url in soup.find('div', id='js-breadcrumbs').find_all('a')[-2:])
        # group = soup.find('div', id='col-sm-9 automobile-left-col')
        # print('\t\t\t\t\t', group)
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
                'Раздел': chapter,
                'Группа': group,
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

    async def get_data_from_page(self, session, url, url_index):
        await self.get_delay(1, 3)
        self.URL_COUNTER += 1
        try:
            async with session.get(url, headers=self._get_header()) as response:
                soup = self.get_soup(await response.read())
                if (a := soup.find('div', class_='row block404')):
                    raise ValueError(f'Error 404 - {a.text}')
                self.DATA_FOR_CSV.append(
                    self.get_data(soup, url)
                )
                print(f"\t[{self.URL_COUNTER}][SUCCESS id {url_index}] ДАННЫЕ СОБРАНЫ ПО: {url}")
        except Exception as e:
            self.ERRORS.setdefault('get_data_from_page', {}).setdefault(f"{url_index}. {url}", tuple(e.args))
            self.ERRORS_URLS.add(url)
            print(f"\t[ERROR id {url_index}] ОШИБКА! ")

    async def get_tasks_car_items(self, chunk_urls):
        self.TASKS.clear()
        print(f'[INFO] Формирование задач для начала сбора url товаров...')
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(0), trust_env=True) as session:
            for url_index, url in enumerate(chunk_urls, 1):
                self.TASKS.append(
                    asyncio.create_task(
                        self.get_data_from_page(session, url, url_index)
                    )
                )

            await asyncio.gather(*self.TASKS)

    def run_get_data_from_page_tasks(self):
        if not type(self).ALL_GOODS_URLS:
            type(self).ALL_GOODS_URLS = tuple(self.read_file('all_goods_urls.txt'))
            chunks = self.get_chunks(self.ALL_GOODS_URLS, 100)
        for chunk_id, chunk_urls in enumerate(chunks):
            print('-' * 100)
            print(f'{"\t" * 10} Chunk #{chunk_id}')
            print('-' * 100)
            asyncio.run(
                self.get_tasks_car_items(chunk_urls)
            )
            self.write_to_json(f"{self.DEFAULT_URL_PATH_ERRORS}/{self.get_datetime(True)}",
                               f'[{self.get_datetime()}]_ERRORS_data_items.json', self.ERRORS, isadd=True)
            self.write_to_file(f"{self.DEFAULT_URL_PATH_ERRORS}/{self.get_datetime(True)}",
                               f'[{self.get_datetime()}]_ERRORS_URLS_data_items.txt', self.ERRORS_URLS,
                               workmode='a')
            self.write_to_file(self.DEFAULT_URL_PATH_CONTINUES, 'all_goods_urls.txt',
                               self.ALL_GOODS_URLS[(chunk_id + 1) * 100:])
            self.write_to_json(self.DEFAULT_URL_PATH, 'all_data_items.json', self.DATA_FOR_CSV, isadd=True)
            break

        self.write_to_csv(self.DEFAULT_URL_PATH, f'RESULT.csv', self.DATA_FOR_CSV)
        type(self).URL_COUNTER = 0

    def run_all_tasks(self):
        # Эта часть ищет все ссылки брендов на каждую группу товара.
        # start = time.monotonic()
        # parser.run_attrs_groups_tasks()
        # end = time.monotonic()
        # print(
        #     f"Время работы скрипта получение списка ({len(self.URLS_WITH_ATTRS_GROUPS)}): {end - start} секунд. \n{'=' * 50}")
        #
        # print()

        # Эта часть ищет ссылка на сами товары.
        start = time.monotonic()
        self.run_car_item_tasks()
        end = time.monotonic()
        print(
            f"Время работы скрипта получение списка ссылок на товары({self.get_length(self.URLS_WITH_ATTRS_GROUPS)}): {end - start} секунд. \n{'=' * 50}")

        print()

        # # Эта часть ищет данны по списку ссылок и затем сохраняет в csv
        # start = time.monotonic()
        # self.run_get_data_from_page_tasks()
        # end = time.monotonic()
        # print(
        #     f"Время работы скрипта получение списка ссылок на товары({len(self.ALL_GOODS_URLS)}): {end - start} секунд. \n{'=' * 50}")


if __name__ == '__main__':
    # TODO в каталоге continues после того как файл будет пустым рассмотреть необходимость его удаления
    parser = ParserBamberBy()
    parser.run_all_tasks()

    # with open('import.csv', 'r', encoding='cp1251') as f:
    #     reader = csv.DictReader(f)
    #
    #     with open('import_utf.csv', 'w', newline='', encoding='utf-8') as f:
    #         writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
    #         writer.writeheader()
    #         for row in reader:
    #             writer.writerow(row)
    # pprint(reader.fieldnames)
