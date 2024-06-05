import asyncio
import csv
import datetime
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
    BASE_URLS_CATEGORIES = 'https://bamper.by/catalog/zapchasti/'
    # PARSER_URL = 'https://bamper.by/zapchast_elektrogidrousilitel-rulya/8792-1084241/'
    DEFAULT_URL_PATH = "data/urls"
    DEFAULT_URL_PATH_ERRORS = "data/urls/errors"
    DEFAULT_URL_PATH_CONTINUES = "data/urls/continues"

    RESULT_CSV = []

    LINKS = []
    HEADERS = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'User-Agent': UserAgent().random,
    }
    TASKS = []
    URLS_WITH_CAR_BRAND = []
    URLS_WITH_CAR_MODEL = []
    ALL_GOODS_URLS = []

    DATA_FOR_CSV = []
    FIELDNAME = (
        'Группа',
        'Раздел',
        'Артикул',
        'Название',
        'Примечания',
        'Номер запчасти',
        'Цена',
        'Валюта',
    )

    ERRORS = {}
    ERRORS_URLS = set()
    PREVIOUS_ACTIVE_PAGE = ''
    URL_COUNTER = 0

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
                if isadd and ischeck_file:
                    old_data.update(
                        data
                    )
                    json.dump(old_data, f_json, indent=4, ensure_ascii=False)
                else:
                    json.dump(data, f_json, indent=4, ensure_ascii=False)

            print(f'[INFO] File "{path}/{filename}" is create\n')

    @staticmethod
    def write_to_file(path, filename=None, data=None, workmode='r', istxt=False):
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

                with open(f"{path}/{filename}", mode='w', encoding='utf-8-sig') as f_csv:
                    writer = csv.DictWriter(f_csv, fieldnames=data)
                    writer.writeheader()
                    writer.writerows(data)
            else:
                print(f'\t[INFO] File is not create!\t\tdata:"{data}"\n\t\tfilename: "{path}/{filename}"')

    @staticmethod
    def read_file(path):
        __class__.check_dirs(path, check_file=True)

        with open(path, 'r', encoding='utf-8-sig') as f:
            return (row.strip() for row in f.readlines())

    @staticmethod
    def get_active_page(soup):
        return soup.find('div', class_='pagination-bar').find('li', class_='active').text

    @staticmethod
    def check_pagination(soup):
        if (pagination_bar := soup.find('div', class_='pagination-bar').find('a', class_='modern-page-next')):
            return pagination_bar.get('href')
        else:
            return '1 страница'

    def get_main_urls(self, url):
        """
         get urls list from main page:  self.BASE_URLS_CATEGORIES
        :param url:
        :return:
        """
        result = []
        response = requests.get(url, headers=self._get_header()).text
        soup = BeautifulSoup(response, 'html.parser')
        for row in soup.find('div', class_='relative').find_all('ul', class_='cat-list'):
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

    async def get_delay(self, start, stop):
        await asyncio.sleep(randrange(start, stop))

    def get_chunks(self, obj, shift):
        return (obj[i:i + shift] for i in range(0, len(obj), shift))

    async def _parsing_urls_from_soup(self, session, url, url_index):
        print(f"[INFO id {url_index}] Сбор данных по {url}")
        async with session.get(url, headers=self._get_header()) as response:
            soup = self.get_soup(await response.read())
            return self.get_urls_from_soup(soup)

    async def get_list_car_brands_url(self, session, url, url_index):  # url это ссылка с self.BASE_URLS_CATEGORIES
        await self.get_delay(1, 2)
        try:
            data = await self._parsing_urls_from_soup(session, url, url_index)
        except Exception as e:
            data = ''
            self.ERRORS.setdefault('get_list_car_brands_url', {}).setdefault(f"{url_index}. {url}", tuple(e.args))
            self.ERRORS_URLS.add(url)
        if data:
            self.URLS_WITH_CAR_BRAND.extend(
                data
            )
            print(f"\t[SUCCESS id {url_index}] ДАННЫЕ СОБРАНЫ ПО: {url}")
        else:
            print(f"\t[ERROR id {url_index}] ОШИБКА! ")

    async def get_list_car_models_url(self, session, url, url_index):
        with open('text.txt', 'a', encoding='utf-8-sig') as f:
            print(url, file=f)
        await self.get_delay(1, 2)
        try:
            data = await self._parsing_urls_from_soup(session, url, url_index)
        except Exception as e:
            data = ''
            self.ERRORS.setdefault('get_list_car_models_url', {}).setdefault(f"{url_index}. {url}", tuple(e.args))
            self.ERRORS_URLS.add(url)
        if data:
            self.URLS_WITH_CAR_MODEL.extend(
                data
            )
            print(f"\t[SUCCESS id {url_index}] ДАННЫЕ СОБРАНЫ ПО: {url}")
        else:
            print(f"\t[ERROR id {url_index}] ОШИБКА! ")

    async def get_tasks_car_brands(self):
        self.GOODS_ALL_URL_LIST = self.get_main_urls(self.BASE_URLS_CATEGORIES)
        # self.write_to_file(self.DEFAULT_URL_PATH, 'main_url.txt', self.GOODS_ALL_URL_LIST)  # файл для теста(проверка урлов брендов)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(0), trust_env=True) as session:
            for url_index, url in enumerate(self.GOODS_ALL_URL_LIST, 1):
                self.TASKS.append(
                    asyncio.create_task(self.get_list_car_brands_url(session, url, url_index))
                )
            await asyncio.gather(*self.TASKS)

    def run_car_brands_tasks(self):
        asyncio.run(
            self.get_tasks_car_brands()
        )
        self.write_to_file(self.DEFAULT_URL_PATH, 'urls_with_car_brands.txt', self.URLS_WITH_CAR_BRAND, workmode='a')
        self.write_to_json(self.DEFAULT_URL_PATH, 'ERRORS_car_brands.json', self.ERRORS)
        self.write_to_file(self.DEFAULT_URL_PATH_ERRORS, 'ERRORS_URLS_car_brands.txt', self.ERRORS_URLS, workmode='w')

        self.ERRORS.clear()
        self.ERRORS_URLS.clear()

    async def get_tasks_car_models(self, chunk):
        self.TASKS.clear()
        print(f'[INFO] Формирование задач для начала сбора url моделей...')
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(0), trust_env=True) as session:
            for url_index, url in enumerate(chunk):
                self.TASKS.append(
                    asyncio.create_task(
                        self.get_list_car_models_url(session, url, url_index)
                    )
                )

            await asyncio.gather(*self.TASKS)

    def run_car_model_tasks(self):
        if not type(self).URLS_WITH_CAR_BRAND:
            type(self).URLS_WITH_CAR_BRAND = tuple(self.read_file('data/urls/urls_with_car_brands.txt'))
            chunks = self.get_chunks(self.URLS_WITH_CAR_BRAND, 100)
        for chunk in chunks:
            asyncio.run(
                self.get_tasks_car_models(chunk)
            )
            self.write_to_file(self.DEFAULT_URL_PATH, 'urls_with_car_models.txt', self.URLS_WITH_CAR_MODEL,
                               workmode='a')
            self.write_to_json(self.DEFAULT_URL_PATH, 'ERRORS_car_models.json', self.ERRORS, isadd=True)
            self.write_to_file(self.DEFAULT_URL_PATH_ERRORS, 'ERRORS_URLS_car_models.txt', self.ERRORS_URLS,
                               workmode='a')

        self.ERRORS.clear()
        self.ERRORS_URLS.clear()

    async def get_all_goods_from_page(self, session, url, url_index):
        await self.get_delay(1, 3)
        self.URL_COUNTER += 1
        print(f"[{self.URL_COUNTER}][ INFO id {url_index}] Сбор данных по {url}")
        flag = True
        # istime = False
        while flag:
            # if datetime.now().time().strftime("%H:%M:%S") == "08:58:59":
            #     istime = True
            #     break
            try:
                async with session.get(url, headers=self._get_header()) as response:
                    soup = self.get_soup(await response.read())
                    for row_index, row in enumerate(
                            soup.find('div', class_='list-wrapper').find_all('div', class_='add-image'), 1):
                        self.ALL_GOODS_URLS.append(
                            self.BASE_URL + row.find('a').get('href')
                        )
                    paginagions = self.check_pagination(soup)
                    print('\t\t\t\t', paginagions)
                    if paginagions != '1 страница':
                        if self.BASE_URLS_CATEGORIES == self.get_active_page(soup):
                            flag = False
                            continue
                        url = self.BASE_URL + paginagions
                        self.PREVIOUS_ACTIVE_PAGE = self.get_active_page(soup)
                    else:
                        flag = False
                    print(f"\t[SUCCESS id {url_index}] ДАННЫЕ СОБРАНЫ ПО: {url}")
            except Exception as e:
                self.ERRORS.setdefault('get_all_goods_from_page', {}).setdefault(f"{url_index}. {url}", tuple(e.args))
                self.ERRORS_URLS.add(url)
                print(f"\t[ERROR id {url_index}] ОШИБКА! ")
        # if istime:
        #     return

    async def get_tasks_car_goods(self, chunk):
        self.TASKS.clear()
        print(f'[INFO] Формирование задач для начала сбора url товаров...')
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(0), trust_env=True) as session:
            # for url_index, url in enumerate(self.URLS_WITH_CAR_MODEL, 1):
            for url_index, url in enumerate(chunk, 1):
                self.TASKS.append(
                    asyncio.create_task(
                        self.get_all_goods_from_page(session, url, url_index)
                    )
                )
            await asyncio.gather(*self.TASKS)

    def run_car_item_tasks(self):
        if not type(self).URLS_WITH_CAR_MODEL:
            type(self).URLS_WITH_CAR_MODEL = tuple(self.read_file('data/urls/urls_with_car_models.txt'))
            chunks = self.get_chunks(self.URLS_WITH_CAR_MODEL, 100)
        for chunk_num, chunk_urls in enumerate(chunks):
            print('-' * 100)
            print(f'{"\t" * 10} Chunk #{chunk_num}')
            print('-' * 100)
            asyncio.run(
                self.get_tasks_car_goods(chunk_urls)
            )
            self.write_to_file(self.DEFAULT_URL_PATH, 'all_goods_urls.txt', self.ALL_GOODS_URLS, workmode='a')
            self.write_to_json(self.DEFAULT_URL_PATH, 'ERRORS_goods_urls.json', self.ERRORS, isadd=True)
            self.write_to_file(self.DEFAULT_URL_PATH_ERRORS, 'ERRORS_URLS_goods_urls.txt', self.ERRORS_URLS,
                               workmode='w')
            self.write_to_json(self.DEFAULT_URL_PATH_CONTINUES, 'all_goods_urls.json', self.ALL_GOODS_URLS)
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
        url_name = ''
        # url_name = (url.get('href') for url in soup.find('div', id='js-breadcrumbs').find_all('a')[-2:])
        # url_name = soup.find('div', id='col-sm-9 automobile-left-col')
        # print('\t\t\t\t\t', url_name)
        if (a := soup.find('div', style="font-size: 17px;")):
            engine_v = a.text.split(',')[0].strip()
        else:
            engine_v = ''
        result.update(
            {
                'url': url,
                'goods_name': item_name,
                'price': price,
                'units': units,
                'item_comment': item_comment,
                'vendor_code': vendor_code,
                'item_number': item_number,
                'city': city,
                'engine_v': engine_v,
                'url_name': ' - '.join(url_name)
            }
        )
        return result

    async def get_data_from_page(self, session, url, url_index):
        await self.get_delay(1, 3)
        self.URL_COUNTER += 1
        try:
            async with session.get(url, headers=self._get_header()) as response:
                soup = self.get_soup(await response.read())
                self.DATA_FOR_CSV.append(
                    self.get_data(soup, url)
                )
                print(f"\t[SUCCESS id {url_index}] ДАННЫЕ СОБРАНЫ ПО: {url}")
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
            type(self).ALL_GOODS_URLS = tuple(self.read_file('data/urls/all_goods_urls.txt'))
            chunks = self.get_chunks(self.ALL_GOODS_URLS, 100)
        for chunk_num, chunk_urls in enumerate(chunks):
            print('-' * 100)
            print(f'{"\t" * 10} Chunk #{chunk_num}')
            print('-' * 100)
            asyncio.run(
                self.get_tasks_car_items(chunk_urls)
            )
            # self.write_to_file(self.DEFAULT_URL_PATH, 'all_goods_urls.txt', self.ALL_GOODS_URLS, workmode='a')
            self.write_to_json(self.DEFAULT_URL_PATH_ERRORS, 'ERRORS_data_items.json', self.ERRORS, isadd=True)
            self.write_to_file(self.DEFAULT_URL_PATH_ERRORS, 'ERRORS_URLS_data_items.txt', self.ERRORS_URLS,
                               workmode='w')
            self.write_to_json(self.DEFAULT_URL_PATH_CONTINUES, 'all_goods_urls.txt',
                               self.ALL_GOODS_URLS[chunk_num * 100:])
            self.write_to_json(self.DEFAULT_URL_PATH, 'all_data_items.json', self.DATA_FOR_CSV)
            break
        type(self).URL_COUNTER = 0

    def run_all_tasks(self):
        # Эта часть ищет все ссылки брендов на каждую группу товара.
        # print(f"{'='*50}\nНачат сбор урлов брендов:\n{'='*50}")
        # start = time.monotonic()
        # parser.run_car_brands_tasks()
        # end = time.monotonic()
        # print(f"Время работы скрипта получение списка брендов({len(self.URLS_WITH_CAR_BRAND)}): {end - start} секунд. \n{'='*50}")

        print()

        # Эта часть ищет ссылки на все модели брендов
        # print(f"{'=' * 50}\nНачат сбор урлов моделей:\n{'=' * 50}")
        # start = time.monotonic()
        # parser.run_car_model_tasks()
        # end = time.monotonic()
        # print(
        #     f"Время работы скрипта получение списка моделей({len(self.URLS_WITH_CAR_MODEL)}): {end - start} секунд. \n{'=' * 50}")

        print()

        # # Эта часть ищет ссылка на сами товары.
        # start = time.monotonic()
        # self.run_car_item_tasks()
        # end = time.monotonic()
        # print(
        #     f"Время работы скрипта получение списка ссылок на товары({len(self.ALL_GOODS_URLS)}): {end - start} секунд. \n{'=' * 50}")

        print()

        # Эта часть ищет данны по списку ссылок и затем сохраняет в csv
        start = time.monotonic()
        self.run_get_data_from_page_tasks()
        end = time.monotonic()
        print(
            f"Время работы скрипта получение списка ссылок на товары({len(self.ALL_GOODS_URLS)}): {end - start} секунд. \n{'=' * 50}")


if __name__ == '__main__':
    # TODO: проверить как обрабатывать пагинацию
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
