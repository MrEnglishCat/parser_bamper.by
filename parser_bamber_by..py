import asyncio
import csv
import json
import os
import sys
import time
from asyncio import tasks
from pprint import pprint

import aiohttp
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

# sys.stdin.reconfigure(encoding='utf-8')
# sys.stdout.reconfigure(encoding='utf-8')


class ParserBamberBy:
    BASE_URL = 'https://www.bamper.by'
    BASE_URLS_CATEGORIES = 'https://bamper.by/catalog/zapchasti/'
    # PARSER_URL = 'https://bamper.by/zapchast_elektrogidrousilitel-rulya/8792-1084241/'
    RESULT_CSV = []
    LINKS = []
    HEADERS = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'User-Agent': UserAgent().random,
        # 'Content-Type': 'text/html; charset=UTF-8',
        # 'Accept-Encoding': 'gzip, deflate, br',
        # 'Accept-Language': 'ru,en-US;q=0.9,en;q=0.8',
    }
    TASKS = []

    @staticmethod
    def check_dirs(path):
        if not os.path.exists(path):
            os.makedirs(path)

    @staticmethod
    def write_to_file(path, filename=None, data=None, isjson=False):
        __class__.check_dirs(path),

        if filename and data:
            with open(f"{path}/{filename}", 'w',
                      encoding='utf-8-sig',
                      newline='\n' if isjson else '') as f:  # кодировка UTF-8-sig в ней MSexcel и другие(WPS, libre) распознают русские буквы
                if isjson:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                else:
                    print(data)

                    # ??? CSV
                    f.writelines(data)
            print(f'File {filename} is create')
        else:
            print('no data or filename')

    def get_main_urls(self, url):
        """
         get urls list from main page:  self.BASE_URLS_CATEGORIES
        :param url:
        :return:
        """
        result = []
        response = requests.get(url, headers=self.HEADERS).text
        soup = BeautifulSoup(response, 'html.parser')
        for row in soup.find('div', class_='relative').find_all('ul', class_='cat-list'):
            for row_row in row.find_all('li'):
                result.append(
                    self.BASE_URL + row_row.find('a').get('href')
                )

        return result

    def get_soup(self, response):
        # response = requests.get(url, headers=self.HEADERS).text
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

    async def _parsing_urls_from_soup(self, session,  url):
        async with session.get(url, headers=self.HEADERS) as response:
            soup = self.get_soup(await response.read())
            return self.get_urls_from_soup(soup)

    # async def get_other_urls(self, session,  url):
    async def get_other_urls(self, session, url):  # url это ссылка с self.BASE_URLS_CATEGORIES
        urls_with_car_brand = []
        urls_with_car_model = []
        all_goods_urls = []
        # for async
        # links = self._parsing_urls_from_soup(url)
        # urls_with_car_brand.extend(
        #     links
        # )
        # for u in url:
        print(f"\t[INFO] car_brand_url: {url}")
        urls_with_car_brand.extend(
            await self._parsing_urls_from_soup(session, url)
        )


        for car_model_url in urls_with_car_brand:
            urls_with_car_model.extend(
                await self._parsing_urls_from_soup(session, car_model_url)
            )
            print(f"\t[INFO] car_model_url: {car_model_url}")
            for goods_url in urls_with_car_model:
                async with session.get(goods_url, headers=self.HEADERS) as response:
                    soup = self.get_soup(await response.read())

                    try:
                        pagination = soup.find('div', class_='pagination-bar').find('a', class_='modern-page-next').get('href')
                    except Exception as e:
                        pagination = '-=1 PAGE =-'
                    print(f"\t\t[INFO] goods_url:{pagination} ---  {goods_url}")
                    # for row in soup.find('div', class_='tab-pane active').find_all('div', class_='item-list'):
                    for row in soup.find_all('div', class_='item-list'):
                        # print('-----', row)
                        # result_url = row.find('div', class_='add_image').find('a').get('href')
                        all_goods_urls.append(
                            # result_url
                            self.BASE_URL + row.find('a').get('href')
                        )
    # break
        print(f'[INFO] link: {url}')

        self.write_to_file('data', 'urls_with_car_brand.json', urls_with_car_brand, isjson=True)
        self.write_to_file('data', 'urls_with_car_model.json', urls_with_car_model, isjson=True)
        self.write_to_file('data', 'all_goods_urls.json', urls_with_car_model, isjson=True)
        return all_goods_urls

    def get_data_from_page(self, url):
        result = {}
        response = requests.get(url, headers=self.HEADERS)
        # soup = BeautifulSoup(response.text, 'html.parser')
        soup = BeautifulSoup(response.content, 'lxml')
        goods_name = soup.find('h1', class_='auto-heading onestring').find('span').text.strip()
        price = soup.find('meta', itemprop='price').get('content')
        units = soup.find('meta', itemprop='priceCurrency').get('content')
        media_hiting = soup.find('span', class_='media-heading cut-h-375').text.strip()
        vendor_code = soup.find('span', class_='data-type f13').text
        goods_number = soup.find('span', class_='media-heading cut-h-65').text.strip()
        # city = soup.find('div', class_='seller-info').find('span', class_='float:left;').find('b').text.strip()
        # url_name = (url.get('href') for url in soup.find('div', id='js-breadcrumbs').find_all('a')[-2:])

        if (a := soup.find('div', style="font-size: 17px;")):
            engine_v = a.text.split(',')[0].strip()
        else:
            engine_v = ''
        result.update(
            {
                'goods_name': goods_name,
                'price': price,
                'units': units,
                'media_hiting': media_hiting,
                'vendor_code': vendor_code,
                'goods_number': goods_number,
                # 'city': city,
                'engine_v': engine_v,
                # 'url_name':' - '.join(url_name)
            }
        )
        # pprint(result)

    async def get_tasks(self):
        self.GOODS_ALL_URL_LIST = self.get_main_urls(self.BASE_URLS_CATEGORIES)
        self.write_to_file('data', 'main_url.json', self.GOODS_ALL_URL_LIST, isjson=True)
        async with aiohttp.ClientSession(trust_env=True) as session:
            for url in [self.GOODS_ALL_URL_LIST[0]]:
                self.TASKS.append(
                    asyncio.create_task(self.get_other_urls(session, url))
                )



            await asyncio.gather(*self.TASKS)

    def run(self):
        asyncio.run(
            self.get_tasks()
        )
        # self.GOODS_ALL_URL_LIST = self.get_main_urls(self.BASE_URLS_CATEGORIES)
        # self.write_to_file('data', 'main_url.json', self.GOODS_ALL_URL_LIST, isjson=True)
        # all_goods_urls = self.get_other_urls(self.GOODS_ALL_URL_LIST)
        # self.get_page(self.PARSER_URL)

# TODO: проверить как обрабатывать пагинацию
parser = ParserBamberBy()
start = time.monotonic()
parser.run()
end = time.monotonic()

# with open('import.csv', 'r', encoding='cp1251') as f:
#     reader = csv.DictReader(f)
#
#     with open('import_utf.csv', 'w', newline='', encoding='utf-8') as f:
#         writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
#         writer.writeheader()
#         for row in reader:
#             writer.writerow(row)
# pprint(reader.fieldnames)
