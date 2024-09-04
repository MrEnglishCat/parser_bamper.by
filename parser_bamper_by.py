import asyncio
import csv
import json
import os
import sys
import time
import re
# from lib2to3.btm_utils import reduce_tree

import nest_asyncio

import aiohttp
import requests
from pprint import pprint
from random import randrange
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from datetime import datetime


# Token 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
# git clone https://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@github.com/MrEnglishCat/parser_bamper.by.git
# sys.stdin.reconfigure(encoding='utf-8')  # если в терминале проблемы с кодировкой, то раскомитить 17 и 18 строчки, либо изменить кодировку в терминале
# sys.stdout.reconfigure(encoding='utf-8')

nest_asyncio.apply()


class ParserBamperBy:
    """
    Парсер всех найденных позиций на сайте bamper.by.
    Парсинг начинается со ссылки BASE_URLS_CATEGORIES = 'https://bamper.by/catalog/modeli/'
    """
    BASE_URL = 'https://www.bamper.by'
    BASE_URLS_CATEGORIES = 'https://bamper.by/catalog/modeli/'
    ALL_CAR_URL_LIST = []
    URLS_WITH_ATTRS_GROUPS = []
    # ALL_GOODS_URLS = []
    # DEFAULT_ ... пути для файлов
    DEFAULT_URL_PATH = "data/urls"
    DEFAULT_URL_PATH_ALL_GOODS_URLS = "data/urls/all_goods_urls"
    DEFAULT_URL_PATH_CSV = "data/result"
    DEFAULT_URL_PATH_ERRORS = f"data/urls/errors"
    DEFAULT_URL_PATH_CONTINUES = f"data/urls/continues"
    DEFAULT_TEST_URL_PATH = "data/test"  # TODO TEST каталог для тестовых файлов
    COOKIES = None

    HEADERS = {
        'Accept': '*/*',
        # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'User-Agent': UserAgent().random,
    }

    CSV_FIELDNAMES = (
        'Производитель',
        'Модель',
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
        'Ссылки на фото',
    )

    def __init__(self):
        self.TASKS = []
        self.OBJ_ID = None
        # для сбора данных
        self.URLS_WITH_ATTRS_GROUPS = []
        self.ALL_GOODS_URLS = []

        # для работы с CSV/json
        self.DATA_FOR_CSV = []
        self.RESULT_CSV = []

        self.ERRORS = {}
        self.ERRORS_URLS = set()
        self.URL_COUNTER = 0  # счётчик ссылок

    @staticmethod
    def _get_length_dict(obj: dict) -> int:
        """
        Получение общего количества ссылок. Работает с определенной вложенностью словаря dict
        """
        result = 0
        for chapter in obj.values():
            for row in chapter.values():
                result += len(row)
        return result

    @staticmethod
    def _get_length_iterable(obj: list | tuple) -> int:
        return len(obj)

    def _set_first_cookies(self, response):
        """
        Пока что нигде не используется
        Args:
            response:

        Returns:

        """
        self.COOKIES = response.cookies

    @staticmethod
    def _get_cookies(response: aiohttp.ClientResponse=None) -> dict:
        """

        Args:
            response: aiohttp.ClientRespons. Default is None

        Returns: dict | cookies

        """
        cookies = {
            '_ym_uid': '1717400752994434456',
            '_ym_d': '1717400752',
            '_gid': 'GA1.2.855576063.1725315174',
            'BITRIX_SM_aLastSearch': 'a%3A10%3A%7Bi%3A0%3Ba%3A2%3A%7Bs%3A5%3A%22title%22%3BN%3Bs%3A3%3A%22url%22%3Bs%3A76%3A%22%2Fzchbu%2Flocal%2Ftemplates%2Fbsclassified%2Fassets%2Fplugins%2Fswipperswiper.min.js.map%2F%22%3B%7Di%3A1%3Ba%3A2%3A%7Bs%3A5%3A%22title%22%3Bs%3A43%3A%22%D0%97%D0%B0%D0%BF%D1%87%D0%B0%D1%81%D1%82%D0%B8%20%20Aito%20M5%2C%202022-2024%20%D0%B3.%D0%B2.%22%3Bs%3A3%3A%22url%22%3Bs%3A41%3A%22%2Fzchbu%2Fmarka_aito%2Fmodel_m5%2Fgod_2022-2024%2F%22%3B%7Di%3A2%3Ba%3A2%3A%7Bs%3A5%3A%22title%22%3BN%3Bs%3A3%3A%22url%22%3Bs%3A76%3A%22%2Fzchbu%2Flocal%2Ftemplates%2Fbsclassified%2Fassets%2Fplugins%2Fswipperswiper.min.js.map%2F%22%3B%7Di%3A3%3Ba%3A2%3A%7Bs%3A5%3A%22title%22%3Bs%3A43%3A%22%D0%97%D0%B0%D0%BF%D1%87%D0%B0%D1%81%D1%82%D0%B8%20%20Aito%20M5%2C%202022-2024%20%D0%B3.%D0%B2.%22%3Bs%3A3%3A%22url%22%3Bs%3A41%3A%22%2Fzchbu%2Fmarka_aito%2Fmodel_m5%2Fgod_2022-2024%2F%22%3B%7Di%3A4%3Ba%3A2%3A%7Bs%3A5%3A%22title%22%3BN%3Bs%3A3%3A%22url%22%3Bs%3A76%3A%22%2Fzchbu%2Flocal%2Ftemplates%2Fbsclassified%2Fassets%2Fplugins%2Fswipperswiper.min.js.map%2F%22%3B%7Di%3A5%3Ba%3A2%3A%7Bs%3A5%3A%22title%22%3Bs%3A28%3A%22CD-%D1%87%D0%B5%D0%B9%D0%BD%D0%B4%D0%B6%D0%B5%D1%80%20Acura%20CL%22%3Bs%3A3%3A%22url%22%3Bs%3A51%3A%22%2Fzchbu%2Fzapchast_cd-cheyndzher%2Fmarka_acura%2Fmodel_cl%2F%22%3B%7Di%3A6%3Ba%3A2%3A%7Bs%3A5%3A%22title%22%3BN%3Bs%3A3%3A%22url%22%3Bs%3A76%3A%22%2Fzchbu%2Flocal%2Ftemplates%2Fbsclassified%2Fassets%2Fplugins%2Fswipperswiper.min.js.map%2F%22%3B%7Di%3A7%3Ba%3A2%3A%7Bs%3A5%3A%22title%22%3Bs%3A28%3A%22CD-%D1%87%D0%B5%D0%B9%D0%BD%D0%B4%D0%B6%D0%B5%D1%80%20Acura%20CL%22%3Bs%3A3%3A%22url%22%3Bs%3A51%3A%22%2Fzchbu%2Fzapchast_cd-cheyndzher%2Fmarka_acura%2Fmodel_cl%2F%22%3B%7Di%3A8%3Ba%3A2%3A%7Bs%3A5%3A%22title%22%3BN%3Bs%3A3%3A%22url%22%3Bs%3A76%3A%22%2Fzchbu%2Flocal%2Ftemplates%2Fbsclassified%2Fassets%2Fplugins%2Fswipperswiper.min.js.map%2F%22%3B%7Di%3A9%3Ba%3A2%3A%7Bs%3A5%3A%22title%22%3Bs%3A28%3A%22CD-%D1%87%D0%B5%D0%B9%D0%BD%D0%B4%D0%B6%D0%B5%D1%80%20Acura%20CL%22%3Bs%3A3%3A%22url%22%3Bs%3A51%3A%22%2Fzchbu%2Fzapchast_cd-cheyndzher%2Fmarka_acura%2Fmodel_cl%2F%22%3B%7D%7D',
            '_ga': 'GA1.2.129790464.1717101810',
            '_ga_6M4HY0QKW3': 'GS1.1.1725465245.109.1.1725465590.0.0.0',
            'PHPSESSID': 'mvgtfst3aash4s33gdc2pt4gtf',
        }
        if response is None:
            return cookies
        return response.cookies


    @staticmethod
    def _get_header(response=None) -> dict:
        """
        Используется для рандомизации headers в запросах

        :return: заголовки/dict
        """

        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'ru,en;q=0.9',
            'cache-control': 'max-age=0',
            # TODO подумать как подгружать куки из приходящих ответов на запросы
            # 'cookie': 'store.test; videoblog_viewed=%5B%22623Ub5kg_co%22%5D; _ym_uid=1719992655224117596; _ym_d=1719992655; BX_USER_ID=3a669f33c2a47f32e32882596a3c4475; FCCDCF=%5Bnull%2Cnull%2Cnull%2C%5B%22CQDmW4AQDmW4AEsACBRUBCFoAP_gAEPgAATAKLpB_C7FbSlCwH53aPsEcAhPRtAwxoQhAAbBEuIFQAKQYBQCgkExNAzgFCACAAAAOCbBIQMECAAACVAgYAAQIAAMIAQQAAIIJAAAgAEAAEAYCAACAAAAEAIIAACBEAAAmBgAAIIACBAAhABACACACAKgAAABAgCAAAAACAEAAAAAAAgAkQgACAAAAAAAAAAIAAAAAAAAABAAAAAAAAAAAAgAAAAIJXJB_C7FbSlCyHhXYPsEMAhfRtAQxoQhAAbBImIFQAKQYAQCkkEzJEigECQAAAAAICRBIAEIAAgACFAhQAAQIBAMAAQQAAoIIAAAgCEAAEAQAAACAAAAEAIIAAAAEAAAiQhAAIICCBAAAAAAKACECACAAAAAAgAAAgAACAEAACAAgAAAkAgACAAAAAAAAAAIAAAAAAAAABAA%22%2C%222~43.55.57.70.89.93.108.122.124.144.149.196.230.236.259.266.286.311.313.322.323.358.370.385.413.415.424.436.445.449.486.494.495.540.560.574.587.591.609.821.827.864.899.931.938.979.981.1029.1033.1048.1051.1067.1092.1095.1097.1099.1126.1152.1188.1205.1225.1226.1227.1230.1276.1290.1301.1329.1365.1375.1415.1421.1423.1440.1449.1512.1514.1516.1542.1570.1577.1583.1598.1616.1651.1678.1697.1716.1720.1725.1735.1753.1765.1782.1800.1832.1845.1870.1878.1889.1898.1911.1917.1928.1958.1964.1969.1985.2010.2056.2072.2074.2135.2137.2166.2186.2222.2225.2253.2279.2292.2299.2312.2328.2331.2334.2336.2343.2354.2357.2373.2377.2387.2403.2405.2406.2407.2415.2427.2440.2461.2472.2501.2506.2517.2526.2527.2552.2567.2568.2571.2572.2575.2577.2584.2589.2604.2609.2614.2621.2624.2629.2645.2646.2657.2658.2660.2661.2669.2677.2767.2768.2792.2801.2817.2822.2827.2832.2834.2838.2849.2876.2878.2883.2887.2891.2893.2898.2900.2901.2920.2923.2931.2947.2965.2970.2973.2975.2979.2983.2987.2995.2999.3002.3008.3009.3018.3025.3043.3052.3055.3059.3075.3094.3099.3107.3119.3136.3155.3198.3210.3217.3225.3227.3228.3231.3234.3236.3237.3250.3253.3257.3270.3288.3289.3300.3316.3330.3831.9731.10631.11531.14237.14332.21233.23031.28031.29631.32531~dv.%22%2C%221F8E5EEE-DA1A-4D30-B5E2-CC9C10740E89%22%5D%5D; BITRIX_SM_KOMPLEKTY_SHOW1TIME=Y; store.test; _gid=GA1.2.1699092652.1725251911; __gads=ID=efda6d337bd78af5:T=1722334307:RT=1725284942:S=ALNI_MaVAsjAXscWPkZxUmRaX7gYYA4CAA; __gpi=UID=00000e818f569ac6:T=1722334307:RT=1725284942:S=ALNI_MYHZe1X9YWR21PomeZ19iIaP78hMQ; _ym_isad=2; store.test=; __eoi=ID=55f2cea5f4c2be64:T=1722334307:RT=1725456566:S=AA-AfjY7j0w_6G21IdulZQQ3E9ml; cf_clearance=hUgHj9luqg4i7Y6pU.v8JklWboK0Pb.4I0PrMCA1mT4-1725469585-1.2.1.1-Irgo.Hd_QNUEXJqlwVsik8bawGwTyRJ7KmQngsEWUslan4Hw02aDqsZUuTd3cAnNe6V9NIVuWUaNC0PxMZTzTGJmvk.UpbjAj1n7Bqr.78kRSi7Ab_WOgL.IQHfM2TqoV2xS1u4GZMSD1aeQnJAQ7xXvO8sN3II_88rp0anmoR6U5kG049p63QMKgS6yqmQmCnCeMNsYke4TX81SYkIvcJrEJ2hhxdy050a7sjKRGZEgbn.DYCe6OduKs4fHHLUM0SU75W0i6rmSF0JSXajNXcIeFWfehrcd4J3rWn187.1yHU.LCyMeh0PdMpqIfUwJ6hHOC1aFJynoU86o4Fm5SwCBUkOCsf9jGfVILgPIbmHjNwFzA90cztfn5JFIZDJ_QOFpRzWXajR69jTDVV.dSyJmYzN3nl24IOJzHoWTUv71fwCcNDJ0GQbT3PKCmD_N6TybywHeqcUF8pni4Iymgw; BITRIX_SM_aLastSearch=a%3A10%3A%7Bi%3A0%3Ba%3A2%3A%7Bs%3A5%3A%22title%22%3BN%3Bs%3A3%3A%22url%22%3Bs%3A76%3A%22%2Fzchbu%2Flocal%2Ftemplates%2Fbsclassified%2Fassets%2Fplugins%2Fswipperswiper.min.js.map%2F%22%3B%7Di%3A1%3Ba%3A2%3A%7Bs%3A5%3A%22title%22%3Bs%3A28%3A%22CD-%D1%87%D0%B5%D0%B9%D0%BD%D0%B4%D0%B6%D0%B5%D1%80%20Acura%20CL%22%3Bs%3A3%3A%22url%22%3Bs%3A51%3A%22%2Fzchbu%2Fzapchast_cd-cheyndzher%2Fmarka_acura%2Fmodel_cl%2F%22%3B%7Di%3A2%3Ba%3A2%3A%7Bs%3A5%3A%22title%22%3BN%3Bs%3A3%3A%22url%22%3Bs%3A76%3A%22%2Fzchbu%2Flocal%2Ftemplates%2Fbsclassified%2Fassets%2Fplugins%2Fswipperswiper.min.js.map%2F%22%3B%7Di%3A3%3Ba%3A2%3A%7Bs%3A5%3A%22title%22%3Bs%3A28%3A%22CD-%D1%87%D0%B5%D0%B9%D0%BD%D0%B4%D0%B6%D0%B5%D1%80%20Acura%20CL%22%3Bs%3A3%3A%22url%22%3Bs%3A51%3A%22%2Fzchbu%2Fzapchast_cd-cheyndzher%2Fmarka_acura%2Fmodel_cl%2F%22%3B%7Di%3A4%3Ba%3A2%3A%7Bs%3A5%3A%22title%22%3BN%3Bs%3A3%3A%22url%22%3Bs%3A76%3A%22%2Fzchbu%2Flocal%2Ftemplates%2Fbsclassified%2Fassets%2Fplugins%2Fswipperswiper.min.js.map%2F%22%3B%7Di%3A5%3Ba%3A2%3A%7Bs%3A5%3A%22title%22%3Bs%3A32%3A%22%D0%93%D0%B8%D1%80%D0%BE%D1%81%D0%BA%D0%BE%D0%BF%20Mitsubishi%20L200%22%3Bs%3A3%3A%22url%22%3Bs%3A53%3A%22%2Fzchbu%2Fzapchast_giroskop%2Fmarka_mitsubishi%2Fmodel_l200%2F%22%3B%7Di%3A6%3Ba%3A2%3A%7Bs%3A5%3A%22title%22%3Bs%3A33%3A%22%D0%92%D0%B0%D0%BA%D1%83%D1%83%D0%BC%D0%BD%D1%8B%D0%B9%20%D1%80%D0%B5%D1%81%D0%B8%D0%B2%D0%B5%D1%80%22%3Bs%3A3%3A%22url%22%3Bs%3A34%3A%22%2Fzchbu%2Fzapchast_vakuumnyy-resiver%2F%22%3B%7Di%3A7%3Ba%3A2%3A%7Bs%3A5%3A%22title%22%3Bs%3A58%3A%22%D0%92%D0%B0%D0%BA%D1%83%D1%83%D0%BC%D0%BD%D1%8B%D0%B9%20%D0%BC%D0%BE%D0%B4%D1%83%D0%BB%D1%8F%D1%82%D0%BE%D1%80%20%D0%90%D0%9A%D0%9F%D0%9F%20Fiat%20Ducato%22%3Bs%3A3%3A%22url%22%3Bs%3A66%3A%22%2Fzchbu%2Fzapchast_vakuumnyy-modulyator-akpp%2Fmarka_fiat%2Fmodel_ducato%2F%22%3B%7Di%3A8%3Ba%3A2%3A%7Bs%3A5%3A%22title%22%3Bs%3A65%3A%22%D0%91%D0%BB%D0%BE%D0%BA%20%D1%83%D0%BF%D1%80%D0%B0%D0%B2%D0%BB%D0%B5%D0%BD%D0%B8%D1%8F%20%D0%B0%D0%BA%D0%BA%D1%83%D0%BC%D1%83%D0%BB%D1%8F%D1%82%D0%BE%D1%80%D0%BE%D0%BC%20%28%D0%90%D0%9A%D0%91%29%22%3Bs%3A3%3A%22url%22%3Bs%3A52%3A%22%2Fzchbu%2Fzapchast_blok-upravleniya-akkumulyatorom-akb%2F%22%3B%7Di%3A9%3Ba%3A2%3A%7Bs%3A5%3A%22title%22%3Bs%3A217%3A%22%D0%91%D0%BB%D0%BE%D0%BA%20%D1%81%D0%BE%D0%B3%D0%BB%D0%B0%D1%81%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F%20%D1%84%D0%B0%D1%80%D0%BA%D0%BE%D0%BF%D0%B0%2C%20%D0%BC%D0%BE%D0%B4%D1%83%D0%BB%D1%8C%20%D0%BF%D0%BE%D0%B4%D0%BA%D0%BB%D1%8E%D1%87%D0%B5%D0%BD%D0%B8%D1%8F%20%D0%BF%D1%80%D0%B8%D1%86%D0%B5%D0%BF%D0%B0%2C%20%D0%B1%D0%BB%D0%BE%D0%BA%20%D1%83%D0%BF%D1%80%D0%B0%D0%B2%D0%BB%D0%B5%D0%BD%D0%B8%D1%8F%20%D0%B1%D1%83%D0%BA%D1%81%D0%B8%D1%80%D0%BE%D0%B2%D0%BE%D1%87%D0%BD%D0%BE%D0%B3%D0%BE%20%D0%BA%D1%80%D1%8E%D0%BA%D0%B0%2C%20%D0%B1%D0%BB%D0%BE%D0%BA%20%D1%83%D0%BF%D1%80%D0%B0%D0%B2%D0%BB%D0%B5%D0%BD%D0%B8%D1%8F%20%D1%84%D0%B0%D1%80%D0%BA%D0%BE%D0%BF%D0%B0%22%3Bs%3A3%3A%22url%22%3Bs%3A43%3A%22%2Fzchbu%2Fzapchast_blok-soglasovaniya-farkopa%2F%22%3B%7D%7D; PHPSESSID=9u0ar6t2i4jok1non4t8bvqtvi; _ga_6M4HY0QKW3=GS1.1.1725467731.27.1.1725469827.0.0.0; _ga=GA1.2.46988855.1719992655; _gat_UA-31751536-4=1; FCNEC=%5B%5B%22AKsRol9fxfJkR_bKhlTvhiBnVqmCziiQYZ1PHysnYF_FIjp-6pahOzFHSpggGhboR7h71vHH3rF2U6a4X9eKnNmkha6c4Bg9AUIXhO0zk6qvJdcsYoHUtHB4r-8vqxTYffmi5S48ZWgoy5Y7bpM03kOljg8JFv6VGg%3D%3D%22%5D%5D',
            # 'cookie': __class__._get_cookies(),
            'priority': 'u=0, i',
            # 'referer': 'https://bamper.by/zchbu/zapchast_cd-cheyndzher/marka_acura/model_cl/',  # TODO рассмотреть возможность передавать сюда ссылку.
            'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "YaBrowser";v="24.7", "Yowser";v="2.5"',
            'sec-ch-ua-arch': '"x86"',
            'sec-ch-ua-bitness': '"64"',
            'sec-ch-ua-full-version': '"24.7.2.1098"',
            'sec-ch-ua-full-version-list': '"Not/A)Brand";v="8.0.0.0", "Chromium";v="126.0.6478.234", "YaBrowser";v="24.7.2.1098", "Yowser";v="2.5"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-model': '""',
            'sec-ch-ua-platform': '"Windows"',
            'sec-ch-ua-platform-version': '"10.0.0"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 YaBrowser/24.7.0.0 Safari/537.36',
        }
        #TODO ниже комменты не удалять пока что. Почему-то сайт не выдает данные с UserAgent Возможно устарели данные в модуле UserAgent
        # return {
        #     # 'authority': 'bamper.by',
        #     # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        #     'Accept': '*/*',
        #     'accept-language': 'ru,en-US;q=0.9,en;q=0.8',
        #     'User-Agent': UserAgent().random,
        # }

        return headers

    @staticmethod
    def _check_dirs(path: str, check_file: bool = False) -> bool | None:
        """
        Используется для проверки наличия пути. Или проверки наличия файла(при check_file=True)

        :path:  При check_file=False - проверяет, существуют ли каталоги, если каких-то нету, то их создает, метод возвращает None
                При check_file=True - проверяет, существует ли указанный файл, если существует возвращает True, иначе False
        :result: True|False|None
        """
        if check_file:
            return os.path.exists(path) and os.path.isfile(path)
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
    def _write_to_csv(path: str, filename: str = None, data: dict = None) -> None:
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
    def _read_file(path: str, isjson: bool = False) -> None:
        """
        Чтение данных из txt или json

        :path путь до файла
        :isjson Если False - чтение будет из txt. Если True, то чтение из json
        :return None
        """
        empty = list()
        if __class__._check_dirs(path, check_file=True):

            with open(path, 'r', encoding='utf-8-sig') as f:
                if isjson:
                    return json.load(f)
                return (row.strip() for row in f.readlines())
        else:
            return empty

    @staticmethod
    def _get_active_page(soup: BeautifulSoup) -> str:
        """
        Метод для получения номера активной страницы

        soup: BeautifulSoup
        """
        return soup.find('div', class_='pagination-bar').find('li', class_='active').text.strip()

    @staticmethod
    def _check_pagination(soup: BeautifulSoup) -> str:
        """
        Проверка наличия пагинации

        soup: BeautifulSoup
        """
        try:
            if (pagination_bar := soup.find('div', class_='pagination-bar').find('a', class_='modern-page-next')):
                return pagination_bar.get('href')
            else:
                return '1 страница'
        except:
            return '1 страница'

    @staticmethod
    def _get_datetime(split: bool = False) -> str | list:
        """
            получение даты и времени. При необходимости(split=True) отделение даты от вермени
        """
        result = datetime.now().strftime("%d.%m.%Y_%H.%M.%S")
        if split:
            return result.split('_')[0]
        return result

    def get_main_urls(self, url: str) -> list:
        """
         get urls list from main page:  self.BASE_URLS_CATEGORIES

        :param url: сюда передается первая ссылка с которой начинается поиск
        :return: возвращает словарь с брендом - моделью - списком ссылок на модели
        """
        result = []
        car_brand = ''
        pattern_car_model = fr"(?<={car_brand}-).+"
        response = requests.get(url, headers=self._get_header(), cookies=self._get_cookies()).text
        soup = BeautifulSoup(response, 'html.parser')
        # list_of_data_to_be_processed = soup.find('div', class_='inner-box').find_all('div', class_='col-md-12')

        try:
            list_of_data_to_be_processed = soup.find('div', class_='inner-box relative').find_all('div',
                                                                                                  class_='col-md-12')
        except Exception as e:
            self.ERRORS.setdefault('get_main_urls', {}).setdefault(f"{url}", e.args)
            self.ERRORS_URLS.add(url)
            print(f"\t[ERROR  {url}] ОШИБКА! {e}")
            # TODO принты ниже удалить
            # print('=' * 100)
            # print(soup)
            # print('=' * 100)
            #
            list_of_data_to_be_processed = []

        for row in list_of_data_to_be_processed:
            car_brand = row.find('h3').text.strip()
            for row_row in row.find('div', class_='row').find_all('a'):
                car_model = row_row.find('b').text.strip()
                car_model_without_brand = re.search(pattern_car_model, car_model).group(0)
                # for row_row in row.find_all('li'):
                result.append(
                    [
                        car_brand,
                        car_model_without_brand,
                        self.BASE_URL + row_row.get('href')
                    ]

                )
        return result

    def get_soup(self, response: requests) -> BeautifulSoup:
        """
        Получение объекта BeautifulSoup для дальнейшего поиска элементов страницы
        """
        soup = BeautifulSoup(response, 'html.parser')
        return soup

    def get_urls_from_soup(self, soup: BeautifulSoup, car_brand: str, car_model: str) -> None:
        """
        Получает ссылки из объекта BeautifulSoup. Добавляет их в атрибут класса URLS_WITH_ATTRS_GROUPS
        """
        group = None
        # chapter = None
        for row in soup.find('div', class_='relative').find_all('li'):
            # print(row.text)
            row_text = row.text.strip()
            if 'list-header' in row.get('class', ''):
                group = row_text
                continue
            chapter = row_text
            type(self).URLS_WITH_ATTRS_GROUPS.append(
                [
                    car_brand,
                    car_model,
                    group,
                    chapter,
                    self.BASE_URL + row.find('a').get('href')
                ]
            )

    async def get_delay(self, start: int, stop: int) -> None:
        """
        Вносит рандомную задержку. Для пайзы в работе скрипта между запросами.

        :start начало для диапазона из которого выбирается время паузы
        :stop окончание для диапазона из которого выбирается время паузы
        """
        await asyncio.sleep(randrange(start, stop))

    def get_chunks(self, obj: list | tuple, chunk_length: int) -> list | tuple:
        """
        Делит большой итерируемый объект на более маленькие для удобства обработыки
        """

        return (obj[i:i + chunk_length] for i in range(0, len(obj), chunk_length))

    async def _parsing_urls_from_soup(self, session: aiohttp.ClientSession, url: str, url_index: int, car_brand: str,
                                      car_model: str,
                                      is_list_headers: bool = False) -> None:
        """
        Отправляет запрос к url и ответ передает в метод get_soup, результат которого передается в
        метод get_urls_from_soup.
        """
        self.URL_COUNTER += 1
        print(f"[{self.URL_COUNTER}][INFO id {url_index}] Сбор данных по {url}")
        async with session.get(url, headers=self._get_header(), cookies=self._get_cookies()) as response:
            soup = self.get_soup(await response.read())
            self.get_urls_from_soup(soup, car_brand, car_model)

    async def get_list_attr_groups_url(self, session: aiohttp.ClientSession, url: str,
                                       url_index: int, car_brand: str,
                                       car_model: str) -> None:  # url это ссылка с self.BASE_URLS_CATEGORIES
        """
            получение списка групп, разделов и ссылок на сами товары
        """
        await self.get_delay(2, 3)
        try:
            await self._parsing_urls_from_soup(session, url, url_index, car_brand, car_model, is_list_headers=True)
            print(f"\t[SUCCESS id {url_index}] ДАННЫЕ СОБРАНЫ ПО: {url}")
        except Exception as e:
            self.ERRORS.setdefault('get_list_car_brands_url', {}).setdefault(f"{url}", e.args)
            self.ERRORS_URLS.add(url)
            print(f"\t[ERROR id {url_index}] ОШИБКА! ")

    async def get_tasks_attrs_groups(self, chunk_data: list[list]) -> None:
        """
        Полчает на вход чанк(итерируемый объект) по его ссылкам формирует таски используя метод get_list_attr_groups_url
        """
        self.TASKS.clear()
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(180), trust_env=True) as session:
            for url_index, car_data in enumerate(chunk_data, 1):
                car_brand = car_data[0]
                car_model = car_data[1]
                url = car_data[2]
                self.TASKS.append(
                    asyncio.create_task(self.get_list_attr_groups_url(session, url, url_index, car_brand, car_model))
                )
                # break  # TODO TEST на одной изначальной ссылке КОММЕНТИТЬ!!!
            await asyncio.gather(*self.TASKS)

    def run_attrs_groups_tasks(self) -> None:
        """
        метод запускающий на выполнение сформированные задачи в get_tasks_attrs_groups
        Так же сохраняет результаты собранных данных и ошибок в отдельные файлы.
        В конце очищает атрибуты класса где хранятся ошибки (ERRORS, ERRORS_URLS)
        """
        if not self._check_dirs(f"{self.DEFAULT_URL_PATH}/main_urls.json", True):
            print(f"{'=' * 50}\nНачат сбор урлов всех моделей авто:\n{'=' * 50}")
            type(self).ALL_CAR_URL_LIST.extend(
                self.get_main_urls(self.BASE_URLS_CATEGORIES)
            )
            self._write_to_json(self.DEFAULT_URL_PATH, 'main_urls.json',
                                data=type(self).ALL_CAR_URL_LIST)

        chunks = self.get_chunks(type(self).ALL_CAR_URL_LIST,
                                 200)  # объект генератор, прочитать можно 1 раз, после данных в нем не будет
        # len_chunks = len(chunks)
        for chunk_id, chunk_data in enumerate(chunks):
            print('-' * 100)
            print(f'{"\t" * 10} Chunk id: #{chunk_id}')
            print('-' * 100)
            asyncio.run(
                self.get_tasks_attrs_groups(chunk_data)
            )

            self._write_to_json(self.DEFAULT_URL_PATH, 'urls_with_attrs_groups.json', type(self).URLS_WITH_ATTRS_GROUPS,
                                isadd=True)
            self._write_to_json(f"{self.DEFAULT_URL_PATH_ERRORS}/{self._get_datetime(True)}",
                                f'ERRORS_attrs_groups.json',
                                self.ERRORS, isadd=True)
            self._write_to_file(f"{self.DEFAULT_URL_PATH_ERRORS}/{self._get_datetime(True)}",
                                f'ERRORS_URLS_attrs_groups.txt', self.ERRORS_URLS, workmode='a')
            type(self).URLS_WITH_ATTRS_GROUPS.clear()
            # if chunk_id == 0:  # TODO TEST ограничение на количество обрабатываемых чанков при получении ссылок на модели авто
            #     break
        self.ERRORS.clear()
        self.ERRORS_URLS.clear()

    async def get_all_goods_from_page(self,
                                      session: aiohttp.ClientSession,
                                      url: str,
                                      url_index: int,
                                      car_brand: str,
                                      car_model: str,
                                      group: str,
                                      chapter: str,

                                      ) -> None:
        """
        Метод получающий все позиции с загруженной странице по адресу url.
            после поиска  url'ов товаров на странице они доабвляются в атрибут класса ALL_GOODS_URLS
        session: aiohttp.ClientSession
        url: url - загружаемая ссылка
        url_index: int индекс url в пределах chunk'a
        group: str - название группы запчастей
        chapter: str - название раздела запчастей

        """
        await self.get_delay(2, 3)
        self.URL_COUNTER += 1
        PREVIOUS_ACTIVE_PAGE = ''  # переменная только для этой функции get_all_goods_from_page()
        print(
            f"[INSTANCE id {self.OBJ_ID}][URL COUNTER #{self.URL_COUNTER}][ INFO id {url_index}] Сбор данных по {url}")
        start = True
        while start:
            await self.get_delay(2, 3)
            try:
                async with session.get(url, headers=self._get_header(), cookies=self._get_cookies()) as response:
                    soup = self.get_soup(await response.read())
                    if PREVIOUS_ACTIVE_PAGE == self._get_active_page(soup):
                        start = False
                        # print(f"ID {url_index}", PREVIOUS_ACTIVE_PAGE, url)
                        continue
                    for row_index, row in enumerate(
                            soup.find('div', class_='list-wrapper').find_all('div', class_='add-image'), 1):
                        self.ALL_GOODS_URLS.append(
                            [
                                car_brand,
                                car_model,
                                group,
                                chapter,
                                self.BASE_URL + row.find('a').get('href')
                            ]
                        )

                    paginagions = self._check_pagination(soup)

                    # print('\t\t\t\t', paginagions)
                    if paginagions != '1 страница':
                        url = self.BASE_URL + paginagions
                        PREVIOUS_ACTIVE_PAGE = self._get_active_page(soup)
                    else:
                        PREVIOUS_ACTIVE_PAGE = self._get_active_page(soup)
                        start = False

                    print(
                        f"\t[INSTANCE id {self.OBJ_ID}][URL COUNTER #{self.URL_COUNTER}][SUCCESS url id {url_index}][PAGE: {PREVIOUS_ACTIVE_PAGE}] ДАННЫЕ СОБРАНЫ ПО: {url}")

            except Exception as e:
                self.ERRORS.setdefault('get_all_goods_from_page', {}).setdefault(f"{url}", e.args)
                self.ERRORS_URLS.add(url)
                print(
                    f"\t[INSTANCE id {self.OBJ_ID}][URL COUNTER #{self.URL_COUNTER}][ERROR url id {url_index}] ОШИБКА! {e.args}")  # TODO ERRORS

    async def get_tasks_car_goods(self, chunk_data: list | tuple) -> None:
        """
        метод для формирования задач по получению ссылок на товары.

        chunk: итерируемый объект в котором модели авто
        group: str - название группы запчастей
        chapter: str - название раздела запчастей

        """
        self.TASKS.clear()
        print(f'[{self.OBJ_ID}][INFO] Формирование задач для начала сбора url товаров...')
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(180), trust_env=True) as session:
            for url_index, data in enumerate(chunk_data, 1):
                car_brand = data[0]
                car_model = data[1]
                group = data[2]
                chapter = data[3]
                url = data[4]
                self.TASKS.append(
                    asyncio.create_task(
                        self.get_all_goods_from_page(session, url, url_index, car_brand, car_model, group, chapter)
                    )
                )

            await asyncio.gather(*self.TASKS)

    def run_car_item_tasks(self) -> None:
        """
        Запускает задачи на выполнение по сбору ссылок на товары. Так же если скрипт тестируется,
        то предусмотрена загрузка данных в атрибут класса URLS_WITH_ATTRS_GROUPS из файла 'data/urls/urls_with_attrs_groups.json'

        так же сохраняет собранные данные и ошибки в отдельные файлы. Сохранение происходит по чанкам. Последующие
        данные других чанков записываются в конец файла созданного при обработке первого чанка
        """

        if not self.URLS_WITH_ATTRS_GROUPS:
            self.URLS_WITH_ATTRS_GROUPS = self._read_file('data/urls/urls_with_attrs_groups.json', isjson=True)

        chunks = self.get_chunks(self.URLS_WITH_ATTRS_GROUPS,
                                 chunk_length=200)  # TODO объект генератор, прочитать можно 1 раз, после данных в нем не будет
        # len_chunks = len(chunks)
        start_chunk = time.monotonic()

        for chunk_id, chunk_data in enumerate(chunks):
            print('-' * 100)
            print(f'{"\t" * 10}[{self.OBJ_ID}] Chunk id: #{chunk_id}')
            print('-' * 100)

            asyncio.run(
                self.get_tasks_car_goods(chunk_data)
            )
            try:
                self._write_to_json(f"{self.DEFAULT_URL_PATH_ERRORS}/{self._get_datetime(True)}",
                                    f'ERRORS_goods_urls.json', self.ERRORS, isadd=True)
            except Exception as e:
                print("______ERRRRRRRRORRRRR________", e)

            self._write_to_file(f"{self.DEFAULT_URL_PATH_ERRORS}/{self._get_datetime(True)}",
                                f'ERRORS_URLS_goods_urls.txt', self.ERRORS_URLS,
                                workmode='a')
            self.ERRORS.clear()
            self.ERRORS_URLS.clear()
            # if chunk_id == 0:  # TODO TEST ограничение на количество обрабатываемых чанков при получении ссылок на сами объявдения
            #     break

            if not chunk_id % 10:
                end_chunk = time.monotonic()
                self._write_to_json(f"{self.DEFAULT_URL_PATH_ALL_GOODS_URLS}/{self.OBJ_ID}",
                                    f'[chunk id {chunk_id}] all_goods_urls.json',
                                    self.ALL_GOODS_URLS)
                self._write_to_file(
                    self.DEFAULT_URL_PATH,
                    'timing.txt',
                    (
                        f"\tВремя работы скрипта получение списка [последний chunk id {chunk_id}]({(chunk_id + 1) * 150}/{self._get_length_iterable(self.URLS_WITH_ATTRS_GROUPS)}): {end_chunk - start_chunk} секунд.",
                    ),
                    workmode='a'
                )
                self.ALL_GOODS_URLS.clear()

        if self.ALL_GOODS_URLS:
            print("____SUPER____", self.OBJ_ID)

            end_chunk = time.monotonic()
            self._write_to_json(f"{self.DEFAULT_URL_PATH_ALL_GOODS_URLS}/{self.OBJ_ID}",
                                f'[chunk id {chunk_id}] all_goods_urls.json',
                                self.ALL_GOODS_URLS)
            self._write_to_file(self.DEFAULT_URL_PATH, 'timing.txt', (
                f"\tВремя работы скрипта получение списка [последний chunk id {chunk_id}]({(chunk_id + 1) * 150}/{self._get_length_iterable(self.URLS_WITH_ATTRS_GROUPS)}): {end_chunk - start_chunk} секунд.",),
                                workmode='a')
            self.ALL_GOODS_URLS.clear()

        self.URL_COUNTER = 0
        self.URLS_WITH_ATTRS_GROUPS.clear()

    def get_data(self,
                 soup: BeautifulSoup,
                 url: str,
                 car_brand: str,
                 car_model: str,
                 group: str,
                 chapter: str) -> dict:
        """
        Метод получения данных со страницы самого товара

        soup: BeautifulSoup - объект из которого берутся данные
        url: url - использовался только при тестах для добавления в итоговый результат
        group: str - название группы запчастей
        chapter: str - название раздела запчастей

        return: возвращает словарь с данными вида:
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
                'Ссылки на фото',
            }

        """
        result = {}

        image_urls = []
        data_of_image_urls = soup.find('div', class_='detail-image').find_all('img')
        for url in data_of_image_urls:
            image_urls.append(
                self.BASE_URL + url['src'])

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
                'Производитель': car_brand,
                'Модель': car_model,
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
                'Ссылки на фото': ','.join(image_urls) if image_urls else 'Изображение не найдены',
            }
        )
        return result

    async def get_data_from_page(self,
                                 session: aiohttp.ClientSession,
                                 url: str,
                                 url_index: int,
                                 car_brand: str,
                                 car_model: str,
                                 group: str,
                                 chapter: str,

                                 ) -> None:
        """
        Метод получающий объект BeautifulSoup по указанному url, запускающий сбор данных со страницы
        После записывающий в атрибут класса DATA_FOR_CSV.

        session: объект сессии aiohttp.ClientSession
        url: url для поиска
        url_index: int индекс url в пределах chunk'a
        group: str - название группы запчастей
        chapter: str - название раздела запчастей
        return: None
        """
        await self.get_delay(1, 3)
        self.URL_COUNTER += 1
        print(
            f"[INSTANCE id {self.OBJ_ID}][URL COUNTER #{self.URL_COUNTER}][ INFO id {url_index}] Сбор данных по {url}")
        try:
            async with session.get(url, headers=self._get_header(), cookies=self._get_cookies()) as response:
                soup = self.get_soup(await response.read())
                if (a := soup.find('div', class_='row block404')):
                    raise ValueError(f'Error 404 - {a.text}')
                self.DATA_FOR_CSV.append(
                    self.get_data(soup, url, car_brand, car_model, group, chapter)
                )
                print(
                    f"\t[INSTANCE id {self.OBJ_ID}][URL COUNTER #{self.URL_COUNTER}][SUCCESS url id {url_index}] ДАННЫЕ СОБРАНЫ ПО: {url}")
        except Exception as e:
            self.ERRORS.setdefault('get_data_from_page', {}).setdefault(f"{url_index}. {url}", e.args)
            self.ERRORS_URLS.add(url)
            print(
                f"\t[INSTANCE id {self.OBJ_ID}][URL COUNTER #{self.URL_COUNTER}][ERROR id {url_index}] ОШИБКА! {e.args}")

    async def get_tasks_car_items(self, chunk_data: list | tuple) -> None:
        """
            Формирование задач по сбору данных о товарах

            chunk_urls: итерируемый объект в котором содержатся  url для поиска данных о товарах
            group: str - название группы запчастей
            chapter: str - название раздела запчастей

            return: None

        """
        self.TASKS.clear()
        print(f'[instance {self.OBJ_ID}][INFO] Формирование задач для начала сбора данных о товарах...')
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(180), trust_env=True) as session:
            for url_index, data in enumerate(chunk_data, 1):
                car_brand = data[0]
                car_model = data[1]
                group = data[2]
                chapter = data[3]
                url = data[4]
                self.TASKS.append(
                    asyncio.create_task(
                        self.get_data_from_page(session, url, url_index, car_brand, car_model, group, chapter)
                    )
                )

            await asyncio.gather(*self.TASKS)

    def run_get_data_from_page_tasks(self) -> None:
        """
            Запуск задач сформированных методом get_tasks_car_items
            После сохраниение собранных данных из атрибута класса DATA_FOR_CSV в csv, а ошибок в отдельный файл
        """

        # time.sleep(5)

        list_of_files = os.listdir(f"{self.DEFAULT_URL_PATH_ALL_GOODS_URLS}/{self.OBJ_ID}")
        for filename in list_of_files:

            self.ALL_GOODS_URLS = self._read_file(f'{self.DEFAULT_URL_PATH_ALL_GOODS_URLS}/{self.OBJ_ID}/{filename}',
                                                  isjson=True)

            start_chunk = time.monotonic()
            chunks = self.get_chunks(self.ALL_GOODS_URLS, 300)
            for chunk_id, chunk_data in enumerate(chunks):
                print('-' * 100)
                print(f'{"\t" * 10}[{self.OBJ_ID}] Chunk #{chunk_id}')
                print('-' * 100)
                asyncio.run(
                    self.get_tasks_car_items(chunk_data)
                )
                try:
                    self._write_to_json(f"{self.DEFAULT_URL_PATH_ERRORS}/{self._get_datetime(True)}",
                                        f'ERRORS_data_items.json', self.ERRORS, isadd=True)
                except Exception as e:
                    print("____ERRRRRRORRR_____", e)
                self._write_to_file(f"{self.DEFAULT_URL_PATH_ERRORS}/{self._get_datetime(True)}",
                                    f'ERRORS_URLS_data_items.txt', self.ERRORS_URLS,
                                    workmode='a')
                if not chunk_id % 10:
                    end_chunk = time.monotonic()
                    self._write_to_csv(f"{self.DEFAULT_URL_PATH_CSV}/csv",
                                       f'[inst_id {self.OBJ_ID}][chunk_id {chunk_id}]_RESULT.csv', self.DATA_FOR_CSV)
                    self._write_to_json(f"{self.DEFAULT_URL_PATH_CSV}/res_json",
                                        f'[inst_id {self.OBJ_ID}][chunk_id {chunk_id}]_all_data_items.json',
                                        self.DATA_FOR_CSV)
                    self._write_to_file(self.DEFAULT_URL_PATH, 'timing.txt', (
                        f"\tВремя работы скрипта получение данных по товарам [последний chunk id {chunk_id}]({(chunk_id + 1) * 300}/{self._get_length_iterable(self.URLS_WITH_ATTRS_GROUPS) if chunk_id != 0 else (chunk_id + 1) * 300}): {end_chunk - start_chunk} секунд.",),
                                        workmode='a')
                    self.DATA_FOR_CSV.clear()

                # if chunk_id == 0:  # TODO TEST ограничение на количество обрабатываемых чанков при получении данных о товаре
                #     break
            if self.DATA_FOR_CSV:
                end_chunk = time.monotonic()

                self._write_to_csv(f"{self.DEFAULT_URL_PATH_CSV}/csv",
                                   f'[inst_id {self.OBJ_ID}][chunk_id {chunk_id}]_RESULT.csv', self.DATA_FOR_CSV)
                self._write_to_json(f"{self.DEFAULT_URL_PATH_CSV}/res_json",
                                    f'[inst_id {self.OBJ_ID}][chunk_id {chunk_id}]_all_data_items.json',
                                    self.DATA_FOR_CSV)
                self._write_to_file(self.DEFAULT_URL_PATH, 'timing.txt', (
                    f"\tВремя работы скрипта получение данных по товарам [последний chunk id {chunk_id}]({(chunk_id + 1) * 300}/{self._get_length_iterable(self.URLS_WITH_ATTRS_GROUPS) if chunk_id != 0 else (chunk_id + 1) * 300}): {end_chunk - start_chunk} секунд.",),
                                    workmode='a')
                self.DATA_FOR_CSV.clear()

            self.ERRORS_URLS.clear()
            self.ERRORS.clear()
        type(self).URL_COUNTER = 0

    def run_first_task(self) -> None:
        # Эта часть ищет все ссылки брендов на каждую группу товара.
        start = time.monotonic()
        self.run_attrs_groups_tasks()
        end = time.monotonic()
        print(
            f"Время работы скрипта получение списка ({self._get_length_iterable(self.URLS_WITH_ATTRS_GROUPS)}): {end - start} секунд. \n{'=' * 50}")

        print()
        self._write_to_file(self.DEFAULT_URL_PATH, 'timing.txt', (
            f"Время работы скрипта получение списка ({self._get_length_iterable(self.URLS_WITH_ATTRS_GROUPS)}): {end - start} секунд.",),
                            workmode='a')

    async def run_all_tasks(self) -> None:
        """
            Метод изначально удаляет старые файлы с данными(кроме файлов ошибок)
            После поочередно запускает методы:
                1 run_attrs_groups_tasks
                2 run_car_item_tasks
                3 run_get_data_from_page_tasks
        """
        # Эта часть ищет ссылки на сами товары.
        start = time.monotonic()
        self.run_car_item_tasks()
        end = time.monotonic()
        print(
            f"[{self.OBJ_ID}] Время работы скрипта получение списка ссылок на товары({self._get_length_iterable(self.ALL_GOODS_URLS)}): {end - start} секунд. \n{'=' * 50}")

        print()
        self._write_to_file(self.DEFAULT_URL_PATH, 'timing.txt', (
            f"[{self.OBJ_ID}]Время работы скрипта получение списка ссылок на товары({self._get_length_iterable(self.ALL_GOODS_URLS)}): {end - start} секунд.",),
                            workmode='a')

        # Эта часть ищет данные по списку ссылок и затем сохраняет в csv
        start = time.monotonic()
        self.run_get_data_from_page_tasks()
        end = time.monotonic()
        print(
            f"[{self.OBJ_ID}] Время работы скрипта получение данных о товарах({self._get_length_iterable(self.ALL_GOODS_URLS)}): {end - start} секунд. \n{'=' * 50}")
        self._write_to_file(self.DEFAULT_URL_PATH, 'timing.txt', (
            f"[{self.OBJ_ID}]Время работы скрипта получение данных о товарах({self._get_length_iterable(self.ALL_GOODS_URLS)}): {end - start} секунд.",),
                            workmode='a')


class MultiplyParser(ParserBamperBy):
    '''
    Класс создающий несколько экземпляров ParserBamperBy и запускает их асинхронно
    '''
    PARSER_INSTANCE = []
    TASKS = []

    def create_parser_instance(self, obj: ParserBamperBy):
        '''
        Получение ссылок по которым будет поиск товаров
        Создание экземпляров парсера
        '''

        # Удаление 2 основных файлов для сбора ссылок
        if self._check_dirs(f"{self.DEFAULT_URL_PATH}/main_urls.json", check_file=True):
            os.remove(f"{self.DEFAULT_URL_PATH}/main_urls.json")

        if self._check_dirs(f"{self.DEFAULT_URL_PATH}/urls_with_attrs_groups.json", check_file=True):
            os.remove(f"{self.DEFAULT_URL_PATH}/urls_with_attrs_groups.json")
        ################################################
        try:
            self.run_attrs_groups_tasks()  # Наполняет type(self).URLS_WITH_ATTRS_GROUPS атрибут класса!
        except Exception as e:
            print("ERRORS_line_965", e)

        if not type(self).URLS_WITH_ATTRS_GROUPS:
            type(self).URLS_WITH_ATTRS_GROUPS = self._read_file('data/urls/urls_with_attrs_groups.json', isjson=True)

        if type(self).URLS_WITH_ATTRS_GROUPS:
            chunks = self.get_chunks(type(self).URLS_WITH_ATTRS_GROUPS,
                                 len(type(self).URLS_WITH_ATTRS_GROUPS) // 3)  # TODO self.URLS_WITH_ATTRS_GROUPS to type(self).URLS_WITH_ATTRS_GROUPS

            for chunk in chunks:
                instance = obj()
                instance.URLS_WITH_ATTRS_GROUPS = chunk
                self.PARSER_INSTANCE.append(instance)
        else:
            raise ValueError("Файл 'data/urls/urls_with_attrs_groups.json' - не найден! \nСкрипт остановлен!")
    async def get_tasks(self):

        # nest_asyncio.apply()
        for obj_id, obj in enumerate(self.PARSER_INSTANCE, 1):

            # Удаление файлов с результатами работы прошлых запусков.
            obj.OBJ_ID = obj_id

            if os.path.exists(f"{self.DEFAULT_URL_PATH_ALL_GOODS_URLS}/{obj_id}"):
                files = os.listdir(f"{self.DEFAULT_URL_PATH_ALL_GOODS_URLS}/{obj_id}")
                for file in files:
                    os.remove(f"{self.DEFAULT_URL_PATH_ALL_GOODS_URLS}/{obj_id}/{file}")

            if os.path.exists(f"{self.DEFAULT_URL_PATH_CSV}/csv/"):
                files = os.listdir(f"{self.DEFAULT_URL_PATH_CSV}/csv/")
                for file in files:
                    os.remove(f"{self.DEFAULT_URL_PATH_CSV}/csv/{file}")

            if os.path.exists(f"{self.DEFAULT_URL_PATH_CSV}/res_json/"):
                files = os.listdir(f"{self.DEFAULT_URL_PATH_CSV}/res_json/")
                for file in files:
                    os.remove(f"{self.DEFAULT_URL_PATH_CSV}/res_json/{file}")

            ############################################################################
            self.TASKS.append(
                asyncio.create_task(
                    obj.run_all_tasks()
                )
            )
        await asyncio.gather(*self.TASKS)

    def run_tasks(self):
        asyncio.run(self.get_tasks())


if __name__ == '__main__':
    # TODO continue рассмотреть возможность сделать сохранение оставшихся чанков, на случай, если парсер словит исключение
    #     которое не обработано
    # TODO continue в каталоге continues после того как файл будет пустым рассмотреть необходимость его удаления
    parser = MultiplyParser()
    try:
        parser.create_parser_instance(ParserBamperBy)
    except Exception as e:
        # print(e)
        print()
    parser.run_tasks()
