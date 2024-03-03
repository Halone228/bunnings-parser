from functools import reduce, lru_cache

import loguru
from botasaurus import request, AntiDetectRequests, browser, AntiDetectDriver
from botasaurus.create_stealth_driver import create_stealth_driver
from json import load, loads

from uuid import uuid4

from botasaurus.user_agent import UserAgent
from jwt import decode
from typing import TypedDict
from pandas import DataFrame
from icecream import ic
from requests.cookies import RequestsCookieJar


def main_start():
    def chunks(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]


    class ParseData(TypedDict):
        headers: dict
        group: str
        user_id: str


    class ProductGetData(TypedDict):
        headers: dict
        payload: dict


    def generate_headers(token: str, session_id: str):
        decoded_token = decode(token, options={"verify_signature": False})
        user_id = decoded_token['sub']
        return {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Authorization': f'Bearer {token}',
            "Connection": 'keep-alive',
            "Content-Length": '5874',
            "Content-Type": 'application/json',
            "Host": 'api.prod.bunnings.com.au',
            "Origin": 'https://www.bunnings.com.au',
            "Referer": 'https://www.bunnings.com.au/',
            "Sec-Fetch-Dest": 'empty',
            "Sec-Fetch-Mode": 'cors',
            "Sec-Fetch-Site": 'same-site',
            "User-Agent": 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/120.0.6099.940 YaBrowser/24.1.1.940 Yowser/2.5 Safari/537.36',
            "X-region": 'VICMetro',
            "clientId": 'mHPVWnzuBkrW7rmt56XGwKkb5Gp9BJMk',
            "correlationid": f'{user_id}',
            "country": 'AU',
            "currency": 'AUD',
            "locale": 'en_AU',
            "locationCode": '6400',
            "sec-ch-ua": '1',
            "sec-ch-ua-mobile": '?0',
            "sec-ch-ua-platform": '"Linux"',
            "sessionid": f'{session_id}',
            "stream": 'RETAIL',
            "userId": 'anonymous'
        }


    @browser(
        create_driver=create_stealth_driver('https://www.bunnings.com.au/products'),
        profile='base',
        headless=True
    )
    def get_page(driver: AntiDetectDriver, data):
        main_links = driver.links('.article-card>a')
        links = []
        for i in main_links:
            driver.get(i)
            links.extend(driver.links('.article-card>a'))
        links = list(
            map(
                lambda link: link.split('/')[-1],
                filter(
                    lambda v: 'products' in v,
                    links
                )
            )
        )
        raw_cookie = driver.get_cookie('guest-token-storage')
        cookie = loads(raw_cookie['value'])
        token = cookie['token']

        return links, generate_headers(token, driver.get_cookie('personalization_session_id')['value']), driver.get_cookies()


    with open('test.json', encoding='utf-8') as f:
        copy_payload = load(f)


    @lru_cache(maxsize=-1)
    def get_mako():
        with open('payload.mako', 'r', encoding='utf-8') as file:
            return file.read()


    @lru_cache(maxsize=-1)
    def generate_json_from_mako(user_id, group) -> dict:
        mako = get_mako()
        return loads(mako.format_map({
            'group': group,
            'user_id': user_id
        }))


    # def generate_payload(headers):
    #     payload = copy_payload.copy()
    #     for i in range(2):
    #         payload['firstResult'] = i*int(payload['numberOfResults'])
    #         yield {
    #             'json_payload': payload,
    #             'headers': headers
    #         }


    def get_product(client: AntiDetectRequests, data: ProductGetData):
        response = client.post(
            'https://api.prod.bunnings.com.au/v1/facets/category',
            json=data['payload'],
            headers=data['headers'],
            timeout=100000000
        )
        if response.status_code != 200:
            return None
        return response.json()


    dick = dict


    def generate_api_endpoints(group_count: dick, _headers):
        user_id = _headers['correlationid']
        num_res = 1000
        for group in group_count:
            _g = group[0]
            _c = group[1]
            def_payload = generate_json_from_mako(user_id, _g)
            def_payload['numberOfResults'] = f"{num_res}"
            tries = _c//num_res
            for i in range(0, tries):
                return_payload = def_payload.copy()
                return_payload['firstResult'] = num_res*i
                yield {
                    'payload': return_payload,
                    'headers': _headers
                }
            def_payload['numberOfResults'] = f"{(_c - tries*num_res)}"
            def_payload['firstResult'] = tries*num_res
            yield {
                'payload': def_payload,
                'headers': _headers
            }


    @request(
        parallel=4
    )
    def get_group_count(client: AntiDetectRequests, data: ParseData):
        payload = generate_json_from_mako(data['user_id'], data['group'])
        payload['numberOfResults'] = "0"
        result = get_product(client, {
            'headers': data['headers'],
            'payload': payload
        })
        return [
            data['group'], result['data']['totalCount']
        ]


    def get_start_url(data):
        return data


    @browser(
        reuse_driver=True,
        create_driver=create_stealth_driver(start_url=get_start_url, wait=15),
        parallel=10,
        headless=True,
        block_images=True,
        block_resources=True,
        cache=True,
        user_agent=UserAgent.user_agent_106
    )
    def get_product_page(client: AntiDetectDriver, data: str):
        soup = client.bs4()
        images = [i.get('src') for i in soup.select('img.productImageLarge')]
        breadcrumbs = [i.text.strip() for i in soup.select('nav[aria-label=Breadcrumb] > ul > li')][3:5]
        description = soup.select_one('[data-locator=features_list]').parent.text.strip()
        return {
            'url': data,
            'description': description,
            'breadcrumbs': '->'.join(breadcrumbs),
            'images': '\n'.join(images)
        }


    def _reduce(a, v):
        a.extend(v)
        return a


    def return_valid_item(item: dict):
        return {
            'article': item.get('itemnumber') or item.get('code') or item.get('permanentid'),
            'url': 'https://www.bunnings.com.au'+item['productroutingurl'],
            'price': item['price_6400'],
            'breadcrumbs': "->".join(item['supercategoriescode'][:2]),
            'name': item['title'],
            'count': item['productcount'],
            'images': item.get('thumbnailimageurl', ''),
            'description': "\n".join(item.get('keysellingpoints', ['']))
        }


    @request(
        parallel=8
    )
    def get_info(client: AntiDetectRequests, data: ProductGetData):
        result = get_product(client, data)
        if not result:
            return []
        return list(map(return_valid_item, map(lambda item: item['raw'], result['data']['results'])))


    @request(
        parallel=20,
    )
    def get_count(client: AntiDetectRequests, data):
        try:
            response_stock = client.post(
                'https://api.prod.bunnings.com.au/v1/stores/products/stock',
                headers=data['headers'],
                json={'products': data['products']},
                params={
                    'pageSize': 8000
                },
                timeout=8000000
            )
        except Exception as e:
            loguru.logger.exception(e)
            return {}
        from collections import defaultdict
        code_data = defaultdict(int)
        stock_data = response_stock.json()
        if 'data' not in stock_data:
            print(data['products'])
            print(stock_data)
            return {}
        stores = stock_data['data']['stores']

        def zero(v):
            return v if v is not None else 0

        for store in stores:
            for prod in store['products']:
                code_data[prod['code']] = code_data[prod['code']] + zero(prod['stock'].get('stockLevel', 0))
        return code_data


    links, headers, cookies = get_page()
    counts = get_group_count(
        [{
            'user_id': headers['correlationid'],
            'headers': headers,
            'group': i
        } for i in links]
    )

    end_data = reduce(_reduce, get_info(list(generate_api_endpoints(counts, headers))), [])
    print(len(end_data))
    df = DataFrame.from_records(
        end_data
    )
    df.set_index('article', inplace=True)
    counts_process = get_count([{
        'headers': headers,
        'products': data
    } for data in chunks(df.index.to_list(), 3)])
    products_pages_process = get_product_page(df['url'].to_list())

    counts = reduce(lambda x, y: x | y, counts_process, {})
    for k, v in counts.items():
        df.loc[k, 'count'] = v

    result = reduce(lambda x, y: x | {y.pop('url'): y}, products_pages_process, {})
    url_id_dct = {data['url']: idx for idx, data in df.iterrows()}
    for url, data in result.items():
        df.loc[url_id_dct[url], ['description', 'images', 'breadcrumbs']] = (data['description'],
                                                                              data['images'], data['breadcrumbs'])

    df.to_excel(f'results/result{uuid4().hex[:15]}.xlsx', sheet_name='result')
