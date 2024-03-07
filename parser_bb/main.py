from functools import lru_cache
import loguru
from botasaurus import request, AntiDetectRequests, browser, AntiDetectDriver
from botasaurus.cache import DontCache
from botasaurus.create_stealth_driver import create_stealth_driver
from json import loads
from os import getenv
from sqlalchemy import select, update

from .database import (
    insert_data,
    update_stock,
    get_products_count,
    get_all_articles,
    get_all_urls,
    ProductModel,
    engine,
    set_stock_parsed,
    update_data,
    compress_full_info_parsed,
    compress_stock_parsed,
    set_full_info_parsed,
    sesmaker,
)

from botasaurus.user_agent import UserAgent
from jwt import decode
from typing import TypedDict
from pandas import DataFrame, read_sql
from tqdm import tqdm
from botasaurus import calc_max_parallel_browsers


def main_start():
    def chunks(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    class ParseData(TypedDict):
        headers: dict
        group: str
        user_id: str

    class ProductGetData(TypedDict):
        headers: dict
        payload: dict

    def generate_headers(token: str, session_id: str):
        decoded_token = decode(token, options={"verify_signature": False})
        user_id = decoded_token["sub"]
        return {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br",
            "Authorization": f"Bearer {token}",
            "Connection": "keep-alive",
            "Content-Length": "5874",
            "Content-Type": "application/json",
            "Host": "api.prod.bunnings.com.au",
            "Origin": "https://www.bunnings.com.au",
            "Referer": "https://www.bunnings.com.au/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.6099.940 YaBrowser/24.1.1.940 Yowser/2.5 Safari/537.36",
            "X-region": "VICMetro",
            "clientId": "mHPVWnzuBkrW7rmt56XGwKkb5Gp9BJMk",
            "correlationid": f"{user_id}",
            "country": "AU",
            "currency": "AUD",
            "locale": "en_AU",
            "locationCode": "6400",
            "sec-ch-ua": "1",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Linux"',
            "sessionid": f"{session_id}",
            "stream": "RETAIL",
            "userId": "anonymous",
        }

    @browser(
        create_driver=create_stealth_driver(
            "https://www.bunnings.com.au/products", wait=15
        ),
        user_agent=UserAgent.user_agent_106,
        max_retry=5,
        retry_wait=4,
        reuse_driver=True,
    )
    def get_page(driver: AntiDetectDriver, data):
        main_links = driver.links(".article-card>a")
        links = []
        for i in main_links:
            driver.get(i)
            links.extend(driver.links(".article-card>a"))
        links = list(
            map(
                lambda link: link.split("/")[-1],
                filter(lambda v: "products" in v, links),
            )
        )
        raw_cookie = driver.get_cookie("guest-token-storage")
        cookie = loads(raw_cookie["value"])
        token = cookie["token"]

        return (
            links,
            generate_headers(
                token, driver.get_cookie("personalization_session_id")["value"]
            ),
            driver.get_cookies(),
        )

    @lru_cache(maxsize=-1)
    def get_mako():
        with open("payload.mako", "r", encoding="utf-8") as file:
            return file.read()

    @lru_cache(maxsize=-1)
    def generate_json_from_mako(user_id, group) -> dict:
        mako = get_mako()
        return loads(mako.format_map({"group": group, "user_id": user_id}))

    def get_product(client: AntiDetectRequests, data: ProductGetData):
        response = client.post(
            "https://api.prod.bunnings.com.au/v1/facets/category",
            json=data["payload"],
            headers=data["headers"],
            timeout=100000000,
        )
        if response.status_code != 200:
            return None
        return response.json()

    def generate_api_endpoints(group_count: dict, _headers):
        user_id = _headers["correlationid"]
        num_res = 1000
        for group in group_count:
            _g = group[0]
            _c = group[1]
            def_payload = generate_json_from_mako(user_id, _g)
            def_payload["numberOfResults"] = f"{num_res}"
            tries = _c // num_res
            for i in range(0, tries):
                return_payload = def_payload.copy()
                return_payload["firstResult"] = num_res * i
                yield {"payload": return_payload, "headers": _headers}
            def_payload["numberOfResults"] = f"{(_c - tries*num_res)}"
            def_payload["firstResult"] = tries * num_res
            yield {"payload": def_payload, "headers": _headers}

    @request(parallel=2)
    def get_group_count(client: AntiDetectRequests, data: ParseData):
        payload = generate_json_from_mako(data["user_id"], data["group"])
        payload["numberOfResults"] = "0"
        result = get_product(client, {"headers": data["headers"], "payload": payload})
        return [data["group"], result["data"]["totalCount"]]

    def get_start_url(data):
        return data

    @browser(
        reuse_driver=True,
        create_driver=create_stealth_driver(start_url=get_start_url, wait=15),
        parallel=calc_max_parallel_browsers.calc_max_parallel_browsers(
            max=int(getenv("MAX_BROWSERS", 7))
        ),
        headless=True,
        block_images=True,
        block_resources=True,
        cache=True,
        user_agent=UserAgent.user_agent_106,
    )
    def get_product_page(client: AntiDetectDriver, data: str):
        nonlocal full_info
        full_info.update()
        soup = client.bs4()
        images = [i.get("src") for i in soup.select("img.productImageLarge")]
        breadcrumbs = [
            i.text.strip() for i in soup.select("nav[aria-label=Breadcrumb] > ul > li")
        ][3:5]
        try:
            description = soup.select_one(
                "[data-locator=features_list]"
            ).parent.text.strip()
        except:
            description = ""
        _st = {
            "url": data,
            "description": description,
            "breadcrumbs": "->".join(breadcrumbs),
            "images": "\n".join(images),
        }
        set_full_info_parsed([_st])
        update_data([_st])

    def _reduce(a, v):
        a.extend(v)
        return a

    def return_valid_item(item: dict):
        return {
            "article": item.get("itemnumber")
            or item.get("code")
            or item.get("permanentid"),
            "url": "https://www.bunnings.com.au" + item["productroutingurl"],
            "price": item["price_6400"],
            "breadcrumbs": "->".join(item["supercategoriescode"][:2]),
            "name": item["title"],
            "count": item["productcount"],
            "images": item.get("thumbnailimageurl", ""),
            "description": "\n".join(item.get("keysellingpoints", [""])),
        }

    @request(parallel=20)
    def get_info(client: AntiDetectRequests, data: ProductGetData):
        result = get_product(client, data)
        if not result:
            return []
        nonlocal first_level_data
        first_level_data.update(len(result["data"]["results"]))
        data = list(
            map(
                return_valid_item,
                map(lambda item: item["raw"], result["data"]["results"]),
            )
        )
        insert_data(data) if data else None
        # with sesmaker() as session:
        #     session.execute(
        #         update(ProductModel),
        #         [{'article': i['article'], 'name': i['name']} for i in data]
        #     )
        #     session.commit()

    @request(
        parallel=15,
    )
    def get_count(client: AntiDetectRequests, data):
        nonlocal stock_data_loader
        try:
            response_stock = client.post(
                "https://api.prod.bunnings.com.au/v1/stores/products/stock",
                headers=data["headers"],
                json={"products": data["products"]},
                params={"pageSize": 8000},
                timeout=8000000,
            )
            stock_data_loader.update(len(data["products"]))
        except Exception as e:
            loguru.logger.exception(e)
            stock_data_loader.update(len(data["products"]))
            return {}
        from collections import defaultdict

        code_data = defaultdict(int)
        stock_data = response_stock.json()
        if "data" not in stock_data:
            print(data["products"])
            print(stock_data)
            return {}
        stores = stock_data["data"]["stores"]

        def zero(v):
            return v if v is not None else 0

        for store in stores:
            for prod in store["products"]:
                code_data[prod["code"]] = code_data[prod["code"]] + zero(
                    prod["stock"].get("stockLevel", 0)
                )
        update_stock([{"article": k, "count": v} for k, v in code_data.items()])
        set_stock_parsed(code_data)

    links, headers, cookies = get_page()
    counts = get_group_count(
        [
            {"user_id": headers["correlationid"], "headers": headers, "group": i}
            for i in links
        ]
    )
    first_level_data = tqdm(
        desc="First level data parse", total=sum([i[1] for i in counts])
    )
    get_info(list(generate_api_endpoints(counts, headers)))
    del first_level_data

    stock_data_loader = tqdm(desc="Stock data parse", total=get_products_count())
    get_count(
        [
            {"headers": headers, "products": data}
            for data in chunks(list(compress_stock_parsed(get_all_articles())), 7)
        ]
    )
    del stock_data_loader
    full_info = tqdm(desc="Full info parse", total=get_products_count())
    product_page_ul = list(compress_full_info_parsed(get_all_urls()))
    get_product_page(product_page_ul)
    del full_info

    df: DataFrame = read_sql(
        select(ProductModel), engine.connect(), index_col="article"
    )
    df.to_excel("data/results.xlsx", sheet_name="result")
