"""Module defines the main entry point for the Apify Actor.

Feel free to modify this file to suit your specific needs.

To build Apify Actors, utilize the Apify SDK toolkit, read more at the official documentation:
https://docs.apify.com/sdk/python
"""

from __future__ import annotations
from urllib.parse import urljoin
from lxml import html
import re
from bs4 import BeautifulSoup
import gzip
import base64
import requests
from apify import Actor
from httpx import AsyncClient
import json
from datetime import datetime

_run_context = {
    "counter": 0  # MUST be an integer, not None
}


async def get_timestamp():
    return datetime.utcnow().isoformat() + "Z"


async def generate_source_run_id():
    # Ensure counter is initialized
    if _run_context.get("counter") is None:
        _run_context["counter"] = 0

    _run_context["counter"] += 1
    return f"run-portolapaints-{_run_context['counter']:03d}"


async def fetch_html(url: str) -> str:
    async with AsyncClient() as client:
        Actor.log.info(f"Fetching: {url}")
        response = await client.get(url, follow_redirects=False)
        if response.status_code == 200:
            return response.text


async def main() -> None:
    """Define a main entry point for the Apify Actor.

    This coroutine is executed using `asyncio.run()`, so it must remain an asynchronous function for proper execution.
    Asynchronous execution is required for communication with Apify platform, and it also enhances performance in
    the field of web scraping significantly.
    """
    # Enter the context of the Actor.
    async with Actor:
        # Retrieve the Actor input, and use default values if not provided.
        actor_input = await Actor.get_input() or {}
        start_urls = actor_input.get('start_urls', [
            'https://portolapaints.com/collections/new-standard-1-2',
            'https://portolapaints.com/collections/roman-clay',
            'https://portolapaints.com/collections/lime-wash'
        ])

        # Exit if no start URLs are provided.
        if not start_urls:
            Actor.log.info('No start URLs specified in Actor input, exiting...')
            await Actor.exit()

        # Enqueue the start URLs with an initial crawl depth of 0.
        for start_url in start_urls:
            All_Link = []
            page = 1
            while True:
                # Create an HTTPX client to fetch the HTML content of the URLs.
                async with AsyncClient() as client:
                    try:
                        # Fetch the HTTP response from the specified URL using HTTPX.
                        response = await client.get(start_url, follow_redirects=True)

                        tree = html.fromstring(response.text)

                        all_links = tree.xpath('//div[@class="productItem__wrapper"]/a/@href')
                        if len(All_Link) == len(all_links):
                            break
                        for link in all_links:
                            link_url = urljoin('https://portolapaints.com', link)

                            if link_url.startswith(('http://', 'https://')):
                                All_Link.append(link)
                                await process_link_url(link_url)
                        page += 1
                    except Exception:
                        Actor.log.exception(f'Cannot extract data from {start_url}.')


async def process_link_url(product_url: str):
    content_html = await fetch_html(product_url)
    if not content_html:
        Actor.log.info(f"Response Not Found: {product_url}")
        return
    soup = BeautifulSoup(content_html, 'html.parser')
    visible_text = soup.get_text(strip=True)
    compressed = gzip.compress(visible_text.encode('utf-8'))
    raw_text = base64.b64encode(compressed).decode('utf-8')
    tree = html.fromstring(content_html)
    description = ''.join(tree.xpath('//meta[@name="description"]/@content')).strip().replace(' ', ' ').replace('\n',
                                                                                                                ' ').strip()
    keys = []
    nav_elements = tree.xpath('//header/following-sibling::div/nav/p/text()')
    for nav_element in nav_elements:
        nav = nav_element.strip()
        if nav:
            keys.append(nav)
    nav_data_list = []
    try:
        nav_list = tree.xpath('//header/following-sibling::div/nav/following-sibling::div//ul')
    except:
        nav_list = []
    for i in nav_list:
        key = i.xpath('./li/strong/text()')
        key = [k.replace(':', '').strip() for k in key]
        value = i.xpath('./li/text()')
        value = [v.strip() for v in value]
        recommended_usage = value[0].replace('&', '').split(', ')
        value[0] = recommended_usage
        application = value[-2].split(', ')
        value[-2] = application
        nav_data = dict(zip(key, value))
        nav_data_list.append(nav_data)
    key_data = dict(zip(keys, nav_data_list))

    try:
        variant_json_string = \
            ''.join(tree.xpath('//script[@id="web-pixels-manager-setup"]/text()')).strip().split('productVariants":')[
                1].split(
                '],')[0].strip() + ']'
    except:
        variant_json_string = None
    variants = json.loads(variant_json_string)
    for variant in variants:
        variant_id = variant['id']
        product_name = variant['product']['title'].strip()
        variant_name = variant['title']
        specification_name = ''
        if '5/60 Flat' in variant_name:
            specification_name = variant_name.split('(')[0].replace('Flat', 'House & Trim').strip()
        if 'Flat' not in variant_name:
            specification_name = variant_name.split('(')[0].strip()
        result = None
        if not key_data:
            result = None
        for key in key_data:
            if specification_name in key:
                result = key_data[key]
                break
        recommendedUsage = None
        sheen = None
        coverage = None
        dryToTouch = None
        recoatTime = None
        application = None
        formulation = None
        if result:
            if 'Recoat Time' in result['Dry to Touch']:
                recot_time = result['Dry to Touch'].split(',')[-1]
                recot_time_key = recot_time.split(':')[0].strip()
                recot_time_value = recot_time.split(':')[1].strip()
                result.update({recot_time_key: recot_time_value})
                result['Dry to Touch'] = result['Dry to Touch'].split(',')[0].strip()
            recommendedUsage = result['Recommended Usage']
            sheen = result['Sheen']
            coverage = result['Coverage']
            dryToTouch = result['Dry to Touch']
            try:
                recoatTime = result['Recoat Time']
            except:
                recoatTime = None
            if not recoatTime:
                recoatTime = result['Re-coat Time']
            application = result['Application']
            formulation = result['Formulation']

        performance = {"recommendedUsage": recommendedUsage,
                       "sheen": sheen,
                       "coverage": coverage,
                       "dryToTouch": dryToTouch,
                       "recoatTime": recoatTime,
                       "application": application,
                       "formulation": formulation}
        useCase = performance['recommendedUsage']
        if not useCase:
            useCase_ = ''.join(tree.xpath(
                '(//header/following-sibling::div/nav/following-sibling::div//div[@class="rte comman_paragrap"]/p/text())[1]'))
            if 'can be applied to' in useCase_:
                useCase = []
                useCase__ = useCase_.split('can be applied to')[1]
                useCase.append(useCase__)
            if ' is applied in ' in useCase_:
                useCase = []
                useCase_text = useCase_.split(' is applied in ')[1]
                useCase.append(useCase_text)
        name = f'{product_name}–{variant_name}'
        match = re.search(r'\((.*?)\)', variant_name)
        unit_match = re.search(r'(\d+\.?\d*)\s*(\w+)$', variant_name)
        if not match:
            material = variant_name.split('/')[0].strip()
        else:
            material = match.group(0).replace('(', '').replace(')', '').strip()
        if not match and not unit_match:
            match = re.match(r'^\d+\s\w+', variant_name)
            unit = match.group(0)
        else:
            unit = unit_match.group(0)
        if material == unit:
            material = variant_name.split('/')[-1].strip()
        product_id = f"portola-paints-{product_name.lower().replace(' ', '-')}-{variant_name.lower().replace(' (', '-').replace(')', '').replace(' ', '-').replace('#3-/-', '')}".replace(
            '-/-', '-')
        vendor = "Portola Paints"
        category = "Wall Finishes"
        subcategory = "Paint"
        variant_price = variant['price']['amount']
        Color = product_name
        if 'Flat' in variant_name:
            finish = variant_name.split('(')[0].strip()
        else:
            finish = variant_name.split('/')[0].strip().split('(')[0]
        url = urljoin(product_url, f'?variant={variant_id}')
        color_note = ''.join(
            tree.xpath("(//em[text()='Color Notes:'])[1]/parent::span/following-sibling::text()")).strip()
        if not color_note:
            color_note = ''.join(tree.xpath(
                "(//em[text()='Color Notes:'])[1]/parent::span/parent::span/following-sibling::span/text()")).strip()
        if not color_note:
            color_note = None
        additionalData = {
            "priceUnit": f"per {unit}",
            "colorNotes": color_note,
            "raw_text": raw_text
        }
        headers = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,hi;q=0.7',
            'priority': 'u=1, i',
            'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
        }

        params = {
            '_': '1748427774749',
        }
        product_name_for_tags = ''
        if 'Roman Clay' in variant_name:
            product_name_for_tags = f"{product_name.lower().replace(' ', '-')}-roman-clay"
        if not 'Roman Clay' in variant_name and 'Lime Wash' in variant_name:
            product_name_for_tags = f"{product_name.lower().replace(' ', '-')}-lime-wash"
        if not 'Roman Clay' in variant_name and not 'Lime Wash' in variant_name:
            product_name_for_tags = f"{product_name.lower().replace(' ', '-')}-acrylic"

        response_tags = requests.get(f'https://portolapaints.com/products/{product_name_for_tags}.json', params=params,
                                     headers=headers, )
        if response_tags.status_code != 200:
            product_name_for_tags = product_url.split('/')[-1].strip()
            response_tags = requests.get(f'https://portolapaints.com/products/{product_name_for_tags}.json',
                                         params=params,
                                         headers=headers, )
        if response_tags.status_code == 200:
            content_for_tags = json.loads(response_tags.text)
            collection = content_for_tags['product']['product_type'].strip()
            images = content_for_tags['product']['images']
            images_link = []
            for img in images:
                src = img['src']
                images_link.append(src)
        else:
            collection = None
            images_link = None
        variant_group = f"{product_name.replace(' ', '-').lower()}"
        item = {"id": product_id,
                "name": name,
                "vendor": vendor,
                "category": category,
                "subcategory": subcategory,
                "description": description,
                "imageUrl": images_link,
                "url": url,
                "material": material,
                "useCase": useCase,
                "leadTime": None,
                "price": variant_price,
                "sustainability": [],
                "certifications": [],
                "documents": [],
                "location": "USA",
                "collection": collection,
                "variantGroup": variant_group,
                "storedImagePath": None,
                "color": Color,
                "finish": finish,
                "tags": [],
                "createdAt": await get_timestamp(),
                "lastUpdated": await get_timestamp(),
                "sourceRunId": await generate_source_run_id(),
                "sourceType": "scraped",
                "dataConfidence": "high",
                "wasManuallyEdited": False,
                "specifications": {"performance": performance},
                "additionalData": additionalData}
        await Actor.push_data(item)
