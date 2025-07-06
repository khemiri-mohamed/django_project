"""Module defines the main entry point for the Apify Actor.

Feel free to modify this file to suit your specific needs.

To build Apify Actors, utilize the Apify SDK toolkit, read more at the official documentation:
https://docs.apify.com/sdk/python
"""

from __future__ import annotations

import re
import time
from urllib.parse import urljoin
from lxml import html
from bs4 import BeautifulSoup
import gzip
import base64
from apify import Actor
from httpx import AsyncClient
import json
from datetime import datetime

deduped_items = {}

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
    return f"run-flor-{_run_context['counter']:03d}"


async def fetch_html(url: str, params) -> str:
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',

    }
    async with AsyncClient(timeout=30.0) as client:
        Actor.log.info(f"Fetching: {url}")
        response = await client.get(url, follow_redirects=False, params=params, headers=headers)
        if response.status_code == 200:
            return response.text
    if response.status_code != 200:
        retry = 1
        while True:
            time.sleep(2)
            if retry > 3:
                break
            async with AsyncClient() as client:
                Actor.log.info(f"Fetching: {url}")
                response = await client.get(url, follow_redirects=False, params=params, headers=headers)
                if response.status_code == 200:
                    break
                retry += 1
        if response.status_code == 200:
            return response.text


async def main() -> None:
    """Define a main entry point for the Apify Actor.

    This coroutine is executed using `asyncio.run()`, so it must remain an asynchronous function for proper execution.
    Asynchronous execution is required for communication with Apify platform, and it also enhances performance in
    the field of web scraping significantly.
    """
    async with Actor:
        Actor.log.info('Hello from the Actor!')
        actor_input = await Actor.get_input() or {}
        start_urls = actor_input.get('start_urls', [
            'https://www.flor.com/area-rugs_carpet-tiles/',
        ])

        # Exit if no start URLs are provided.
        if not start_urls:
            Actor.log.info('No start URLs specified in Actor input, exiting...')
            await Actor.exit()

        for start_url in start_urls:

            All_Link = []
            sz = 48
            while True:
                params = {
                    'start': '0',
                    'sz': f'{sz}',
                }
                updated_start_url = start_url
                try:
                    response = await fetch_html(updated_start_url, params)
                    if response:
                        tree = html.fromstring(response)
                        all_links = tree.xpath(
                            '//div[@class="b-product-tile__wishlist js-product"]/following-sibling::a/@href')
                        if len(All_Link) == len(all_links):
                            break
                        for link in all_links:
                            link_url = urljoin('https://www.flor.com', link)
                            if "/sale/" in link_url:
                                continue
                            if link_url in All_Link:
                                continue
                            if link_url.startswith(('http://', 'https://')):
                                All_Link.append(link_url)
                        sz += 24

                except Exception as e:
                    Actor.log.exception(f'Cannot extract data from {updated_start_url}.')
                    print(e)
            for product_url in All_Link:
                await process_link_url(product_url)

        all_unique_items = [data for _, data in deduped_items.values()]
        for unique_items in all_unique_items:
            unique_items['sourceRunId'] = await generate_source_run_id()
            await Actor.push_data(unique_items)


async def process_link_url(product_url: str):
    content_html = await fetch_html(product_url, params=None)
    soup = BeautifulSoup(content_html, 'html.parser')
    visible_text = soup.get_text(strip=True)
    compressed = gzip.compress(visible_text.encode('utf-8'))
    raw_text = base64.b64encode(compressed).decode('utf-8')
    tree = html.fromstring(content_html)
    error_page = tree.xpath('//img[@class="b-error-page__img h-visible-md h-visible-lg h-visible-xl h-visible-xxl"]')
    if not error_page:
        variant_data_list = ''.join(tree.xpath('//div/@data-product')).strip()
        json_data = json.loads(variant_data_list)
        name_add_up_first_part = ''.join(tree.xpath('//h1[@id="productTitle"]/text()'))
        try:
            name_add_up_third_part = \
                ''.join(tree.xpath("//div[contains(text(),'Tile Size')]/following-sibling::div/text()")).strip().split(
                    '(')[
                    0]
        except:
            name_add_up_third_part = ''
        try:
            variant_list = json_data['product']['variants']
        except:
            variant_list = None
        if variant_list:
            for variant in variant_list:
                if name_add_up_third_part:
                    name = f"{variant['name']} ({name_add_up_third_part} Tile)"
                    width = float(name_add_up_third_part.split('in x')[0])
                    height = float(name_add_up_third_part.split('in x')[1].replace('in', '').strip())
                else:
                    name = f'{name_add_up_first_part}'
                    width = None
                    height = None
                url = variant['url']
                if "/sale/" in url:
                    continue
                try:
                    updated_color = variant['name'].split('-')[1].strip().title()
                except:
                    updated_color = None
                if not updated_color:
                    continue
                id_name = updated_color.lower().replace(' / ', '-').replace(' - ', '-').replace(' ', '-')
                if height and width:
                    Id = f'flor-{id_name}-{str(width)}x{str(height)}-tile'
                else:
                    Id = f'flor-{id_name}-tile'
                description = ''.join(tree.xpath('//meta[@property="og:description"]/@content'))
                images = variant['image_url']
                if 'flor-image-placeholder' in images:
                    Images = None
                else:
                    Images = [images]
                if not Images:
                    continue
                material = ''.join(
                    tree.xpath("//div[contains(text(),'Fiber Content')]/following-sibling::div/text()")).strip()
                if not material:
                    material = None
                lead_time = ''.join(
                    tree.xpath("//button[contains(text(),' Delivery')]/following-sibling::div/p/text()")).strip().split(
                    'items ship within')[-1].strip()
                if not lead_time:
                    lead_time = None
                price = float(''.join(
                    tree.xpath('//div[@class="b-prs__price h-margin-top-16"]//div/span/span[1]/text()')[-1]).replace(
                    '\n', '').replace(',', '').replace('$', '').strip())
                if not price:
                    price = float(''.join(tree.xpath(
                        '(//div[@class="b-prs__price h-margin-top-16"]//div[@class="b-price_range range"]//div/span/text())[2]')).replace(
                        '\n', '').replace(',', '').replace('$', '').strip())
                sustainability_one = ''.join(tree.xpath("//div[contains(text(),'Total Recycled Content')]/text()"))
                sustainability_two = ''.join(
                    tree.xpath(
                        "//div[contains(text(),'Total Recycled Content')]/following-sibling::div/text()")).strip()
                sustainability = f'{sustainability_two} {sustainability_one}'.strip()
                if not sustainability:
                    sustainability = None
                certificate = ''.join(tree.xpath("//div[contains(text(),'Certified')]/text()")).strip()
                if not certificate:
                    certificate = []
                else:
                    certificate = [certificate]
                varintGroup = f'{name_add_up_first_part.split("-")[0].lower()}-{product_url.split("-")[-2].split("/")[-1]}-{product_url.split("-")[-1].replace(".html", "")}'.replace(
                    ' ', '-')
                try:
                    thickness = float(''.join(tree.xpath(
                        "//div[contains(text(),'Total Thickness')]/following-sibling::div/text()")).strip().replace(
                        'in', ''))
                except:
                    thickness = None
                dimensions = {
                    "width": width,
                    "length": height,
                    "thickness": thickness,
                    "units": "in"
                }
                composition = [{
                    "material": material,
                    "percentage": None
                }]
                construction = ''.join(
                    tree.xpath("//div[contains(text(),'Product Construction')]/following-sibling::div/text()")).strip()
                pile_height = ''.join(
                    tree.xpath("//div[contains(text(),'Pile Height')]/following-sibling::div/text()")).strip()
                pile_height_match = re.search(r'([-+]?\d*\.\d+)\s*(in)', pile_height)
                if pile_height_match:
                    pile_height_value = float(pile_height_match.group(1))  # Convert number part to float
                else:
                    pile_height_value = None
                specifications = {
                    "dimensions": dimensions,
                    "composition": composition if composition else None,
                    "construction": construction if construction else None,
                    "pileHeight": pile_height_value if pile_height else None,
                    "application": ["Floor"],
                    "performance": None,
                    "care": None
                }
                price_unit = ''.join(
                    tree.xpath('//div[@class="b-prs__price h-margin-top-16"]//div/span/span[2]/text()')).strip()
                Pile_Density = ''.join(
                    tree.xpath("//div[contains(text(),'Pile Density')]/following-sibling::div/text()")).strip().replace(
                    ',', '').split('in')[0]
                if Pile_Density:
                    Pile_Density = float(Pile_Density)
                else:
                    Pile_Density = None
                pile_thickness = ''.join(
                    tree.xpath("//div[contains(text(),'Pile Thickness')]/following-sibling::div/text()")).strip()
                post_consumer = ''.join(
                    tree.xpath("//div[contains(text(),'Post Consumer')]/following-sibling::div/text()")).strip()
                industry = ''.join(
                    tree.xpath("//div[contains(text(),'Post Industrial')]/following-sibling::div/text()")).strip()
                recycle = ''.join(
                    tree.xpath(
                        "//div[contains(text(),'Total Recycled Content')]/following-sibling::div/text()")).strip()
                carbon = ''.join(
                    tree.xpath("//div[contains(text(),'Carbon Footprint')]/following-sibling::div/text()")).strip()
                backing = ''.join(
                    tree.xpath("//div[contains(text(),'Standard Backing')]/following-sibling::div/text()")).strip()
                static_kv = ''.join(
                    tree.xpath("//div[contains(text(),'Static Kv')]/following-sibling::div/text()")).strip()
                installation = ''.join(
                    tree.xpath("//div[contains(text(),'Installation')]/following-sibling::div/text()")).strip()
                pileThickness = re.search(r'([-+]?\d*\.\d+)\s*(in)', pile_thickness)
                if pileThickness:
                    pileThickness_value = float(pileThickness.group(1))  # Convert number part to float
                else:
                    pileThickness_value = None
                if post_consumer:
                    consumer_value = float(post_consumer.strip('%')) / 100
                else:
                    consumer_value = None
                if industry:
                    industry_value = float(industry.strip('%')) / 100
                else:
                    industry_value = None
                if recycle:
                    recycle_value = float(recycle.strip('%')) / 100
                else:
                    recycle_value = None
                additionalData = {
                    "priceUnit": price_unit.lower() if price_unit else None,
                    "pileDensity": Pile_Density if Pile_Density else None,
                    "pileThickness": pileThickness_value if pile_thickness else None,
                    "postConsumer": consumer_value if post_consumer else None,
                    "postIndustrial": industry_value if industry else None,
                    "totalRecycledContent": recycle_value if recycle else None,
                    "carbonFootprint": carbon if carbon else None,
                    "backing": backing if backing else None,
                    "staticKv": static_kv if static_kv else None,
                    "installation": installation if installation else None,
                    "raw_text": raw_text
                }
                fingerprint = (
                    url.split("?")[0].replace("/sale/", "").lower(),
                    f"{width}x{height}",
                    updated_color.lower()
                )

                # --- Extract variant number from product_url (like -03, -07) ---
                variant_match = re.search(r'(\d{2})\.html$', product_url)
                variant_number = int(variant_match.group(1)) if variant_match else 0

                item = {
                    "id": Id,
                    "name": name,
                    "vendor": "FLOR",
                    "category": "Rugs",
                    "subcategory": "Machine-made",
                    "description": description,
                    "imageUrl": Images,
                    "url": url,
                    "material": material,
                    "useCase": None,
                    "leadTime": lead_time,
                    "price": price,
                    "sustainability": sustainability,
                    "certifications": certificate,
                    "documents": [],
                    "location": None,
                    "collection": None,
                    "variantGroup": varintGroup,
                    "storedImagePath": None,
                    "color": updated_color.title(),
                    "finish": None,
                    "tags": [],
                    "createdAt": await get_timestamp(),
                    "lastUpdated": await get_timestamp(),
                    "sourceRunId": None,
                    "sourceType": "scraped",
                    "dataConfidence": "high",
                    "wasManuallyEdited": False,
                    "specifications": specifications,
                    "additionalData": additionalData
                }
                if fingerprint not in deduped_items:
                    deduped_items[fingerprint] = (variant_number, item)
                else:
                    existing_variant_number, _ = deduped_items[fingerprint]
                    if variant_number > existing_variant_number:
                        deduped_items[fingerprint] = (variant_number, item)
    else:
        return False
