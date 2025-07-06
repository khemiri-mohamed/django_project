"""Module defines the main entry point for the Apify Actor.

Feel free to modify this file to suit your specific needs.

To build Apify Actors, utilize the Apify SDK toolkit, read more at the official documentation:
https://docs.apify.com/sdk/python
"""

from __future__ import annotations
import time
from urllib.parse import urljoin
from lxml import html
from bs4 import BeautifulSoup
import gzip
import base64
from datetime import datetime
from apify import Actor
from httpx import AsyncClient
import json

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
    return f"run-ziatile-{_run_context['counter']:03d}"


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
            "https://www.ziatile.com/collections/zellige",
            "https://www.ziatile.com/collections/cement-tile",
            "https://www.ziatile.com/collections/cotto",
            "https://www.ziatile.com/collections/terrazzo",
            "https://www.ziatile.com/collections/marble-tile",
            "https://www.ziatile.com/collections/cantera-tile",
            "https://www.ziatile.com/collections/limestone-tile",
            "https://www.ziatile.com/collections/ceramic-tile"
        ])

        # Exit if no start URLs are provided.
        if not start_urls:
            Actor.log.info('No start URLs specified in Actor input, exiting...')
            await Actor.exit()

        # Enqueue the start URLs with an initial crawl depth of 0.
        for start_url in start_urls:
            if 'zellige' in start_url:
                subCategory = "Zellige"
            if 'cement-tile' in start_url:
                subCategory = "Cement"
            if 'cotto' in start_url:
                subCategory = "Terracotta"
            if 'terrazzo' in start_url:
                subCategory = "Terrazzo"
            if 'marble-tile' in start_url:
                subCategory = "Marble"
            if 'cantera-tile' in start_url:
                subCategory = "Cantera"
            if 'limestone-tile' in start_url:
                subCategory = "Limestone"
            if 'ceramic-tile' in start_url:
                subCategory = "Ceramic"
            All_urls = []
            position = 100
            while True:
                params = {
                    'position': f'{position}',
                }
                # Create an HTTPX client to fetch the HTML content of the URLs.
                async with AsyncClient() as client:
                    try:
                        # Fetch the HTTP response from the specified URL using HTTPX.
                        response = await client.get(start_url, follow_redirects=True, params=params)

                        tree = html.fromstring(response.text)

                        all_links = tree.xpath('//div[@data-position]/div/a/@href')
                        if len(All_urls) == len(all_links):
                            break

                        for link in all_links:

                            link_url = urljoin('https://www.ziatile.com', link)

                            if link_url.startswith(('http://', 'https://')):
                                if link_url not in All_urls:
                                    All_urls.append(link_url)
                                    await process_link_url(link_url, subCategory)
                        position += 100
                    except Exception:
                        Actor.log.exception(f'Cannot extract data from {start_url}.')


async def process_link_url(product_url: str, subCategory: str):
    time.sleep(1)
    content_html = await fetch_html(product_url)
    if not content_html:
        Actor.log.info(f"Response Not Found: {product_url}")
        return
    soup = BeautifulSoup(content_html, 'html.parser')
    visible_text = soup.get_text(strip=True)
    compressed = gzip.compress(visible_text.encode('utf-8'))
    raw_text = base64.b64encode(compressed).decode('utf-8')
    tree = html.fromstring(content_html)
    json_response = tree.xpath('//script[@id="__NEXT_DATA__"]/text()')[0]
    json_data = json.loads(json_response)
    name = ''.join(json_data['props']['pageProps']['product']['title'])
    product_type = ''.join(json_data['props']['pageProps']['product']['productType'])

    product_id = 'zia-' + name.lower().replace(' ', '-') + '-' + product_type.lower()

    description = ''.join(tree.xpath(
        '//div[@class="product__noteWrapper"]/div//div[@class="sc-79669c64-8 dyCBTc"]//p/text()'))
    img_url = []
    images = json_data['props']['pageProps']['product']['images']
    for img in images:
        src = img['src']
        img_url.append(src)
    use_case = []
    tile_usages = json_data['props']['story']['content']['tileUsages']
    for tile_ in tile_usages:
        if tile_['productTemplate'] == 'pdp-zellige':
            tile_usage = tile_['tileUsageAreas']
            for tile in tile_usage:
                if tile['usable']:
                    tile__usages = tile['title']
                    use_case.append(tile__usages)
    if not img_url:
        img_url = []
    tags = json_data['props']['pageProps']['product']['tags']
    tags[:] = [z for z in tags if all(x not in z for x in [':', '|', ' - ', ': '])]
    price = float(''.join(tree.xpath(
        "//p[text()='Price per ft']/parent::div/following-sibling::div/p/span[2]/text()")))
    tiles_per_box = ''.join(tree.xpath("//p[text()='Tiles/Box']/following-sibling::p/text()"))
    box_Coverage_SqFt = ''.join(
        tree.xpath("//p[text()='Total ft']/parent::div/following-sibling::p/span/text()"))
    price_per_tile = float(''.join(tree.xpath(
        "//p[text()='Price per tile']/parent::div/following-sibling::p/span[2]/text()")))
    overageRecommendation = '-'.join(tree.xpath('//select[@name="overage"]/option/text()')[1:]).strip()

    additionalData = {
        "priceUnit": "per sqft",
        "tilesPerBox": float(tiles_per_box),
        "boxCoverageSqFt": float(box_Coverage_SqFt),
        "pricePerTile": float(price_per_tile),
        "overageRecommendation": overageRecommendation,
        "raw_text": raw_text
    }
    meta_data = json_data['props']['pageProps']['product']['metafields']
    lead_time = None
    color = None
    thickness = None
    width = None
    length = None
    specifications = {}
    for md in meta_data:
        if not md:
            continue
        key = md['key']
        if key == 'lead_time':
            lead_time = md['value']
        if key == 'tag' or key == 'color':
            if color:
                continue
            color = md['value'].title()
        if key == 'thickness':
            thickness = float(md['value'])
        if key == 'width':
            width = float(md['value'])
        if key == 'length':
            length = float(md['value'])
        dimensions_data = {"width": width,
                           "length": length,
                           "thickness": thickness,
                           "units": "in"}
        specifications = {"dimensions": dimensions_data,
                          "application": None,
                          "performance": {
                              "frostResistant": None,
                              "slipResistant": None,
                              "waterAbsorption": None
                          },
                          "care": None
                          }
    if subCategory == 'Zellige':
        material = 'Ceramic'
    else:
        material = subCategory
    if color:
        variantGroup = f'{color.strip().replace(" + ", " ")}-{subCategory.strip()}'.lower().replace(' ', '-')
    else:
        variantGroup = f'{subCategory.strip()}'.lower().replace(' ', '-')
    item = {"id": product_id,
            "name": name,
            "vendor": "Zia Tile",
            "category": "Tile",
            "subcategory": subCategory,
            "description": description,
            "imageUrl": img_url,
            "url": product_url,
            "material": material,
            "useCase": use_case,
            "leadTime": lead_time,
            "price": price,
            "sustainability": [],
            "certifications": [],
            "documents": [],
            "location": "Morocco",
            "collection": None,
            "variantGroup": variantGroup,
            "storedImagePath": None,
            "color": color,
            "finish": None,
            "tags": [],
            "createdAt": await get_timestamp(),
            "lastUpdated": await get_timestamp(),
            "sourceRunId": await generate_source_run_id(),
            "sourceType": "scraped",
            "dataConfidence": "high",
            "wasManuallyEdited": False,
            "specifications": specifications,
            "additionalData": additionalData}

    await Actor.push_data(item)
