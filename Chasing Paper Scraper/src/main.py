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
        start_urls = actor_input.get("url", [
            "https://chasingpaper.com/collections/wallpaper",
            "https://chasingpaper.com/collections/murals"
        ])

        # Exit if no start URLs are provided.
        if not start_urls:
            Actor.log.info('No start URLs specified in Actor input, exiting...')
            await Actor.exit()

        # Enqueue the start URLs with an initial crawl depth of 0.
        for start_url in start_urls:
            if "wallpaper" in start_url:
                subcategory = "Wallpaper"
            if 'murals' in start_url:
                subcategory = "Murals"
            page = 2
            while True:
                # Create an HTTPX client to fetch the HTML content of the URLs.
                async with AsyncClient() as client:
                    try:
                        # Fetch the HTTP response from the specified URL using HTTPX.
                        response = await client.get(start_url, follow_redirects=True)

                        tree = html.fromstring(response.text)

                        all_links = tree.xpath('//article//a[@class="link-wrapper"]/@href')
                        if not all_links:
                            break
                        for link in all_links:
                            link_url = urljoin('https://chasingpaper.com', link)

                            if link_url.startswith(('http://', 'https://')):
                                await process_link_url(link_url, subcategory)
                        next_page_url = ''.join(
                            tree.xpath(f'//nav[@class="pagination"]/ul/li/a[@aria-label="Page {page}"]/@href')).strip()
                        if not next_page_url:
                            break
                        start_url = urljoin('https://chasingpaper.com', next_page_url)
                        page += 1

                    except Exception:
                        Actor.log.exception(f'Cannot extract data from {start_url}.')


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
    return f"run-chasingpaper-{_run_context['counter']:03d}"


async def process_link_url(product_url: str, subcategory: str):
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
    description = ''.join(tree.xpath('//div[@class="product-description__content"]/p//text()')).strip().replace(' ',
                                                                                                                ' ', ).replace(
        '\n', ' ').strip()
    if not description:
        description = ''.join(
            tree.xpath('//*[contains(text(),"Details")]/following-sibling::span/text()')).strip().replace(' ',
                                                                                                          ' ', ).replace(
            '\n', ' ').strip()
    if not description:
        description = ''.join(tree.xpath('//meta[@property="og:description"]/@content')).strip().replace(' ',
                                                                                                         ' ', ).replace(
            '\n', ' ').strip()
    if not description:
        description = None
    try:
        repeatVertical = float(
            ''.join(tree.xpath('//*[contains(text(),"Specs")]/following-sibling::ul/li//text()')).strip().split(
                '”')[
                0].strip().split('"')[0].strip())
    except:
        repeatVertical = None
    all_sizes = ''.join(tree.xpath('//*[contains(text(),"Specs")]/following-sibling::ul/li[1]//text()')).strip()
    all_text = tree.xpath('//*[contains(text(),"Specs")]/following-sibling::ul/li//text()')
    try:
        printed_strings = [s for s in all_text if s.startswith("Printed with")][0].split('.')
    except:
        printed_strings = []
    finish = None
    certifications = []
    sustainability = None
    for printed_string in printed_strings:
        if 'Finish' in printed_string:
            finish = printed_string.replace('Finish', '').strip()
        if 'Printed with' in printed_string:
            sustainability = printed_string
            certification = printed_string.replace('Printed with', '').replace('Certified Ink', '').strip()
            if certification:
                certifications.append(certification)

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
        variant_name = variant['title'].split('--')[0].strip()
        variant_price = variant['price']['amount']
        Color = '-'.join(variant_name.split('/')[2:]).strip()
        tags = variant_name.split('/')[0].strip()
        dimensions = variant_name.split('/')[1].strip()
        variant_id_name = tags.replace(' and ', ' ').replace(' ',
                                                             '-').lower() + '-' + dimensions.strip().lower().replace(
            'ft', '').replace('in', '').replace('high', '').replace('wide', '').strip().replace(' x ', 'x').replace(' ',
                                                                                                                    '-') + '-' + Color.lower()
        Id = f"chasingpaper-{product_url.split('/')[-1].strip()}-{variant_id_name}".replace(' x', 'x').replace('-x', 'x')
        title_name = variant['product']['title']
        name = f"{title_name} {tags} {subcategory} - {Color} ({dimensions})"
        variantGroup = title_name.strip().replace(' ', '-').lower()
        Images = []
        images = variant['image']['src']
        if 'https:' not in images:
            images = 'https:' + images
            Images.append(images)
        url = product_url + f'?variant={variant_id}'
        if 'ft' in dimensions:
            width = float(dimensions.replace('Sample', '').strip().split('x')[0].strip().split('ft')[0].strip())
            width = round(width * 12, 2)
            length = float(
                dimensions.replace('Sample', '').strip().split('x')[1].strip().split(' ')[0].strip().split('ft')[
                    0].strip())
            length = round(length * 12, 2)
        else:
            width = float(dimensions.replace('Sample', '').strip().split('x')[0].strip().split('ft')[0].strip())
            length = float(
                dimensions.replace('Sample', '').strip().split('x')[1].strip().split(' ')[0].strip().split('ft')[
                    0].strip())
        dimensions_data = {"width": width,
                           "length": length,
                           "thickness": None,
                           "units": "in"}
        if not all_sizes:
            all_sizes = None
        pattern = {"type": None,
                   "repeatVertical": repeatVertical,
                   "repeatHorizontal": None,
                   "match": None}
        specifications = {"dimensions": dimensions_data,
                          "pattern": pattern,
                          "application": None,
                          "performance": None,
                          "care": None}
        if subcategory == 'Murals':
            additionalData = {"all_sizes": all_sizes,
                              "wallpaper_type": tags,
                              "priceUnit": "per mural",
                              "raw_text": raw_text}
        else:
            additionalData = {"all_sizes": None,
                              "wallpaper_type": tags,
                              "priceUnit": "per roll",
                              "raw_text": raw_text}
        if 'sample' in dimensions.lower():
            additionalData['priceUnit'] = "per sample"
        item = {"id": Id,
                "name": name,
                "vendor": "Chasing Paper",
                "category": "Wall Finishes",
                "subcategory": subcategory,
                "description": description,
                "imageUrl": Images,
                "url": url,
                "material": tags,
                "useCase": None,
                "leadTime": None,
                "price": variant_price,
                "sustainability": sustainability,
                "certifications": certifications,
                "documents": None,
                "location": "USA",
                "collection": None,
                "variantGroup": variantGroup,
                "storedImagePath": None,
                "color": Color,
                "finish": finish,
                "tags": [tags],
                "createdAt": await get_timestamp(),
                "lastUpdated": await get_timestamp(),
                "sourceRunId": await generate_source_run_id(),
                "sourceType": "scraped",
                "dataConfidence": "high",
                "wasManuallyEdited": False,
                "specifications": specifications,
                "additionalData": additionalData}
        await Actor.push_data([item])