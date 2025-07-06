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
    return f"run-eskayel-{_run_context['counter']:03d}"


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
    async with Actor:
        # Retrieve the Actor input, and use default values if not provided.
        actor_input = await Actor.get_input() or {}
        start_urls = actor_input.get('start_urls', [
            'https://eskayel.com/collections/wallpaper',
            'https://eskayel.com/collections/fabric',
            'https://eskayel.com/collections/rugs'
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
                updated_start_url = f'{start_url}?page={page}'
                async with AsyncClient() as client:
                    try:

                        # Fetch the HTTP response from the specified URL using HTTPX.
                        response = await client.get(updated_start_url, follow_redirects=True)

                        tree = html.fromstring(response.text)

                        all_links = tree.xpath('//a[contains(@href,"/products")]/@href')
                        if not all_links:
                            break
                        for link in all_links:
                            if 'products' in link:
                                if link in All_Link:
                                    continue
                                All_Link.append(link)
                                link_url = urljoin('https://eskayel.com', link)
                                if link_url.startswith(('http://', 'https://')):
                                    await process_link_url(link_url, link)
                        page += 1

                    except Exception:
                        Actor.log.exception(f'Cannot extract data from {start_url}.')


async def process_link_url(product_url: str, link: str):
    content_html = await fetch_html(product_url)
    soup = BeautifulSoup(content_html, 'html.parser')
    visible_text = soup.get_text(strip=True)
    compressed = gzip.compress(visible_text.encode('utf-8'))
    raw_text = base64.b64encode(compressed).decode('utf-8')
    tree = html.fromstring(content_html)

    if 'fabric' in link:
        variant_listing = f"{product_url}/products.json"
        json_response = await fetch_html(variant_listing)

        variant_json = json.loads(json_response)
        data_for_color_and_variant_group = variant_json['product']['title']
        variant_data = variant_json['product']['variants']
        for id_ in variant_data:
            fabric_id = id_['id']
            url = product_url + f'?variant={fabric_id}'
            name = ''.join(tree.xpath('//div[@class="product__title"]/h1/text()'))
            description = ''.join(
                tree.xpath("//h3[text()='Description']/following-sibling::p//text()")).strip()
            if not description:
                description = ''.join(
                    tree.xpath("//h3[text()='Description']/following-sibling::span/text()"))
            images = tree.xpath(
                '//div[@class="product__media media media--transparent gradient global-media-settings"]/img/@src')
            images = ['http:' + img if img.startswith('//') else img for img in images]
            subcategory = None
            material = id_['title']
            material_list_first_cat = ['linen/cotton', 'oyster linen', 'enhanced linen', 'heavyweight linen',
                                       'heavy weight linen']
            material_list_second_cat = ['performance', 'performance and outdoor', '100% polyester', 'Performance ']
            if material in material_list_first_cat:
                subcategory = 'Natural Fiber'
            if material in material_list_second_cat or 'Performance' in name:
                subcategory = 'Synthetic'
            if not subcategory:
                if 'oyster linen' in description.lower():
                    subcategory = 'Natural Fiber'
                if 'linen/cotton' in description.lower():
                    subcategory = 'Natural Fiber'
                if 'cotton' in description.lower():
                    subcategory = 'Natural Fiber'
            useCase = None
            lead_time = \
                ''.join(tree.xpath('//div[@class="product_quote"]/preceding-sibling::p/text()')).split(
                    ":")[-1].strip()
            price = float(id_['price'])
            collection = name.split()[0].lower()
            variantGroup = data_for_color_and_variant_group.split('||')[0].lower().replace(' ', '-')
            color = data_for_color_and_variant_group.split('||')[1].lower().strip()
            full_name = f'{name} - {color} ({material},per yard)'
            finish = None
            width_data = ''.join(tree.xpath("//h3[text()='Specs']/following-sibling::p/text()")).strip()
            match = re.search(r'FABRIC WIDTH:\s*([\d.]+[″"]?)', width_data)
            fabric_width = None
            if match:
                fabric_width = match.group(1)
            try:
                width = float(fabric_width)
            except:
                width = ''
            if not width:
                width = None
            try:
                length = float(
                    ''.join(tree.xpath("//p[contains(text(),'MATERIAL')]/text()[3]")).split(':')[
                        1].split('(')[0].strip().split('x')[1].replace("'", '').strip()) * 12
            except:
                length = ''
            if not length:
                length = None
            product_type = ''.join(tree.xpath('//div[@class="product_quote"]/p/text()')).strip()
            match = re.search(r'\bhalf[-\s]?drop\b', product_type, re.IGNORECASE)
            if match:
                match = match.group()
            try:
                pattern_vertical = float(''.join(tree.xpath('//div[@class="product_quote"]/p/text()')).split(
                    'Vertical Repeat:')[1].split('"')[0].split('”')[0])
            except:
                pattern_vertical = ''
            if not pattern_vertical:
                pattern_vertical = None
            try:
                pattern_horizontal = float(
                    ''.join(tree.xpath('//div[@class="product_quote"]/p/text()')).split(
                        'Horizontal Repeat:')[1].replace('"', '').split('”')[0])
            except:
                pattern_horizontal = ''
            if not pattern_horizontal:
                try:
                    pattern_horizontal = float(
                        ''.join(tree.xpath('//div[@class="product_quote"]/p/text()')).split(':')[
                            1].split('/')[0].split()[-1].replace('"', ''))
                except:
                    pattern_horizontal = ''
            if not pattern_horizontal:
                pattern_horizontal = None
            care = ''.join(tree.xpath(
                "//h3[contains(text(),' Care')]/parent::summary/following-sibling::div/p/text()")).strip()
            if 'Default Title' in material:
                material = None
            specifications = {
                "dimensions": {'width': width,
                               'length': length,
                               'thickness': None,
                               'units': 'in'},
                "composition": [{'material': material,
                                 'percentage': None}],
                "pattern": {
                    "type": match,
                    "repeatVertical": pattern_vertical,
                    "repeatHorizontal": pattern_horizontal,
                    "match": None
                },
                "application": [],
                "performance": None,
                "care": care
            }
            price_unit = 'per yard'
            additionalData = {
                "priceUnit": price_unit,
                "raw_text": raw_text
            }
            Id_part = data_for_color_and_variant_group.strip().replace('||', '-').replace(' ', '-').lower()
            if material:
                Id = f'eskayel-{Id_part}-{material.replace("/", "-")}'.strip().replace(' ', '-')
            else:
                Id = f'eskayel-{Id_part}'.strip().replace(' ', '-')
            sustainability = ''.join(tree.xpath(
                "//h3[contains(text(),' Sustainability')]/parent::summary/following-sibling::div/p/text()")).strip()
            item = {"id": Id,
                    "name": full_name,
                    "vendor": 'Eskayel',
                    "category": 'Textiles',
                    "subcategory": subcategory,
                    "description": description,
                    "imageUrl": images,
                    "url": url,
                    "material": material,
                    "useCase": useCase,
                    "leadTime": lead_time,
                    "price": price,
                    "sustainability": [sustainability],
                    "certifications": [],
                    "documents": [],
                    "location": None,
                    "collection": collection,
                    "variantGroup": variantGroup,
                    "storedImagePath": None,
                    "color": color,
                    "finish": finish,
                    "tags": [],
                    "createdAt": await get_timestamp(),
                    "lastUpdated": await get_timestamp(),
                    "sourceRunId": await generate_source_run_id(),
                    "sourceType": "scraped",
                    "dataConfidence": "high",
                    "wasManuallyEdited": False,
                    "specifications": specifications,
                    "additionalData": additionalData}
            await Actor.push_data([item])
    elif 'rug' in link:
        rug_types = [
            "Hand-knotted",
            "Hand-tufted",
            "Handloom",
            "Machine-made",
            "Braided",
            "Shag",
            "Hooked",
            "Natural Fiber"
        ]
        variant_listing = f"{product_url}/products.json"
        json_response = await fetch_html(variant_listing)
        variant_json = json.loads(json_response)
        data_for_color_and_variant_group = variant_json['product']['title']

        variant_data = variant_json['product']['variants']
        for id_ in variant_data:
            rug_id = id_['id']
            url = f'{product_url}?variant={rug_id}'
            name = ''.join(tree.xpath('//div[@class="product__title"]/h1/text()'))
            edited_rugs = ''
            for rug in rug_types:
                edited_rug = rug.lower().replace('-', ' ')
                edited_name = name.lower()
                if edited_rug in edited_name:
                    edited_rugs += edited_rug.title().replace(' ', '-')

            full_name = name
            description = ''.join(
                tree.xpath("//h3[text()='Description']/following-sibling::p//text()")).strip()
            if not description:
                description = ''.join(
                    tree.xpath("//h3[text()='Description']/following-sibling::span/text()"))
            images = tree.xpath(
                '//div[@class="product__media media media--transparent gradient global-media-settings"]/img/@src')
            images = ['http:' + img if img.startswith('//') else img for img in images]

            useCase = None
            lead_time = \
                ''.join(tree.xpath('//div[@class="product_quote"]/preceding-sibling::p/text()')).split(
                    ":")[-1].strip().replace('***', '')
            price = float(id_['price'])
            collection = name.split()[0].lower()
            finish = None
            product_type = ''.join(tree.xpath('//div[@class="product_quote"]/p/text()')).strip()
            care = ''.join(tree.xpath(
                "//h3[contains(text(),' Care')]/parent::summary/following-sibling::div/p/text()")).strip()

            price_unit = 'per sqft'
            additionalData = {
                "priceUnit": price_unit,
                "raw_text": raw_text
            }
            variantGroup = data_for_color_and_variant_group.split('||')[0].lower().replace(' ', '-')
            color = data_for_color_and_variant_group.split('||')[1].lower().strip()
            material = ''.join(tree.xpath('//div[@class="product_quote"]/p/text()')).strip()
            subcategory = None
            material_list_one = ['Crossweave 100 Knot Count', 'Crossweave 120 Knot Count', '100 Persian Knot',
                                 '100 knot Tibetan Crossweave', '120 knot Tibetan Crossweave']
            material_list_two = ['Flatweave', 'Moroccan Weave', 'Moroccan', 'Moroccan / flatweave']
            material_list_three = ['Semi Shaggy Weave', 'Semi Shaggy Terrier Weave', 'Shaggy', 'Himalayan Shaggy Weave',
                                   'Semi Shaggy Lulu Weave']
            material_list_four = ['High Low Pile']
            material_list_five = ['Braided']
            material_list_six = ['Hooked']
            if material in material_list_one:
                subcategory = 'Hand-knotted'
            if material in material_list_two or 'Flatweave' in name or 'Moroccan Weave' in name:
                subcategory = 'Handloom'
            if material in material_list_three:
                subcategory = 'Shag'
            if material in material_list_four:
                subcategory = 'Machine-made'
            if material in material_list_five:
                subcategory = 'Braided'
            if material in material_list_six:
                subcategory = 'Hooked'
            specifications = {
                "dimensions": {'width': None,
                               'length': None,
                               'thickness': None,
                               'units': None},
                "composition": [{'material': material,
                                 'percentage': None}],
                "pattern": {
                    "type": product_type,
                    "repeatVertical": None,
                    "repeatHorizontal": None,
                    "match": None
                },
                "application": [],
                "performance": None,
                "care": care
            }
            Id_part = data_for_color_and_variant_group.strip().replace('||', '-').replace(' ', '-').lower()
            if material:
                Id = f'eskayel-{Id_part}-{material.replace("/", "-")}'.strip().replace(' ', '-')
            else:
                Id = f'eskayel-{Id_part}'.strip().replace(' ', '-')
            item = {"id": Id,
                    "name": full_name,
                    "vendor": 'Eskayel',
                    "category": 'Rugs',
                    "subcategory": subcategory,
                    "description": description,
                    "imageUrl": images,
                    "url": url,
                    "material": material,
                    "useCase": useCase,
                    "leadTime": lead_time,
                    "price": price,
                    "sustainability": [],
                    "certifications": [],
                    "documents": [],
                    "location": None,
                    "collection": collection,
                    "variantGroup": variantGroup,
                    "storedImagePath": None,
                    "color": color,
                    "finish": finish,
                    "tags": [],
                    "createdAt": await get_timestamp(),
                    "lastUpdated": await get_timestamp(),
                    "sourceRunId": await generate_source_run_id(),
                    "sourceType": "scraped",
                    "dataConfidence": "high",
                    "wasManuallyEdited": False,
                    "specifications": specifications,
                    "additionalData": additionalData}
            await Actor.push_data([item])
    else:
        variant_listing = f"{product_url}/products.json"
        json_response = await fetch_html(variant_listing)
        variant_json = json.loads(json_response)
        data_for_color_and_variant_group = variant_json['product']['title']
        name = ''.join(tree.xpath('//div[@class="product__title"]/h1//text()'))
        add_up_name = ''.join(tree.xpath('//div[@class="product_quote"]/preceding-sibling::p/text()')).split('LEAD')[
            0].strip().replace(':', '(').replace('”', 'in').replace("'", 'ft)').title().replace('In', 'in').replace(
            'Ft', 'ft').strip().replace('( ', '(')
        full_name = name + ' – ' + add_up_name
        description = ''.join(tree.xpath("//h3[text()='Description']/following-sibling::p//text()")).strip()
        if not description:
            description = ''.join(tree.xpath("//h3[text()='Description']/following-sibling::span/text()"))
        images = tree.xpath(
            '//div[@class="product__media media media--transparent gradient global-media-settings"]/img/@src')
        images = ['http:' + img if img.startswith('//') else img for img in images]
        url = product_url
        material = ''.join(tree.xpath("//h3[text()='Specs']/following-sibling::p/text()")).strip()
        material_match = re.search(r'-\s*MATERIAL:\s*(.+)', material, re.IGNORECASE)
        if material_match:
            material = material_match.group(1).strip().split(',')[0].strip().lower().replace('100% ', '')
        useCase = None
        lead_time = ''.join(tree.xpath('//div[@class="product_quote"]/preceding-sibling::p/text()')).split(":")[
            -1].strip()
        price = float(
            ''.join(tree.xpath('//span[@class="price-item price-item--regular"]/text()')).split('/')[0].replace(
                '$', ''))
        collection = name.split()[0].lower()

        finish = None
        try:
            width = float(
                ''.join(tree.xpath("//p[contains(text(),'MATERIAL')]/text()[3]")).split(':')[1].split('(')[
                    0].strip().split('x')[0].replace('” ', '').replace('" ', ''))
        except:
            width = ''
        if not width:
            width = None
        try:
            length = float(
                ''.join(tree.xpath("//p[contains(text(),'MATERIAL')]/text()[3]")).split(':')[1].split('(')[
                    0].strip().split('x')[1].replace("'", '').strip()) * 12
        except:
            length = ''
        if not length:
            length = None

        product_type = ''.join(tree.xpath('//div[@class="product_quote"]/p/text()')).strip()
        match = re.search(r'\bhalf[-\s]?drop\b', product_type, re.IGNORECASE)
        if match:
            match = match.group()
        try:
            pattern_vertical = float(
                ''.join(tree.xpath('//div[@class="product_quote"]/p/text()')).split('Vertical Repeat:')[
                    1].split('"')[0])
        except:
            pattern_vertical = ''
        if not pattern_vertical:
            pattern_vertical = None
        try:
            pattern_horizontal = float(
                ''.join(tree.xpath('//div[@class="product_quote"]/p/text()')).split('Horizontal Repeat')[
                    1].replace('"', ''))
        except:
            pattern_horizontal = ''
        if not pattern_horizontal:
            try:
                pattern_horizontal = float(
                    ''.join(tree.xpath('//div[@class="product_quote"]/p/text()')).split(':')[1].split('/')[
                        0].split()[-1].replace('"', ''))
            except:
                pattern_horizontal = ''
        if not pattern_horizontal:
            pattern_horizontal = None
        if 'Default Title' in material:
            material = None
        variantGroup = data_for_color_and_variant_group.split('||')[0].lower().replace(' ', '-')
        color = data_for_color_and_variant_group.split('||')[1].lower().strip()
        specifications = {
            "dimensions": {'width': width,
                           'length': length,
                           'thickness': None,
                           'units': 'in'},
            "composition": [{'material': material,
                             'percentage': None}],
            "pattern": {
                "type": match,
                "repeatVertical": pattern_vertical,
                "repeatHorizontal": pattern_horizontal,
                "match": None
            },
            "application": [],
            "performance": None,
            "care": None
        }

        additionalData = {
            "priceUnit": 'per roll',
            "raw_text": raw_text
        }
        Id_part = data_for_color_and_variant_group.strip().replace('||', '-').replace(' ', '-').lower()
        if material:
            Id = f'eskayel-{Id_part}-{material.replace("/", "-")}'.strip().replace(' ', '-')
        else:
            Id = f'eskayel-{Id_part}'.strip().replace(' ', '-')
        item = {"id": Id,
                "name": full_name,
                "vendor": 'Eskayel',
                "category": 'Wall Finishes',
                "subcategory": 'Wallpaper',
                "description": description,
                "imageUrl": images,
                "url": url,
                "material": material,
                "useCase": useCase,
                "leadTime": lead_time,
                "price": price,
                "sustainability": [],
                "certifications": ['Class A (ASTM E84)'],
                "documents": [],
                "location": None,
                "collection": collection,
                "variantGroup": variantGroup,
                "storedImagePath": None,
                "color": color,
                "finish": finish,
                "tags": [],
                "createdAt": await get_timestamp(),
                "lastUpdated": await get_timestamp(),
                "sourceRunId": await generate_source_run_id(),
                "sourceType": "scraped",
                "dataConfidence": "high",
                "wasManuallyEdited": False,
                "specifications": specifications,
                "additionalData": additionalData}
        await Actor.push_data([item])
