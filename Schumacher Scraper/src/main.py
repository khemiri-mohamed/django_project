"""Module defines the main entry point for the Apify Actor.

Feel free to modify this file to suit your specific needs.

To build Apify Actors, utilize the Apify SDK toolkit, read more at the official documentation:
https://docs.apify.com/sdk/python
"""

from __future__ import annotations

import re
import time

from lxml import html
from datetime import datetime
from apify import Actor
from httpx import AsyncClient
import json
import gzip
import base64

headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'authorization': '',
    'origin': 'https://schumacher.com',
    'priority': 'u=1, i',
    'referer': 'https://schumacher.com/',
    'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
    'x-domain': 'CATALOG',
    'x-tab-call-increment': '7',
    'x-version': '1.2',
}

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
    return f"run-schumacher-{_run_context['counter']:03d}"


async def fetch_html(url: str) -> str:
    while True:
        try:
            async with AsyncClient() as client:
                Actor.log.info(f"Fetching: {url}")
                response = await client.get(url, follow_redirects=False, timeout=30)
                if response.status_code == 200:
                    return response.text
        except:
            time.sleep(2)
            try:
                async with AsyncClient() as client:
                    Actor.log.info(f"Fetching: {url}")
                    response = await client.get(url, follow_redirects=False, timeout=30)
                    if response.status_code == 200:
                        return response.text
            except:
                pass


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
            "https://schumacher.com/catalog/1?gridSize=lg&_rv=false&_sg=3",
            "https://schumacher.com/catalog/2?gridSize=lg&_rv=false&_sg=3",
            "https://schumacher.com/catalog/8?gridSize=lg&_rv=false"
        ])

        # Exit if no start URLs are provided.
        if not start_urls:
            Actor.log.info('No start URLs specified in Actor input, exiting...')
            await Actor.exit()

        # Enqueue the start URLs with an initial crawl depth of 0.
        for start_url in start_urls:
            categoryId = start_url.split('?')[0].split('/')[-1].strip()
            if categoryId == "1":
                category = "Wall Finishes"
            elif categoryId == "2":
                category = "Fabrics"
            else:
                category = "Rugs"
            page = 0
            while True:
                params = {
                    'sort': [
                        'bestSellerSG,desc',
                        'itemNumber,desc',
                    ],
                    'size': '24',
                    'page': f'{page}',
                    'hasImage': 'true',
                    'skipAnalytics': 'false',
                    'categoryId': categoryId,
                    'gridSize': 'lg',
                    '_rv': 'false',
                }
                url = 'https://api.schumacher.com/catalog/entries'
                # Create an HTTPX client to fetch the HTML content of the URLs.
                async with AsyncClient() as client:
                    try:
                        # Fetch the HTTP response from the specified URL using HTTPX.
                        response = await client.get(url, follow_redirects=True, params=params, headers=headers,
                                                    timeout=30)

                        tree = json.loads(response.text)

                        all_content = tree['content']
                        if not all_content:
                            break
                        for content in all_content:
                            variations = content['variations']
                            for variation in variations:
                                itemNumber = variation['itemNumber']
                                link_url = f"https://schumacher.com/catalog/products/{itemNumber}"
                                if link_url.startswith(('http://', 'https://')):
                                    await process_link_url(link_url, category)
                        page += 1

                    except Exception:
                        Actor.log.exception(f'Cannot extract data from {start_url} at page {page} url {link_url}.')


async def parse_materials(material_str):
    parts = [part.strip() for part in material_str.split(',')]
    result = []

    for part in parts:
        match = re.match(r'(\d+)%\s*(.+)', part)
        if match:
            percentage = int(match.group(1))
            material = match.group(2).strip().capitalize()
            result.append({"material": material, "percentage": percentage})
        else:
            material = part.strip().capitalize()
            if material:  # Avoid empty strings
                result.append({"material": material, "percentage": None})

    return result


async def process_link_url(product_url: str, category: str):
    time.sleep(1)
    if category == "Rugs":
        content_html = await fetch_html(product_url)
        if not content_html:
            Actor.log.info(f"Response Not Found: {product_url}")
            return
        tree = html.fromstring(content_html)
        json_text = ''.join(tree.xpath('//script[@type="application/json"]/text()')).strip()
        compressed = gzip.compress(json_text.encode('utf-8'))
        raw_text = base64.b64encode(compressed).decode('utf-8')
        json_content = json.loads(json_text)
        ssrProduct = json_content['props']['pageProps']['ssrProduct']
        product_name = ssrProduct['name'].strip().title()
        variantGroup = product_name.lower().replace(' ', '-').replace('/', '-')
        colorName = ssrProduct['colorName'].strip().title()

        description = ssrProduct['description'].strip().lower()
        rules = [
            (r'hand[-\s]?knotted|knotted', 'Hand-knotted'),
            (r'hand[-\s]?tufted|tufted', 'Hand-tufted'),
            (r'flat\s?weave|flatweave|dhurrie|pit loom|handloom', 'Handloom'),
            (r'jacquard|power[-\s]?loom|machine made|wilton', 'Machine-made'),
            (r'braided', 'Braided'),
            (r'shag|shaggy|long[-\s]?pile', 'Shag'),
            (r'hooked', 'Hooked'),
        ]
        subCategory = None
        for pattern, subcategory in rules:
            if re.search(pattern, description):
                subCategory = subcategory

        if not subCategory:
            subCategory = 'Natural Fiber'

        # Check for natural fiber rule

        imageUrl = []
        images = ssrProduct['images']
        if images:
            for image in images:
                Image = image['largeUrl']
                imageUrl.append(Image)
        material = []
        country_value = None
        collection = []
        care = []
        attributes = ssrProduct['attributes']
        for attribute in attributes:
            attribute_name = attribute['name']
            if attribute_name == 'Content':
                attribute_values = attribute['value']
                for attribute_value in attribute_values:
                    content_value = attribute_value['value']
                    material.append(content_value)
            if attribute_name == 'Country of Finish':
                country_value = attribute['value'][0]['value'].title()
            if attribute_name == 'Collection':
                collection_values = attribute['value']
                for collection_value in collection_values:
                    collection_value = collection_value['value']
                    collection.append(collection_value)
            if attribute_name == 'Care':
                care_values = attribute['value']
                for care_value in care_values:
                    care_value = care_value['value']
                    care.append(care_value)
        material = ', '.join(material).title()
        collection = ', '.join(collection).title()
        care = ', '.join(care).title()
        if care == '0':
            care = None
        if not material:
            material = None
        if not care:
            care = None
        if not collection:
            collection = None

        if material:
            composition = await parse_materials(material)
        else:
            composition = []
        check_for_attribute = ssrProduct['relatedProducts'][0]['attributes']
        check_for_variant = None
        for attr in check_for_attribute:
            attr_name = attr['name']
            if attr_name == 'Size':
                check_for_variant = attr['value'][0]['value']
        if check_for_variant == 'Non-Standard':
            variants = ssrProduct['relatedProducts']
            for variant in variants:
                itemNumber = variant['itemNumber']
                url = f"https://schumacher.com/catalog/products/{itemNumber}"
                if isinstance(colorName, list):
                    Id = f"""schumacher-{product_name.lower().replace('/', '-').replace(' ', '-')}-{'-'.join(colorName).lower().replace(' & ', ' ').replace(' ', '-')}""".replace(
                        '/', '-')
                    name = f"""{product_name} - {' & '.join(colorName)}"""
                else:
                    Id = f"""schumacher-{product_name.lower().replace('/', '-').replace(' ', '-')}-{colorName.lower().replace(' & ', ' ').replace(' ', '-')}""".replace(
                        '/', '-')
                    name = f"""{product_name} - {colorName} """

                try:
                    length = None
                except:
                    length = None
                try:
                    width = None
                except:
                    width = None
                dimensions = {"width": length,
                              "length": width,
                              "thickness": None,
                              "units": "in"}
                pattern = {"type": None, "repeatVertical": None, "repeatHorizontal": None, "match": None}
                specifications = {"dimensions": dimensions,
                                  "composition": composition,
                                  "construction": [],
                                  "pattern": pattern,
                                  "application": None,
                                  "knotCount": None,
                                  "pileHeight": None,
                                  "edgeFinishing": None,
                                  "handmadeIn": None,
                                  "maxWidth": None,
                                  "maxLength": None,
                                  "colorVariation": None,
                                  "performance": None,
                                  "care": care}
                additionalData = {"raw_text": raw_text}
                if '&' in colorName:
                    colorName = colorName.replace('&', '').split()
                if str(type(colorName)) != "<class 'list'>":
                    colorName = colorName
                item = {
                    "id": Id,
                    "name": name,
                    "vendor": "Schumacher",
                    "category": category,
                    "subcategory": subCategory,
                    "description": description,
                    "imageUrl": imageUrl,
                    "url": url,
                    "material": material,
                    "useCase": None,
                    "leadTime": None,
                    "price": None,
                    "sustainability": None,
                    "certifications": [],
                    "documents": [],
                    "location": country_value,
                    "collection": collection,
                    "variantGroup": variantGroup,
                    "color": colorName,
                    "finish": None,
                    "tags": [],
                    "specifications": specifications,
                    "createdAt": await get_timestamp(),
                    "lastUpdated": await get_timestamp(),
                    "sourceRunId": await generate_source_run_id(),
                    "sourceType": "scraped",
                    "dataConfidence": "high",
                    "wasManuallyEdited": False,
                    "additionalData": additionalData
                }
                await Actor.push_data(item)
        else:
            variants = ssrProduct['relatedProducts']
            for variant in variants:
                itemNumber = variant['itemNumber']
                url = f"https://schumacher.com/catalog/products/{itemNumber}"
                relationshipType = variant['relationshipType']
                if relationshipType.upper() == 'SIZE_VARIATION':
                    value = variant['value']
                    if isinstance(colorName, list):
                        Id = f"""schumacher-{product_name.lower().replace('/', '-').replace(' ', '-')}-{'-'.join(colorName).lower().replace(' & ', ' ').replace(' ', '-')}-{value.lower().replace("'", '').replace(' x ', 'x').replace('"', '').replace(' ', '-')}""".replace(
                            '/', '')
                        name = f"""{product_name} - {' & '.join(colorName)} ({value.replace('"', '')})"""
                    else:
                        Id = f"""schumacher-{product_name.lower().replace('/', '-').replace(' ', '-')}-{colorName.lower().replace(' & ', ' ').replace(' ', '-')}-{value.lower().replace("'", '').replace(' x ', 'x').replace('"', '').replace(' ', '-')}""".replace(
                            '/', '')
                        name = f"""{product_name} - {colorName} ({value.replace('"', '')})"""

                    try:
                        length = round(float(value.split("x")[0].replace('"', '').strip().replace("'", '.')) * 12, 2)
                    except:
                        length = None
                    try:
                        width = round(float(value.split('"')[1].split('x')[1].strip()) * 12, 2)
                    except:
                        width = None
                    dimensions = {"width": length,
                                  "length": width,
                                  "thickness": None,
                                  "units": "in"}
                    pattern = {"type": None, "repeatVertical": None, "repeatHorizontal": None, "match": None}
                    specifications = {"dimensions": dimensions,
                                      "composition": composition,
                                      "construction": [],
                                      "pattern": pattern,
                                      "application": None,
                                      "knotCount": None,
                                      "pileHeight": None,
                                      "edgeFinishing": None,
                                      "handmadeIn": None,
                                      "maxWidth": None,
                                      "maxLength": None,
                                      "colorVariation": None,
                                      "performance": None,
                                      "care": care}
                    additionalData = {"raw_text": raw_text}
                    if '&' in colorName:
                        colorName = colorName.replace('&', '').split()
                    if str(type(colorName)) != "<class 'list'>":
                        colorName = colorName
                    item = {
                        "id": Id,
                        "name": name,
                        "vendor": "Schumacher",
                        "category": category,
                        "subcategory": subCategory,
                        "description": description,
                        "imageUrl": imageUrl,
                        "url": url,
                        "material": material,
                        "useCase": None,
                        "leadTime": None,
                        "price": None,
                        "sustainability": None,
                        "certifications": [],
                        "documents": [],
                        "location": country_value,
                        "collection": collection,
                        "variantGroup": variantGroup,
                        "color": colorName,
                        "finish": None,
                        "tags": [],
                        "specifications": specifications,
                        "createdAt": await get_timestamp(),
                        "lastUpdated": await get_timestamp(),
                        "sourceRunId": await generate_source_run_id(),
                        "sourceType": "scraped",
                        "dataConfidence": "high",
                        "wasManuallyEdited": False,
                        "additionalData": additionalData
                    }
                    await Actor.push_data(item)

    if category == "Wall Finishes" or category == "Fabrics":
        content_html = await fetch_html(product_url)
        if not content_html:
            Actor.log.info(f"Response Not Found: {product_url}")
            return
        tree = html.fromstring(content_html)
        json_text = ''.join(tree.xpath('//script[@type="application/json"]/text()')).strip()
        compressed = gzip.compress(json_text.encode('utf-8'))
        raw_text = base64.b64encode(compressed).decode('utf-8')
        json_content = json.loads(json_text)
        ssrProduct = json_content['props']['pageProps']['ssrProduct']
        try:
            product_name = ssrProduct['name'].strip().title()
        except:
            product_name = None
        if not product_name:
            return
        variantGroup = f"{product_name.lower().replace(' ', '-').replace('/', '-')}-{category.lower().replace(' ', '-')}"
        colorName = ssrProduct['colorName'].strip().title()
        try:
            description = ssrProduct['description'].strip()
        except:
            description = None
        imageUrl = []
        images = ssrProduct['images']
        if images:
            for image in images:
                Image = image['largeUrl']
                imageUrl.append(Image)
        material = []
        performance = []
        country_value = None
        width_value = None
        height_value = None
        Vertical_Repeat_value = None
        Horizontal_Repeat_value = None
        Match = None
        collection = []
        care = []
        attributes = ssrProduct['attributes']
        abrasion_value = None
        for attribute in attributes:
            attribute_name = attribute['name']
            if attribute_name == 'Substrate':
                attribute_values = attribute['value']
                for attribute_value in attribute_values:
                    content_value = attribute_value['value'].title()
                    material.append(content_value)
            if attribute_name == 'Country of Origin':
                country_value = attribute['value'][0]['value'].title()
            if attribute_name == 'Flame Test':
                performance_values = attribute['value']
                for performance_value in performance_values:
                    performance_value = performance_value['value'].title()
                    performance.append(performance_value)
            if attribute_name == 'Collection':
                collection_values = attribute['value']
                for collection_value in collection_values:
                    collection_value = collection_value['value'].title()
                    collection.append(collection_value)
            if attribute_name == 'Care':
                care_values = attribute['value']
                for care_value in care_values:
                    care_value = care_value['value'].title()
                    care.append(care_value)
            if attribute_name == 'Full Panel Set Width In':
                width_value = attribute['value'][0]['value'].title()

            if attribute_name == 'Yards Per Roll In':
                height_value = attribute['value'][0]['value'].title()
            if attribute_name == 'Vertical Repeat In':
                Vertical_Repeat_value = attribute['value'][0]['value'].title()
            if attribute_name == 'Horizontal Repeat In':
                Horizontal_Repeat_value = attribute['value'][0]['value'].title()
            if attribute_name == 'Match':
                Match = attribute['value'][0]['value'].title()
            if attribute_name == 'Abrasion':
                abrasion_value = attribute['value'][0]['value'].title()
        if abrasion_value:
            if category == "Wall Finishes":
                subCategory = "Wallpaper"
            else:
                rules = [
                    # 1. Sheers
                    (r'sheers\s*&\s*casements', 'Sheers'),
                    (r'\bsheer\b', 'Sheers'),
                    # 2. Upholstery
                    (r'\bupholstery\b', 'Upholstery'),
                    (r'\babrasion\b|martindale\b', 'Upholstery'),
                    (r'upholstery-weight|high performance|indoor/outdoor', 'Upholstery'),
                    # 3. Drapery
                    (r'\bcurtain\b', 'Drapery'),
                    (r'\bdrapery\b', 'Drapery'),
                    # 4. Decorative
                    (
                        r'embroideries|embroidered |crewel|braids & tapes|cut velvet|velvets|épinglé|matelassé|moiré|specialty',
                        'Decorative'),
                ]
                subCategory = None
                for pattern, Category in rules:
                    if re.search(pattern, description.lower()) or re.search(pattern, abrasion_value.lower().split()[0]):
                        subCategory = Category
                        break
                    else:
                        subCategory = "Woven"
        else:
            if category == "Wall Finishes":
                subCategory = "Wallpaper"
            else:
                rules = [
                    # 1. Sheers
                    (r'sheers\s*&\s*casements', 'Sheers'),
                    (r'\bsheer\b', 'Sheers'),
                    # 2. Upholstery
                    (r'\bupholstery\b', 'Upholstery'),
                    (r'\babrasion\b|martindale\b', 'Upholstery'),
                    (r'upholstery-weight|high performance|indoor/outdoor', 'Upholstery'),
                    # 3. Drapery
                    (r'\bcurtain\b', 'Drapery'),
                    (r'\bdrapery\b', 'Drapery'),
                    # 4. Decorative
                    (
                        r'embroideries|embroidered |crewel|braids & tapes|cut velvet|velvets|épinglé|matelassé|moiré|specialty',
                        'Decorative'),
                ]
                if description:
                    subCategory = None
                    for pattern, Category in rules:
                        if re.search(pattern, description.lower()):
                            subCategory = Category
                            break
                        else:
                            subCategory = "Woven"
                else:
                    subCategory = None

        material = ', '.join(material)
        collection = ', '.join(collection)
        care = ', '.join(care)
        if care == '0':
            care = None
        if not material:
            material = None
        if not care:
            care = None
        if not collection:
            collection = None

        if material:
            composition = await parse_materials(material)
        else:
            composition = []
        if width_value:
            width = float(width_value.replace('"', '').strip())
        else:
            width = None
        if height_value:
            height = float(height_value.replace('"', '').strip())
        else:
            height = None
        if Vertical_Repeat_value:
            try:
                Vertical_Repeat = float(Vertical_Repeat_value.replace('"', '').replace('cm', '').replace('Cm', ''))
            except:
                Vertical_Repeat = Vertical_Repeat_value
        else:
            Vertical_Repeat = None
        if Horizontal_Repeat_value:
            try:
                Horizontal_Repeat = float(Horizontal_Repeat_value.replace('"', '').replace('cm', '').replace('Cm', ''))
            except:
                Horizontal_Repeat = Horizontal_Repeat_value
        else:
            Horizontal_Repeat = None
        dimensions = {"width": width,
                      "length": height,
                      "thickness": None,
                      "units": "in"}
        pattern = {"type": None, "repeatVertical": Vertical_Repeat, "repeatHorizontal": Horizontal_Repeat,
                   "match": Match}
        specifications = {"dimensions": dimensions,
                          "composition": composition,
                          "construction": [],
                          "pattern": pattern,
                          "application": None,
                          "knotCount": None,
                          "pileHeight": None,
                          "edgeFinishing": None,
                          "handmadeIn": None,
                          "maxWidth": None,
                          "maxLength": None,
                          "colorVariation": None,
                          "performance": performance,
                          "care": care}
        additionalData = {"raw_text": raw_text}
        if '&' in colorName:
            colorName = colorName.replace('&', '').split()
        if str(type(colorName)) != "<class 'list'>":
            colorName = colorName
        if isinstance(colorName, list):
            Id = f"schumacher-{product_name.lower().replace(' ', '-')}-{'-'.join(colorName).lower().replace(' & ', ' ').replace(' ', '-')}-{category.lower().replace(' ', '-')}"
            name = f'{product_name} - {" & ".join(colorName)} - {category}'
        else:
            Id = f"schumacher-{product_name.lower().replace(' ', '-')}-{colorName.lower().replace(' & ', ' ').replace(' ', '-')}-{category.lower().replace(' ', '-')}"
            name = f'{product_name} - {colorName} - {category}'
        if category == "Fabrics":
            category = 'Textiles'
            if isinstance(colorName, list):
                Id = f"schumacher-{product_name.lower().replace(' ', '-')}-{'-'.join(colorName).lower().replace(' & ', ' ').replace(' ', '-')}-{category.lower().replace(' ', '-')}"
                name = f'{product_name} - {" & ".join(colorName)} - {category}'
            else:
                Id = f"schumacher-{product_name.lower().replace(' ', '-')}-{colorName.lower().replace(' & ', ' ').replace(' ', '-')}-{category.lower().replace(' ', '-')}"
                name = f'{product_name} - {colorName} - {category}'
        item = {
            "id": Id,
            "name": name,
            "vendor": "Schumacher",
            "category": category,
            "subcategory": subCategory,
            "description": description,
            "imageUrl": imageUrl,
            "url": product_url,
            "material": material,
            "useCase": None,
            "leadTime": None,
            "price": None,
            "sustainability": None,
            "certifications": [],
            "documents": [],
            "location": country_value,
            "collection": collection,
            "variantGroup": variantGroup,
            "color": colorName,
            "finish": None,
            "tags": [],
            "createdAt": await get_timestamp(),
            "lastUpdated": await get_timestamp(),
            "sourceRunId": await generate_source_run_id(),
            "specifications": specifications,
            "sourceType": "scraped",
            "dataConfidence": "high",
            "wasManuallyEdited": False,
            "additionalData": additionalData
        }

        await Actor.push_data(item)
