"""Module defines the main entry point for the Apify Actor.

Feel free to modify this file to suit your specific needs.

To build Apify Actors, utilize the Apify SDK toolkit, read more at the official documentation:
https://docs.apify.com/sdk/python
"""

from __future__ import annotations
import re

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
    return f"run-flatvernacular-{_run_context['counter']:03d}"


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
            "https://flatvernacular.com/collections/wallpapers/wallpaper",
            "https://flatvernacular.com/collections/fabric"
        ])

        # Exit if no start URLs are provided.
        if not start_urls:
            Actor.log.info('No start URLs specified in Actor input, exiting...')
            await Actor.exit()

        # Enqueue the start URLs with an initial crawl depth of 0.
        for start_url in start_urls:
            page = 1
            while True:
                params = {
                    'page': f'{page}',
                }
                # Create an HTTPX client to fetch the HTML content of the URLs.
                async with AsyncClient() as client:
                    try:
                        # Fetch the HTTP response from the specified URL using HTTPX.
                        response = await client.get(start_url, follow_redirects=True, params=params)

                        tree = html.fromstring(response.text)

                        all_links = tree.xpath('//a[contains(@class, "title")]/@href')
                        if not all_links:
                            break
                        for link in all_links:
                            link_url = urljoin('https://flatvernacular.com/', link)

                            if link_url.startswith(('http://', 'https://')):
                                await process_link_url(link_url)
                        page += 1

                    except Exception:
                        Actor.log.exception(
                            f'Cannot extract data from {start_url} at page {page} at product url {link_url}.')


async def parse_composition(text):
    composition = []
    text = text.strip()

    # Case 1: (50% Cotton 50% Linen)
    paren_match = re.search(r'\(([\d%\w\s]+)\)', text)
    if paren_match:
        inside = paren_match.group(1)
        pairs = re.findall(r'(\d+)%\s*([A-Za-z\s]+)', inside)
        for perc, mat in pairs:
            composition.append({"material": mat.strip(), "percentage": int(perc)})
        return composition

    # Case 2: 50/50% or 50/50 Cotton/Linen
    match = re.search(r'(\d+/\d+)%?\s+([A-Za-z\s]+/[A-Za-z\s]+)', text)
    if match:
        perc_part, material_part = match.groups()
        perc_list = list(map(int, perc_part.split('/')))
        material_list = [m.strip() for m in material_part.split('/')]

        prefix_match = re.match(r'^([A-Za-z]+)', text)
        prefix = prefix_match.group(1) if prefix_match else ""

        if len(perc_list) == len(material_list):
            for perc, mat in zip(perc_list, material_list):
                full_name = f"{prefix} {mat}".strip() if prefix else mat
                composition.append({"material": full_name, "percentage": perc})
            return composition

    # Case 3: inline pairs like "50% Cotton 50% Linen"
    inline_pairs = re.findall(r'(\d+)%\s*([A-Za-z\s]+?)(?=\d+%|$)', text)
    if inline_pairs:
        for perc, mat in inline_pairs:
            composition.append({"material": mat.strip(), "percentage": int(perc)})
        return composition

    # Case 4: single 100% match somewhere in text (e.g., "Belgian 100% Natural Linen")
    match = re.search(r'(\d+)%\s+([A-Za-z\s]+)', text)
    if match:
        perc, mat = match.groups()

        # Try to find prefix before % (e.g., 'Belgian')
        prefix_match = re.match(r'^([A-Za-z]+)', text)
        prefix = prefix_match.group(1) if prefix_match else ""

        material = f"{prefix} {mat}".strip() if prefix and prefix not in mat else mat.strip()
        composition.append({"material": material, "percentage": int(perc)})
        return composition

    return composition


async def process_link_url(product_url: str):
    if 'fabric' in product_url:
        location = 'USA'
        content_html = await fetch_html(product_url)
        if not content_html:
            Actor.log.info(f"Response Not Found: {product_url}")
            return
        soup = BeautifulSoup(content_html, 'html.parser')
        visible_text = soup.get_text(strip=True)
        compressed = gzip.compress(visible_text.encode('utf-8'))
        raw_text = base64.b64encode(compressed).decode('utf-8')
        tree = html.fromstring(content_html)
        description = ''.join(tree.xpath('//meta[@property="og:description"]/@content')).strip().replace(' ',
                                                                                                         ' ').replace(
            '\n', ' ').strip()
        if 'natural fiber' in description.lower():
            subcategory = 'Natural Fiber'
        elif 'synthetic' in description.lower():
            subcategory = 'Synthetic'
        elif 'upholstery' in description.lower():
            subcategory = 'Upholstery'
        elif 'drapery' in description.lower():
            subcategory = 'Drapery'
        elif 'decorative' in description.lower():
            subcategory = 'Decorative'
        elif 'sheers' in description.lower():
            subcategory = 'Sheers'
        else:
            subcategory = 'Woven'
        product_json_text = ''.join(tree.xpath('//script[@id="bold-platform-data"]/text()')).strip()
        product_json_content = json.loads(product_json_text)
        product = product_json_content['product']
        product_description = product['description']
        vertical_pattern = r'<li>\s*Vertical\s+repeat\s*:\s*([\d\.]+["”])'
        horizontal_pattern = r'<li>\s*Horizontal\s+repeat\s*:\s*([\d\.]+["”])'
        try:
            vertical_value = float(
                re.search(vertical_pattern, product_description, re.IGNORECASE).group(1).replace('”', '').replace('"',
                                                                                                                  '').strip())
        except:
            vertical_value = None
        try:
            horizontal_value = float(
                re.search(horizontal_pattern, product_description, re.IGNORECASE).group(1).replace('”', '').replace('"',
                                                                                                                    '').strip())
        except:
            horizontal_value = None
        match_pattern = r'<li>\s*([A-Za-z]+)\s+([A-Za-z]+)\s+match'
        try:
            Match = re.search(match_pattern, product_description).group(0).replace('<li>', '').strip()
        except:
            Match = None
        try:
            leadTime = re.search(r'\b(\d+-\d+\s+weeks?)\b', product_description).group(1)
        except:
            leadTime = 'Made to order'
        images = product['images']
        imageUrl = []
        for image in images:
            image = f'https:{image}'
            imageUrl.append(image)
        product_title = product['title']
        tags = product['tags']
        variants = product['variants']
        all_size = []
        for variant in variants:
            Size = variant['option1']
            if 'x' not in Size:
                Size = variant['option2']
            Price = variant['price'] / 100
            variant_data = {"size": Size,
                            "price": Price,
                            "url": product_url}
            if variant_data not in all_size:
                all_size.append(variant_data)
        Id_base = product_title.lower().replace('fabric', '').replace(' - ', ' ').replace("'", '').strip().replace(' ',
                                                                                                                   '-')
        variantGroup = product_title.replace(' - ', ' ').lower().strip().replace(' ', '-')
        try:
            color = product_title.split('-')[1].replace('Fabric', '').strip()
        except:
            color = None
        if not color:
            color = None
        if color:
            variantGroup = variantGroup.replace(f"{color.lower().replace(' ', '-')}-", '')
        Material = None
        for variant in variants:
            variant_name = variant['title']
            price = variant['price'] / 100
            variant_id = variant['id']
            variant_size = variant['option1']
            if Material:
                variant_material = variant['option2']
                if variant_material:
                    if 'x' in variant_material:
                        variant_material = variant['option1']
                    if variant_material != Material:
                        Material = variant_material

            if not Material:
                variant_material = variant['option2']
                if 'perennial-wildflowers-fabric' in product_url or 'perseid' in product_url or 'nimbo-metis-fabric' in product_url or 'the-heavens' in product_url or 'swallowtail-sunstone-fabric' in product_url:
                    variant_material = 'Belgian 50/50% Cotton/Linen fabric'
                if 'monarch-flax-fabric' in product_url:
                    variant_material = 'Belgian 100% Natural Linen'
                if variant_material:
                    if 'x' in variant_material:
                        variant_material = variant['option1']
                        variant_size = variant['option2']
                if not variant_material:
                    escaped_input = re.escape(variant_size.replace('Large ', '').replace('”', '"'))
                    variant_pattern = rf'.*{escaped_input}.*'
                    try:
                        variant_material_text = re.search(variant_pattern, product_description).group(0)
                    except:
                        variant_material_text = None
                    if not variant_material_text:
                        variant_material_text = re.search(variant_pattern.replace('8', '6'), product_description).group(
                            0)
                    variant_material_text = BeautifulSoup(variant_material_text, 'html.parser').get_text(strip=True)
                    variant_material_text = \
                        variant_material_text.split(variant_size.replace('Large ', '').replace('”', '"'))[0].strip()
                    variant_material_text_escaped = re.escape(variant_material_text)
                    try:
                        variant_material = re.search(rf'\b\w+\s+{variant_material_text_escaped}',
                                                     product_description).group(0).strip()
                        if variant_material == 'div':
                            variant_material = None
                    except:
                        variant_material = None
                    if not variant_material:
                        variant_material_text = BeautifulSoup(variant_material_text, 'html.parser').get_text(strip=True)
                        variant_material_text = variant_material_text.split(
                            variant_size.replace('Large ', '').replace('”', '"').replace('8', '6'))[0].strip()
                        variant_material_text_escaped = re.escape(variant_material_text)
                        try:
                            variant_material = re.search(rf'\b\w+\s+{variant_material_text_escaped}',
                                                         product_description).group(0).strip()
                            if variant_material == 'div':
                                variant_material = None
                        except:
                            if not variant_material_text:
                                variant_material_text = None
                            variant_material = variant_material_text
                Material = variant_material
            if Material == variant_size:
                variant_size = variant['option2']
            if 'x' not in variant_size:
                variant_size = variant['option2']
                Material = variant['option1']
                variant_size = variant_size.replace(Material.replace('Fabric', ''), '').strip()
            if Material == 'div ':
                Material = None
            Id_base_variant_material = Material.lower().replace('/', ' ').replace('%', '').replace('(', '').replace(')',
                                                                                                                    '').strip().replace(
                ' ', '-')
            Id_base_variant_size = variant_size.lower().replace('(', '').replace(')', '').replace('”', '').replace('"',
                                                                                                                   '').replace(
                'wide ', '').replace('long', '').replace(' x ', 'x').strip().replace(' ', '-')
            Id = f'flatvernacular-{Id_base}-{Id_base_variant_material}-{Id_base_variant_size}'
            name = f"""{Id_base.title()} - {Material} - {variant_size.replace('”', '"')}"""
            variant_url = f'{product_url}?variant={variant_id}'
            width = float(
                variant_size.split('(')[1].split('x')[0].replace('”', '').replace('"', '').replace('wide', '').replace(
                    'long', '').strip())
            length = float(variant_size.split('x')[1].replace('”', '').replace('"', '').replace(')', '').replace('wide',
                                                                                                                 '').replace(
                'long', '').strip())
            dimensions = {"width": width,
                          "length": length,
                          "thickness": None,
                          "units": "in"}
            composition = await parse_composition(text=Material)
            pattern = {"type": None,
                       "repeatVertical": vertical_value,
                       "repeatHorizontal": horizontal_value,
                       "match": Match}
            specifications = {"dimensions": dimensions,
                              "composition": composition,
                              "pattern": pattern,
                              "application": None,
                              "performance": None,
                              "care": None}
            priceUnit = None
            if 'sample' in variant_name.lower():
                priceUnit = "per sample"
            if 'yard' in variant_name.lower():
                priceUnit = "per yard"
            if 'swatch' in variant_name.lower():
                priceUnit = "per swatch"
            if 'strike off' in variant_name.lower() or 'strike-off' in variant_name.lower():
                priceUnit = "per strike-off"

            additionalData = {"priceUnit": priceUnit,
                              "surface_treatment": None,
                              "all_sizes": all_size,
                              "raw_text": raw_text}
            item = {
                "id": Id,
                "name": name,
                "vendor": "Flat Vernacular",
                "category": "Textiles",
                "subcategory": subcategory,
                "description": description,
                "imageUrl": imageUrl,
                "url": variant_url,
                "material": Material,
                "useCase": None,
                "leadTime": leadTime,
                "price": price,
                "sustainability": None,
                "certifications": [],
                "documents": [],
                "location": location,
                "collection": None,
                "variantGroup": variantGroup,
                "storedImagePath": None,
                "color": color,
                "finish": None,
                "tags": tags,
                "createdAt": await get_timestamp(),
                "lastUpdated": await get_timestamp(),
                "sourceRunId": await generate_source_run_id(),
                "sourceType": "scraped",
                "dataConfidence": "high",
                "wasManuallyEdited": False,
                "specifications": specifications,
                "additionalData": additionalData
            }
            await Actor.push_data(item)
    else:
        content_html = await fetch_html(product_url)
        if not content_html:
            Actor.log.info(f"Response Not Found: {product_url}")
            return
        soup = BeautifulSoup(content_html, 'html.parser')
        visible_text = soup.get_text(strip=True)
        compressed = gzip.compress(visible_text.encode('utf-8'))
        raw_text = base64.b64encode(compressed).decode('utf-8')
        tree = html.fromstring(content_html)
        description = ''.join(tree.xpath('//meta[@property="og:description"]/@content')).strip().replace(' ',
                                                                                                         ' ').replace(
            '\n', ' ').strip()
        try:
            variant_json_string = ''.join(tree.xpath("//script[@id='bold-platform-data']/text()")).replace('\n',
                                                                                                           '').strip()
        except:
            variant_json_string = None
        if variant_json_string:
            variants = json.loads(variant_json_string)
            imageUrl = []
            images = variants['product']['images']
            for image in images:
                image = f'https:{image}'
                imageUrl.append(image)
            product_data = variants['product']
            product_name = product_data['title']
            try:
                color = product_name.split('-')[1].replace('Wallpaper', '').strip()
            except:
                color = None
            tags = product_data['tags']
            variants_data = product_data['variants']
            for variant in variants_data:
                try:
                    material = variant['option2'].strip()
                except:
                    material = None
                if not material:
                    material = ''.join(
                        tree.xpath('//div[@class="product-detail-accordion"]//details/summary/text()')).strip().title()
                if not material:
                    material = ''.join(tree.xpath('//li[contains(text(), "Material:")]/text()')).replace('Material:',
                                                                                                         '').strip()
                if 'Sample' in material or 'Yard' in material or 'Double Roll' in material:
                    material = variant['option1'].strip()
                variant_id = variant['id']
                variant_name = variant['title'].strip()
                try:
                    variant_size = variant_name.split('/')[1].strip()
                except:
                    variant_size = None
                if not variant_size:
                    variant_size = variant_name.split('(')[0].strip()
                if variant_size == material:
                    variant_size = variant['option1'].strip().split('(')[0].strip()
                    if 'sample' in variant_size.lower():
                        variant_size = "Sample"
                try:
                    price = float(''.join(tree.xpath(
                        """//li/span[contains(text(), '""" + variant_name + """')]/text()""")).strip().replace(
                        '' + variant_name + '', '').replace('$', '').replace(':', '').replace('-', '').strip())
                except:
                    price = None
                if not price:
                    try:
                        price = float(''.join(tree.xpath(
                            """//li/span[contains(text(), '""" + variant_name.replace(' long',
                                                                                      '') + """')]/text()""")).strip().replace(
                            '' + variant_name.replace(' long', '') + '', '').replace('$', '').replace(':', '').replace(
                            '-', '').strip())
                    except:
                        price = None
                if not price:
                    try:
                        price = float(''.join(tree.xpath(
                            '//*[contains(text(),"' + material.upper() + '")]/following-sibling::div//ul/li[contains(text(),"' + variant_size + '")]/text()')).split(
                            '-')[1].replace('$', '').strip())
                    except:
                        price = None
                if not price:
                    try:
                        price = float(''.join(tree.xpath('//*[contains(text(),"' + material.upper().replace('-',
                                                                                                            ' ') + '")]/following-sibling::div//ul/li[contains(text(),"' + variant_size + '")]/text()')).split(
                            '-')[1].replace('$', '').strip())
                    except:
                        price = None
                if not price:
                    price = float(variant['price']) / 100
                try:
                    variant_text = ''.join(tree.xpath(
                        '//*[contains(text(), "' + material.upper() + '")]/following-sibling::div//li[contains(text(), "' + variant_size + '")]/text()'))
                except:
                    variant_text = None
                if not variant_text:
                    try:
                        variant_text = ''.join(tree.xpath(
                            '//*[contains(text(), "' + material.upper().replace('-',
                                                                                ' ') + '")]/following-sibling::div//li[contains(text(), "' + variant_size + '")]/text()'))
                    except:
                        variant_text = None
                if not variant_text:
                    try:
                        variant_text = ''.join(tree.xpath('//*[contains(text(), "' + material.upper().replace('-',
                                                                                                              ' ') + '")]/following-sibling::div//li[contains(text(), "' + variant_size.replace(
                            '" x', '” wide x').replace('")', '” long)') + '")]/text()'))
                    except:
                        variant_text = None
                if not variant_text:
                    variant_text = ''.join(tree.xpath('//*[contains(text(), "' + material.upper().replace('-',
                                                                                                          ' ') + '")]/following-sibling::div//li[contains(text(), "' + variant_size.replace(
                        '"', '”') + '")]/text()'))
                try:
                    width = variant_name.split('/')[0].split('(')[1].split('x')[0].replace('"', '').strip().replace('wide',
                                                                                                            '').strip().split()[
                        0].replace('”', '').strip()
                except:
                    width = None
                if not width:
                    try:
                        width = variant_text.split('(')[1].split('”')[0].strip()
                    except:
                        width = None
                if not width:
                    try:
                        width = variant_name.split('(')[1].split('x')[0].replace('"', '').strip().replace('wide',
                                                                                                          '').strip().split()[
                            0].replace('”', '').strip()
                    except:
                        width = None
                try:
                    length = variant_name.split('/')[0].split('(')[1].split('x')[1].replace('"', '').replace(')',
                                                                                                             '').strip().replace(
                        'long', '').replace('”', '').strip()
                except:
                    length = None
                if not length:
                    try:
                        length = variant_text.split('x')[1].split('”')[0].split(')')[0].replace('long', '').strip()
                    except:
                        length = None
                if not length:
                    try:
                        length = variant_name.split('(')[1].split('x')[1].replace('"', '').replace(')',
                                                                                                   '').strip().replace(
                            'long', '').replace('”', '').strip()
                    except:
                        length = None
                if variant_size == 'Default Title':
                    variant_size = None
                if variant_size:
                    if material:
                        if not width or not length:
                            Id = f"""flatvernacular-{product_name.lower().replace('wallpaper', '').replace(' - ', ' ').strip().replace(' ', '-')}-{material.lower().replace('wallpaper', '').strip().replace(' ', '-')}-{variant_size.lower().replace(' ', '-')}"""
                            name = f"""{product_name} - {material.replace('Wallpaper', '').strip()} - {variant_size}"""
                        else:
                            Id = f"""flatvernacular-{product_name.lower().replace('wallpaper', '').replace(' - ', ' ').strip().replace(' ', '-')}-{material.lower().replace('wallpaper', '').strip().replace(' ', '-')}-{variant_size.lower().split('(')[0].strip().replace(' ', '-')}-{width.replace('yards', '').strip()}x{length.replace('yards', '').strip()}"""
                            name = f"""{product_name} - {material.replace('Wallpaper', '').strip()} - {variant_size.split('(')[0].strip()} ({width.replace('yards', '').strip()}" x {length.replace('yards', '').strip()}")"""
                    else:
                        if not width or not length:
                            Id = f"""flatvernacular-{product_name.lower().replace('wallpaper', '').replace(' - ', ' ').strip().replace(' ', '-')}-{variant_size.lower().replace(' ', '-')}"""
                            name = f"""{product_name} - {variant_size}"""
                        else:
                            Id = f"""flatvernacular-{product_name.lower().replace('wallpaper', '').replace(' - ', ' ').strip().replace(' ', '-')}-{variant_size.lower().replace(' ', '-')}-{width.replace('yards', '').strip()}x{length.replace('yards', '').strip()}"""
                            name = f"""{product_name} - {variant_size} ({width.replace('yards', '').strip()}" x {length.replace('yards', '').strip()}")"""
                else:
                    if material:
                        if not width or not length:
                            Id = f"""flatvernacular-{product_name.lower().replace('wallpaper', '').replace(' - ', ' ').strip().replace(' ', '-')}-{material.lower().replace('wallpaper', '').strip().replace(' ', '-')}"""
                            name = f"""{product_name} - {material.replace('Wallpaper', '').strip()}"""
                        else:
                            Id = f"""flatvernacular-{product_name.lower().replace('wallpaper', '').replace(' - ', ' ').strip().replace(' ', '-')}-{material.lower().replace('wallpaper', '').strip().replace(' ', '-')}-{width.replace('yards', '').strip()}x{length.replace('yards', '').strip()}"""
                            name = f"""{product_name} - {material.replace('Wallpaper', '').strip()} - ({width.replace('yards', '').strip()}" x {length.replace('yards', '').strip()}")"""
                    else:
                        if not width or not length:
                            Id = f"""flatvernacular-{product_name.lower().replace('wallpaper', '').replace(' - ', ' ').strip().replace(' ', '-')}"""
                            name = f"""{product_name}"""
                        else:
                            Id = f"""flatvernacular-{product_name.lower().replace('wallpaper', '').replace(' - ', ' ').strip().replace(' ', '-')}-{width.replace('yards', '').strip()}x{length.replace('yards', '').strip()}"""
                            name = f"""{product_name} ({width.replace('yards', '').strip()}" x {length.replace('yards', '').strip()}")"""
                if width:
                    if 'yards' in width:
                        width = round(float(width.replace('yards', '').strip()) * 36, 2)
                    else:
                        width = float(width)
                if length:
                    try:
                        if 'yards' in length:
                            length = round(float(length.replace('yards', '').strip()) * 36, 2)
                        else:
                            length = float(length)
                    except:
                        length = None
                variant_url = f'{product_url}?variant={variant_id}'
                variantGroup = product_name.lower().replace(' - ', ' ').strip().replace(' ', '-')
                dimensions = {"width": width,
                              "length": length,
                              "thickness": None,
                              "units": "in"}
                pattern_text = ' '.join(tree.xpath(
                    '//*[contains(text(), "' + material.upper() + '")]/following-sibling::div/div/ul/li//text()'))
                if not pattern_text:
                    pattern_text = ' '.join(tree.xpath('//*[contains(text(), "' + material.upper().replace('-',
                                                                                                           ' ') + '")]/following-sibling::div/div/ul/li//text()'))
                pattern_matches = re.search(
                    r'Horizontal Repeat:\s*([\d.]+["”]?)\s*\|\s*Vertical Repeat:\s*([\d.]+["”]?)', pattern_text)
                if not pattern_matches:
                    pattern_matches = re.search(
                        r'Horizontal repeat:\s*([\d.]+["”]?)\s*\|\s*Vertical repeat:\s*([\d.]+["”]?)', pattern_text)
                if not pattern_matches:
                    pattern_matches = re.search(
                        r'Horizontal\s+repeat[:\s]*([\d.]+["”]?)\s*\|\s*Vertical\s+repeat[:\s]*([\d.]+["”]?)',
                        pattern_text)
                if not pattern_matches:
                    pattern_matches = re.search(
                        r'Horizontal\s+Repeat[:\s]*([\d.]+["”]?)\s*\|\s*Vertical\s+Repeat[:\s]*([\d.]+["”]?)',
                        pattern_text)
                try:
                    horizontal_repeat = float(pattern_matches.group(1).replace('"', '').replace('”', '').strip())
                except:
                    horizontal_repeat = None
                if not horizontal_repeat:
                    try:
                        horizontal_repeat = float(
                            ''.join(tree.xpath("//li[contains(text(), 'Horizontal repeat:')]/text()")).replace(
                                'Horizontal repeat:', '').replace('"', '').replace('”', '').strip())
                    except:
                        horizontal_repeat = None
                if not horizontal_repeat:
                    try:
                        horizontal_repeat = float(
                            ''.join(tree.xpath("//li[contains(text(), 'Horizontal Repeat')]/text()")).replace(
                                'Horizontal Repeat', '').replace('"', '').replace('”', '').strip())
                    except:
                        horizontal_repeat = None
                try:
                    vertical_repeat = float(pattern_matches.group(2).replace('"', '').replace('”', '').strip())
                except:
                    vertical_repeat = None
                if not vertical_repeat:
                    try:
                        vertical_repeat = float(
                            ''.join(tree.xpath("//li[contains(text(), 'Vertical repeat:')]/text()")).replace(
                                'Vertical repeat:', '').replace('yards', '').replace('"', '').replace('”', '').strip())
                    except:
                        vertical_repeat = None
                if not vertical_repeat:
                    try:
                        vertical_repeat = float(
                            ''.join(tree.xpath("//li[contains(text(), 'Vertical repeat:')]/span/text()")).replace(
                                'Vertical repeat:', '').replace('yards', '').replace('"', '').replace('”', '').strip())
                    except:
                        vertical_repeat = None
                if not vertical_repeat:
                    try:
                        vertical_repeat = float(
                            ''.join(tree.xpath("//li[contains(text(), 'Vertical Repeat')]/text()")).replace(
                                'Vertical Repeat', '').replace('yards', '').replace('"', '').replace('”', '').strip())
                    except:
                        vertical_repeat = None
                try:
                    Match = re.search(r'Varied Match:\s*([A-Za-z\s]+Match(?:\s+or\s+[A-Za-z\s]+Match)?)', pattern_text,
                                      re.IGNORECASE).group(1)
                    if Match == 'Match':
                        Match = None
                except:
                    Match = None
                if not Match:
                    try:
                        Match = re.search(r'\b(\w+)\s+Horizontal', pattern_text).group(1)
                        if Match == 'Match':
                            Match = None
                    except:
                        Match = None
                if not Match:
                    try:
                        Match = re.search(r'(?:\d+\s+)?([\w\s():-]+Match.*?)(?=\s+Horizontal)', pattern_text).group(1)
                    except:
                        Match = None
                if not Match:
                    try:
                        Match_text = ' '.join(
                            tree.xpath('//div[@class="product-description rte"]/ul/li/text()')).strip()
                        Match = re.search(r'\b([A-Za-z]+ match)\b', Match_text, re.IGNORECASE).group(1)
                    except:
                        Match = None
                if not material:
                    try:
                        material_text = ' '.join(
                            tree.xpath('//div[@class="product-description rte"]/ul/li/text()')).strip()
                        material = re.search(r'\b([A-Za-z\-]+ material)\b', material_text, re.IGNORECASE).group(1)
                    except:
                        material = None
                pattern = {"type": None,
                           "repeatVertical": vertical_repeat,
                           "repeatHorizontal": horizontal_repeat,
                           "match": Match}
                specifications = {"dimensions": dimensions,
                                  "composition": [],
                                  "pattern": pattern,
                                  "application": None,
                                  "performance": None,
                                  "care": None}
                all_size_data = []
                try:
                    all_size_text = tree.xpath(
                        '//*[contains(text(), "' + material.upper() + '")]/following-sibling::div/div/ul/li/text()')
                except:
                    all_size_text = []
                for size_text in all_size_text:
                    if '$' in size_text:
                        Size = size_text.split('-')[0].replace(' wide', '').replace(' long', '').strip()
                        if '$' in Size:
                            Size = None
                        try:
                            Price = float(size_text.split('-')[1].replace('$', '').strip())
                        except:
                            Price = None
                        if Size and Price:
                            size_data = {"size": Size,
                                         "price": Price,
                                         "url": variant_url}
                            all_size_data.append(size_data)
                if not all_size_data:
                    all_size_text = tree.xpath('//div[@data-content-field="excerpt"]/ul[1]/li//text()')
                    for size_text in all_size_text:
                        if '$' in size_text:
                            Size = size_text.split('-')[0].split(':')[0].replace(' wide', '').replace(' long',
                                                                                                      '').strip()
                            try:
                                Price = float(size_text.split(':')[1].replace('$', '').strip())
                            except:
                                Price = None
                            if not Price:
                                try:
                                    Price = float(size_text.split('-')[1].replace('$', '').strip())
                                except:
                                    Price = None
                            if Size and Price:
                                size_data = {"size": Size,
                                             "price": Price,
                                             "url": variant_url}
                                all_size_data.append(size_data)
                if not all_size_data:
                    try:
                        size_list = tree.xpath('//select[@id="option-size"]/option/@value')
                    except:
                        size_list = []
                    for Size in size_list:
                        if Size:
                            size_data = {"size": Size,
                                         "price": None,
                                         "url": variant_url}
                            all_size_data.append(size_data)
                priceUnit = None
                if 'sample' in variant_name.lower():
                    priceUnit = "per sample"
                if 'yard' in variant_name.lower():
                    priceUnit = "per yard"
                if 'double roll' in variant_name.lower():
                    priceUnit = "per double roll"
                if 'single roll' in variant_name.lower():
                    priceUnit = "per roll"
                if 'panel' in variant_name.lower():
                    priceUnit = "per panel"
                if 'sheet' in variant_name.lower():
                    priceUnit = "per sheet"
                additionalData = {"priceUnit": priceUnit,
                                  "all_sizes": all_size_data,
                                  "raw_text": raw_text}
                if not material:
                    material = None
                if color:
                    variantGroup = variantGroup.replace(f"{color.lower().strip().replace(' ', '-')}-", '').replace(
                        f"-{color.lower().strip().replace(' ', '-')}", '')
                else:
                    color = None
                item = {"id": Id.lower(),
                        "name": name,
                        "vendor": "Flat Vernacular",
                        "category": "Wall Finishes",
                        "subcategory": "Wallpaper",
                        "description": description,
                        "imageUrl": imageUrl,
                        "url": variant_url,
                        "material": material,
                        "useCase": None,
                        "leadTime": None,
                        "price": price,
                        "sustainability": None,
                        "certifications": [],
                        "documents": [],
                        "location": None,
                        "collection": None,
                        "variantGroup": variantGroup,
                        "storedImagePath": None,
                        "color": color,
                        "finish": None,
                        "tags": tags,
                        "createdAt": await get_timestamp(),
                        "lastUpdated": await get_timestamp(),
                        "sourceRunId": await generate_source_run_id(),
                        "sourceType": "scraped",
                        "dataConfidence": "high",
                        "wasManuallyEdited": False,
                        "specifications": specifications,
                        "additionalData": additionalData}
                await Actor.push_data(item)
