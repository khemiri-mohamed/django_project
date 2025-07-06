from __future__ import annotations
import re
from fractions import Fraction
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
from lxml import html
from datetime import datetime
from apify import Actor
from httpx import AsyncClient
import gzip
import base64

_run_context = {
    "counter": 0  # MUST be an integer, not None
}


async def generate_source_run_id():
    # Ensure counter is initialized
    if _run_context.get("counter") is None:
        _run_context["counter"] = 0

    _run_context["counter"] += 1
    return f"run-spinneybeck-{_run_context['counter']:03d}"


async def get_timestamp():
    return datetime.utcnow().isoformat() + "Z"


async def fetch_html(url: str) -> str:
    async with AsyncClient() as client:
        Actor.log.info(f"Fetching: {url}")
        response = await client.get(url, follow_redirects=False)
        if response.status_code == 200:
            return response.text


async def main() -> None:
    async with Actor:
        check_for_duplicate = []
        Actor.log.info('Hello from the Actor!')
        actor_input = await Actor.get_input() or {}
        start_urls = actor_input.get("url", [
            "https://www.spinneybeck.com/products/category/upholstery-leather",
            "https://www.spinneybeck.com/shop/product/belting-leather",
            "https://www.spinneybeck.com/products/category/flexible-wood",
            "https://www.spinneybeck.com/products/category/sould-eelgrass",
            "https://www.spinneybeck.com/products/category/softwood",
            "https://www.spinneybeck.com/products/category/wall-panels",
        ])
        if not start_urls:
            Actor.log.info('No start URLs specified in Actor input, exiting...')
            await Actor.exit()

        for start_url in start_urls:
            All_Link = []
            async with AsyncClient() as client:
                try:
                    response = await client.get(start_url, follow_redirects=True)
                    if start_url in All_Link:
                        break
                    tree = html.fromstring(response.text)
                    if 'belting-leather' in start_url:
                        if start_url not in check_for_duplicate:
                            check_for_duplicate.append(start_url)
                        else:
                            print('duplicate_link:-', start_url)
                            continue
                        print(start_url)
                        All_Link.append(start_url)

                    else:
                        all_links = tree.xpath(
                            '//body[@class="body-products"]//section[@class="l-index-items"]/a/@href')
                        if len(All_Link) == len(all_links):
                            break
                        for link in all_links:
                            link_url = urljoin("https://www.spinneybeck.com", link)
                            if link_url.startswith(('http://', 'https://')):
                                if link_url not in check_for_duplicate:
                                    check_for_duplicate.append(link_url)
                                else:
                                    print('duplicate_link:-', link_url)
                                    continue
                                print(link_url)
                                All_Link.append(link_url)
                except Exception:
                    Actor.log.exception(f'Cannot extract data from {start_url}.')
            for url in All_Link:
                await process_link_url(url, start_url)


async def process_link_url(product_url: str, start_url):
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
    colors = [c.strip() for c in tree.xpath('//div[@class="right-wrapper"]//select/option/text()') if c.strip()]
    if not colors:
        colors = [c.strip() for c in
                  tree.xpath("//h2[text()='Color Guide']/following-sibling::img/@data-tooltip-content") if c.strip()]
    for variant in colors:
        product_id = f'spinneybeck-{product_url.strip().split("/")[-1]}-{variant.lower()}'.replace(' ', '-')
        name = "".join(tree.xpath('//div[@class="product-description"]/h1/text()')).strip()
        full_name = f"""{name} - {start_url.split("/")[-1].replace(' - ', '').title()} - {variant}"""
        description = " ".join(
            tree.xpath('//div[@class="product-description__text-set js-product-description-set"]//p/text()')).strip()
        Image_urls = []
        selected_img_xpath = ''.join(tree.xpath(f'//img[@data-tooltip-content="{variant}"]/@src')).split('/')[-1]
        selected_img = f'https://www.spinneybeck.com/images/uploads/colors/swatches/{selected_img_xpath}'
        images = tree.xpath('//ul[@class="product-images__thumbs"]/li/img/@src')
        Image_urls.append(selected_img)
        image_urls = [urljoin("https://www.spinneybeck.com", img) for img in images]
        image_urls = [img.replace('_thumb', '_medium') for img in image_urls]
        for image in image_urls:
            Image_urls.append(image)
        material = ''.join(tree.xpath("//strong[contains(text(),'Content')]/following-sibling::text()")).strip()
        if not material:
            material = None
        usecase = ''.join(
            tree.xpath("//strong[contains(text(),'Primary Uses')]/following-sibling::text()")).replace(" ",
                                                                                                       '').strip().split(
            ',')
        if '' in usecase:
            usecase = []
        if not 'upholstery-leather':
            variantGroup = f'{product_url.strip("/").split("/")[-1]}-{"".join(variant).split("Plywood")[0]}'.lower().strip().replace(
                ' ', '-')
        else:
            variantGroup = product_url.split("/")[-1]
        if 'softwood' in start_url:
            variantGroup = f'{product_url.split("/")[-1]}-{variant.lower().replace(" ", "-")}'
        if 'belting-leather' in start_url:
            variantGroup = 'belting-leather-bl'
        # PDFs
        pdfs = []
        for a in tree.xpath('//div[@id="panel-4"]/ul/li/a'):
            text = a.xpath('string()').strip()
            href = a.get('href')
            if href:
                pdfs.append({"title": text, "url": urljoin("https://www.spinneybeck.com", href)})

        # Details
        details = {}
        li_items = tree.xpath('//div[@class="product-description__text-set js-product-description-set"]//ul/li')
        for li in li_items:
            strong = li.xpath('./strong/text()')
            full = li.xpath('string()').strip()
            if strong:
                key = strong[0].strip()
                val = full.replace(strong[0], '').strip()
                details[key] = val
            else:
                details["note"] = full
        try:
            lead_time = details['Lead Time']
        except:
            lead_time = None
        try:
            Environmental = details['Environmental']
        except:
            Environmental = None
        # Custom additions based on your uploaded format
        finish = None
        for key in details:
            if "finish" in key.lower():
                finish = details[key]
                break
        average = None
        try:
            thick_ness = details['Thickness']
            if '(' in thick_ness:
                try:
                    thick_ness = float(details['Thickness'].split(':')[-1].split()[0].strip())
                    average = thick_ness
                except:
                    try:
                        if '/' in thick_ness:
                            try:
                                thick_ness_v1 = float(details['Thickness'].split('/')[0])
                                thick_ness_v2 = float(details['Thickness'].split('/')[1].split()[0])
                                average = thick_ness_v1 / thick_ness_v2
                            except:
                                thick_ness_v1 = None
                                thick_ness_v2 = None
                        else:
                            average = None
                    except:
                        average = None
            else:
                thick_ness = details['Thickness'].replace('-', '').replace('mm', '').strip().split('–')
                float_values = [float(v) for v in thick_ness]
                # Calculate average
                average = sum(float_values) / len(float_values)
        except:
            average = None
        try:
            try:
                width_data = details['Panel Size'].replace('A, B:', '').split('x')[0].strip().split('-')
            except:
                width_data = details['Sheet Size'].split('x')[0].strip().split('-')
            inches = int(width_data[0].replace('’', ''))
            width_part = width_data[1].replace('”', '').strip()  # '10 3/4'# 10
            width_tokens = width_part.split()
            fraction = float(Fraction(width_tokens[1])) if len(width_tokens) > 1 else 0  # 3/4

            # width = float(width_values[0] * 12 + width_values[-1])
            width = float(inches) * 12 + int(width_tokens[0]) + fraction
            if 'softwood' in start_url:
                width = round(width, 2)
        except:
            width = None
        try:
            try:
                length_data = details['Panel Size'].split('x')[1].split('(')[0].strip().split('-')
            except:
                try:
                    length_data = details['Sheet Size'].split('x')[1].split('(')[0].strip().split('-')
                except:
                    length_data = None
            feet = int(length_data[0].replace('’', '').strip())  # 7

            # Extract inches and fraction
            inch_part = length_data[1].replace('”', '').strip()  # '10 3/4'
            inch_tokens = inch_part.split()

            inches = int(inch_tokens[0])  # 10
            fraction = float(Fraction(inch_tokens[1])) if len(inch_tokens) > 1 else 0  # 3/4

            # Total inches
            length = feet * 12 + inches + fraction
        except:
            length = None
        if width:
            width = round(width * 25.4, 2)
        if length:
            length = round(length * 25.4, 2)
        dimensions = {
            "width": width,
            "length": length,
            "thickness": average,
            "units": "mm"
        }
        try:
            comp_material = ''.join(details['Content']).strip()
        except:
            comp_material = material

        composition = [{'material': comp_material, 'percentage': None}]
        try:
            care = details['Maintenance']
        except:
            try:
                care = details['General Maintenance']
            except:
                care = None
        specifications = {
            "dimensions": dimensions,
            "composition": composition,
            "application": None,
            "performance": None,
            "care": care
        }
        if 'upholstery-leather' not in start_url or 'belting-leather' not in start_url:
            try:
                durability = details['Durability']
            except:
                durability = None
            performance = {
                "durability": durability,
                "abrasion": None,
                "slipResistant": None,
                "indoorOutdoor": None,
                "frostResistant": None,
                "waterAbsorption": None
            }

            specifications['performance'] = performance
        try:
            value = details["Hide Size"].strip().split()[0] if details.get("Hide Size") else None
            result = value if value and re.search(r'\d', value) else None
        except:
            result = None
        try:
            grain = details['Grain']
        except:
            grain = None
        try:
            grainTexture = details['Grain Texture']
        except:
            grainTexture = None
        try:
            tannage = details['Tannage']
        except:
            tannage = None
        try:
            dye = details["Dye"]
        except:
            dye = None
        if 'upholstery-leather' not in start_url or 'belting-leather' not in start_url:
            try:
                panel_opt = details['Panel Options'].strip().replace(" ", '').split(',')
            except:
                panel_opt = None
            try:
                wood_pattern = details['Wood Pattern']
            except:
                wood_pattern = None
            try:
                wood_area = details['Wood Open Area']
            except:
                wood_area = None
            try:
                nrc = float(details['Acoustics'].split('NRC – ')[-1].split(',')[0])
                saa = float(details['Acoustics'].split('SAA – ')[-1])
            except:
                nrc = None
                saa = None

            additionalData = {
                "panelOptions": panel_opt,
                "woodPattern": wood_pattern,
                "woodOpenArea": wood_area,
                "nrc": nrc,
                "saa": saa,
                "raw_text": raw_text
            }

        else:
            additionalData = {
                "hideSize_sqft": result if result else None,
                "grain": grain if grain else None,
                "grainTexture": grainTexture if grainTexture else None,
                "tannage": tannage if tannage else None,
                "dye": dye if dye else None,
                "raw_text": raw_text
            }
        if 'upholstery-leather' in start_url or 'belting-leather' in start_url:
            category = "Textiles"
            subcategory = 'Upholstery'

        else:
            category = "Wall Finishes"
            subcategory = 'Other Wall Treatments'

        item = {
            "id": product_id,
            "name": full_name,
            "vendor": "Spinneybeck",
            "category": category,
            "subcategory": subcategory,
            "description": description,
            "imageUrl": Image_urls,
            "url": product_url,
            "material": material,
            "useCase": usecase,
            "leadTime": lead_time,
            "price": None,
            "sustainability": Environmental,
            "certifications": None,
            "documents": pdfs,
            "location": "Italy",
            "collection": None,
            "variantGroup": variantGroup,
            "storedImagePath": None,
            "color": variant,
            "finish": finish,
            "tags": None,
            "createdAt": await get_timestamp(),
            "lastUpdated": await get_timestamp(),
            "sourceRunId": await generate_source_run_id(),
            "specifications": specifications,
            "additionalData": additionalData
        }
        await Actor.push_data(item)
