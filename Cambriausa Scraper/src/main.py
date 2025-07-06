from __future__ import annotations

import base64
from urllib.parse import urljoin
import gzip
import re
from apify import Actor
from httpx import AsyncClient
import json
from datetime import datetime

_run_context = {
    "counter": 0  # MUST be an integer, not None
}


async def generate_source_run_id():
    # Ensure counter is initialized
    if _run_context.get("counter") is None:
        _run_context["counter"] = 0

    _run_context["counter"] += 1
    return f"run-cambriausa-{_run_context['counter']:03d}"


async def get_timestamp():
    return datetime.utcnow().isoformat() + "Z"


async def fetch_html(url: str, product_url: str) -> str:
    async with AsyncClient() as client:
        Actor.log.info(f"Fetching: {product_url}")
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
            'https://irupi58xna-1.algolianet.com/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.18.0)%3B%20Browser%20(lite)%3B%20instantsearch.js%20(4.56.3)%3B%20react%20(18.2.0)%3B%20react-instantsearch%20(6.45.0)%3B%20react-instantsearch-hooks%20(6.45.0)%3B%20JS%20Helper%20(3.13.5)&x-algolia-api-key=745df580b5ecbbba3dbf8c96ba93eb44&x-algolia-application-id=IRUPI58XNA'])

        if not start_urls:
            Actor.log.info('No start URLs specified in Actor input, exiting...')
            await Actor.exit()

        # Open the default request queue for handling URLs to be processed.

        # Enqueue the start URLs with an initial crawl depth of 0.
        all_urls = []
        for start_url in start_urls:
            page = 0
            while True:
                async with AsyncClient() as client:
                    try:
                        data = '{"requests":[{"indexName":"cusa-en-design-palette","params":"facets=%5B%22colorMerged.name%22%2C%22featuresMerged.name%22%2C%22designSeries.pricing%22%2C%22tags%22%2C%22hierarchicalCategories.lvl0%22%5D&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=18&maxValuesPerFacet=50&page=' + str(
                            page) + '&query=&tagFilters="}]}'
                        response = await client.post(start_url, data=data, follow_redirects=True)
                        tree = json.loads(response.text)
                        all_hits = tree['results'][0]['hits']
                        if not all_hits:
                            break

                        for hit in all_hits:
                            page_url = hit['pageurl']
                            product_url = urljoin('https://www.cambriausa.com/', page_url)
                            all_urls.append(product_url)
                        page += 1
                    except Exception:
                        Actor.log.exception(f'Cannot extract data from {start_url}.')
        for link in all_urls:
            if link.startswith(('http://', 'https://')):
                await process_link_url(link)


async def process_link_url(product_url: str):
    updated_url = f'https://www.cambriausa.com/graphql/execute.json/cusa/design-by-slug;slug={product_url.split("/")[-1]}'
    response_product = await fetch_html(updated_url, product_url)
    compressed = gzip.compress(response_product.encode('utf-8'))
    raw_text = base64.b64encode(compressed).decode('utf-8')
    json_data = json.loads(response_product)
    name = json_data['data']['designList']['items'][0]['designName']
    cleaned_name = re.sub(r'[^a-zA-Z0-9\s]', '', name)
    description = json_data['data']['designList']['items'][0]['description']['html']
    cleaned_description = re.sub(r'<[^>]+>', '', description)
    images = []
    img_no_one = json_data['data']['designList']['items'][0]['fullSlabImage']['_path']
    full_img_one_url = urljoin('https://www.cambriausa.com/', img_no_one)
    images.append(full_img_one_url)
    img_no_two = json_data['data']['designList']['items'][0]['slabDetailImage']['_path']
    full_img_two_url = urljoin('https://www.cambriausa.com/', img_no_two)
    images.append(full_img_two_url)
    images_link = json_data['data']['designList']['items'][0]['inspirationVideoAndImages']
    for img in images_link:
        img_link = img['_path']
        if '.jpg' in img_link:
            img_url = urljoin('https://www.cambriausa.com/', img_link)
            images.append(img_url)
    new_specification_pdf_path = None
    try:
        specification_pdf_path = json_data['data']['designList']['items'][0]['tearSheet']['_path']
    except:
        specification_pdf_path = None
    if specification_pdf_path:
        new_specification_pdf_path = urljoin('https://www.cambriausa.com/', specification_pdf_path)
    new_cad_bim_pdf = None
    try:
        cad_bim_pdf = f"{json_data['data']['designList']['items'][0]['cadbim']['_path']}.html"
    except:
        cad_bim_pdf = None
    if cad_bim_pdf:
        new_cad_bim_pdf = urljoin('https://www.cambriausa.com/', cad_bim_pdf)
    documents = [
        {'title': "Specifications (PDF)", 'url': new_specification_pdf_path, 'type': "spec_sheet"},
        {"title": "CAD/BIM files", "url": new_cad_bim_pdf, "type": "cad_bim"},
        {"title": "Slab image", "url": full_img_one_url, "type": "image"},
        {"title": "Detail image", "url": full_img_two_url, "type": "image"}
    ]
    collection = f"{json_data['data']['designList']['items'][0]['designSeries']['name']} Series"
    variantGroup = product_url.split("/")[-1]
    thickness_list = json_data['data']['designList']['items'][0]['thickness']
    finish_list = json_data['data']['designList']['items'][0]['finishes']
    width_length_data = json_data['data']['designList']['items'][0]['slabSize']['name'].split('(')[0]
    width = float(width_length_data.split('x')[0].replace('in', '').strip())
    length = float(width_length_data.split('x')[1].replace('in', '').strip())
    care = json_data['data']['designList']['items'][0]['productCareCopy'][0]['productCareDescription']['html']
    cleaned_care = re.sub(r'<[^>]+>', '', care)
    new_thickness_value = 0
    for finish in finish_list:
        finish_name = finish['name']
        cleaned__finish_name = re.sub(r'[^a-zA-Z0-9\s]', '', finish_name)
        updated_finish_name = cleaned__finish_name.split()[-1]
        for thick in thickness_list:
            thickness_value = thick['name']
            match = re.search(r'\d+', thickness_value)
            if match:
                new_thickness_value = int(match.group())
            thickness = round(await cm_to_inches(new_thickness_value), 2)
            dimensions = {"width": width, "length": length, "thickness": thickness, "units": "in"}
            specifications = {
                "dimensions": dimensions,
                "composition": [],
                "pattern": None,
                "application": None,
                "performance": None,
                "care": cleaned_care
            }
            additionalData = {
                "priceUnit": None,
                "raw_text": raw_text
            }
            full_name = f"{cleaned_name} — {cleaned__finish_name} ({thickness_value}, {width_length_data})".strip()
            Id = f'cambria-{cleaned_name.replace(" ", "-").lower()}-{updated_finish_name.lower().strip()}-{thickness_value.replace(" ", "-")}'.lower()
            item = {
                "id": Id,
                "name": full_name,
                "vendor": "Cambria",
                "category": "Stone Slabs",
                "subcategory": "Engineered Stone / Quartz",
                "description": cleaned_description,
                "imageUrl": images,
                "url": product_url,
                "material": "Quartz",
                "useCase": None,
                "leadTime": None,
                "price": None,
                "sustainability": None,
                "certifications": [],
                "documents": documents,
                "location": None,
                "collection": collection,
                "variantGroup": variantGroup,
                "storedImagePath": None,
                "color": None,
                "finish": updated_finish_name.lower(),
                "tags": [],
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


async def cm_to_inches(cm):
    return cm * 0.393701
