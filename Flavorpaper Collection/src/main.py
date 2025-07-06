from __future__ import annotations
from urllib.parse import urljoin
from lxml import html
from bs4 import BeautifulSoup
import gzip
import base64
from apify import Actor
from httpx import AsyncClient
from datetime import datetime


_run_context = {
    "counter": 0  # MUST be an integer, not None
}


async def generate_source_run_id():
    # Ensure counter is initialized
    if _run_context.get("counter") is None:
        _run_context["counter"] = 0

    _run_context["counter"] += 1
    return f"run-flavorpaper-{_run_context['counter']:03d}"


async def get_timestamp():
    return datetime.utcnow().isoformat() + "Z"


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
            'https://www.flavorpaper.com/collections/all-products'])

        if not start_urls:
            Actor.log.info('No start URLs specified in Actor input, exiting...')
            await Actor.exit()

        # Open the default request queue for handling URLs to be processed.

        # Enqueue the start URLs with an initial crawl depth of 0.
        for start_url in start_urls:
            async with AsyncClient() as client:
                try:
                    response = await client.get(start_url, follow_redirects=True)
                    tree = html.fromstring(response.text)
                    all_hits = tree.xpath('//div[@class="card-media"]/a/@href')
                    for hit in all_hits:
                        product_url = urljoin('https://www.flavorpaper.com/', hit)
                        await process_link_url(product_url)
                except Exception:
                    Actor.log.exception(f'Cannot extract data from {start_url}.')


async def process_link_url(product_url: str):
    response = await fetch_html(product_url)
    soup = BeautifulSoup(response, 'html.parser')
    visible_text = soup.get_text(strip=True)
    compressed = gzip.compress(visible_text.encode('utf-8'))
    raw_text = base64.b64encode(compressed).decode('utf-8')
    content = html.fromstring(response)
    variants = content.xpath('//input[@class="searchvariant"]')
    product_title = ' - '.join(content.xpath(
        '//div[@class="product__title"]/h1/following-sibling::p[1]/text() | //div[@class="product__title"]/h1/text()')).strip()
    variantGroup = product_title.split('-')[0].strip().replace(' ', '-').lower()
    Color = ''.join(content.xpath('//h1/following-sibling::p[1]/text()')).strip()
    try:
        repeatVertical = float(''.join(content.xpath(
            '//*[contains(text(),"Product Details")]/following-sibling::div//p/strong[contains(text(),"Vertical Repeat:")]/parent::p/text()')).strip().split(
            'in')[0].strip())
    except:
        repeatVertical = None
    try:
        Type = ''.join(content.xpath(
            '//*[contains(text(),"Product Details")]/following-sibling::div//p/strong[contains(text(),"Vertical Repeat:")]/parent::p/text()')).strip().split(
            'in')[1].strip().replace('Match', '').strip()
    except:
        Type = None
    if not Type:
        Type = None
    care = ''.join(content.xpath(
        '//*[contains(text(),"Product Details")]/following-sibling::div//p/strong[contains(text(),"Maintenance:")]/parent::p/text()')).strip()
    if not care:
        care = None
    fire_rating = ''.join(content.xpath(
        '//*[contains(text(),"Product Details")]/following-sibling::div//p/strong[contains(text(),"Fire Rating:")]/parent::p/text()')).strip()
    if not fire_rating:
        fire_rating = None
    description = ''.join(content.xpath("//div[contains(@id, 'main-description')]//text()")).strip()
    if not description:
        description = None
    Images = []
    images = content.xpath('//div[@class="thumbnails"]/img/@src')
    for image in images:
        image = f"https:{image}"
        Images.append(image)
    lead_time = ''.join(content.xpath(
        '//*[contains(text(),"Product Details")]/following-sibling::div//p/strong[contains(text(),"Lead Time:")]/parent::p/text()')).strip()
    if not lead_time:
        lead_time = None
    document_url = 'https:'+''.join(content.xpath(
        '//*[contains(text(),"Product Details")]/following-sibling::div//p/strong[contains(text(),"Installation Instructions:")]/following-sibling::a/@href')).strip()
    if not document_url:
        document_url = None
    for variant in variants:
        variant_title = ''.join(variant.xpath('./@data-title')).strip()
        variant_price = float(''.join(variant.xpath('./@data-price')).replace('$', '').replace(',', '').strip())
        variant_id = ''.join(variant.xpath('./@value')).strip()
        variant_url = urljoin(product_url, f'?variant={variant_id}')
        Id = f"flavorpaper-{product_title.replace(' - ', ' ').replace('!', '').replace('-', '').replace(' ', '-')}-{variant_title.split('-')[0].strip().replace(' / ', ' ').replace(' - ', ' ').replace(' ', '-')}".lower()
        name = f"{product_title} {variant_title.split('-')[0].strip()}"
        material = variant_title.split('/')[0].strip().split('-')[0].strip()
        documents = [{"title": "Installation Instructions",
                     "url": document_url,
                     "type": "install_guide"}]
        size = variant_title.split('/')[1].strip()
        priceUnit = None
        if 'sample' not in size.lower():
            if 'roll' in size.lower():
                priceUnit = 'per Roll'
            elif 'panel' in size.lower():
                priceUnit = 'per Panel'
        else:
            priceUnit = 'per Sample'
        pattern = {"type": Type,
                   "repeatVertical": repeatVertical,
                   "repeatHorizontal": None,
                   "match": Type}
        specifications = {"dimensions": None,
                          "pattern": pattern,
                          "application": None,
                          "performance": None,
                          "care": care,
                          "fireRating": fire_rating}
        additionalData = {"size": size,
                          "priceUnit": priceUnit,
                          "raw_text": raw_text}
        item = {
            "id": Id,
            "name": name,
            "vendor": "Flavor Paper",
            "category": "Wall Finishes",
            "subcategory": "Wallpaper",
            "description": description,
            "imageUrl": Images,
            "url": variant_url,
            "material": material,
            "useCase": None,
            "leadTime": lead_time,
            "price": variant_price,
            "sustainability": None,
            "certifications": [],
            "documents": documents,
            "location": None,
            "collection": None,
            "variantGroup": variantGroup,
            "storedImagePath": None,
            "color": Color,
            "finish": None,
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