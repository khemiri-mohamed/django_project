"""Module defines the main entry point for the Apify Actor.

Feel free to modify this file to suit your specific needs.

To build Apify Actors, utilize the Apify SDK toolkit, read more at the official documentation:
https://docs.apify.com/sdk/python
"""

from __future__ import annotations
from bs4 import BeautifulSoup
import gzip
import base64
import asyncio
import json
import time
from urllib.parse import urljoin
from lxml import html
from apify import Actor
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from httpx import AsyncClient
from word2number import w2n
import re
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
    return f"run-backdrophome-{_run_context['counter']:03d}"


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
            'https://www.backdrophome.com/collections/wallcoverings/',
            'https://www.backdrophome.com/collections/paint/interior/',
            'https://www.backdrophome.com/collections/paint/exterior/',
            'https://www.backdrophome.com/collections/paint/interior/cabinet-door/',
        ])

        # Exit if no start URLs are provided.
        if not start_urls:
            Actor.log.info('No start URLs specified in actor input, exiting...')
            await Actor.exit()

        # Enqueue the start URLs with an initial crawl depth of 0.
        for start_url in start_urls:
            Actor.log.info(f'Enqueuing {start_url} ...')

            # Launch a new Selenium Chrome WebDriver and configure it.
            Actor.log.info('Launching Chrome WebDriver...')
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            driver = webdriver.Chrome(options=chrome_options)
            Actor.log.info("Chrome WebDriver initialized successfully")
            # Test WebDriver setup by navigating to an example page.
            driver.get('http://www.example.com')
            if driver.title != 'Example Domain':
                raise ValueError('Failed to open example page.')

            try:
                # Navigate to the URL using Selenium WebDriver. Use asyncio.to_thread
                # for non-blocking execution.
                await asyncio.to_thread(driver.get, start_url)

                time.sleep(4)
                all_links = []
                for link in driver.find_elements(By.XPATH,
                                                 "//div[contains(@class, 'image-container')]/a | //a[@class='SmartLink__StyledLink-sc-1go449t-0 kIucHj PatternDisplay__NoUnderlineLink-sc-j11yp8-1 bxheFq']"):
                    link_href = link.get_attribute('href')
                    link_url = urljoin(start_url, link_href)

                    if link_url.startswith(('http://', 'https://')):

                        if link_url not in all_links:
                            all_links.append(link_url)
                for Link in all_links:
                    Actor.log.info(f'Scraping {Link} ...')
                    await asyncio.to_thread(driver.get, Link)

                    time.sleep(5)
                    page_source = driver.page_source
                    # Extract the desired data.
                    await get_details(page_source, Link, start_url)

            except Exception:
                Actor.log.exception(f'Cannot extract data from {Link}.')

        driver.quit()


async def get_details(page_source, url, link):
    tree = html.fromstring(page_source)
    soup = BeautifulSoup(page_source, 'html.parser')
    visible_text = soup.get_text(strip=True)
    compressed = gzip.compress(visible_text.encode('utf-8'))
    raw_text = base64.b64encode(compressed).decode('utf-8')
    base_url = url.split('/')[-2].strip()
    product_url = f'https://www.backdrophome.com/page-data/products/{base_url}/page-data.json'
    try:
        json_response = await fetch_html(product_url)
    except:
        json_response = None
    if not json_response:
        retry = 1
        while True:
            time.sleep(2)
            if retry > 2:
                break
            try:
                json_response = await fetch_html(product_url)
            except:
                json_response = None
            if json_response:
                break
            retry += 1
    if json_response:
        json_content = json.loads(json_response)
        tags = json_content['result']['data']['product']['tags']
        description = json_content['result']['data']['productGroup']['description']
        variants = json_content['result']['data']['product']['variants']
        subCategory = ''.join(tree.xpath('//a[@aria-current="page"]/parent::span/parent::div/span[1]/a/text()')).strip()
        id_ = f'backdrophome-{url.split("/")[-2]}'
        Coverage = ''.join(tree.xpath(
            '//*[contains(text(),"Coverage:")]/parent::div/parent::div/parent::td/following-sibling::td//text()')).strip()
        Sheen = ''.join(tree.xpath(
            '//*[contains(text(),"Sheen:")]/parent::div/parent::div/parent::td/following-sibling::td//text()')).strip()
        finish = Sheen.split('SHEEN')[0].strip()
        if 'sheen' in finish:
            finish = None

        Features = ''.join(tree.xpath(
            '//*[contains(text(),"Features:")]/parent::div/parent::div/parent::td/following-sibling::td//text()')).strip()
        Paint_Type = ''.join(tree.xpath(
            '//*[contains(text(),"Paint Type:")]/parent::div/parent::div/parent::td/following-sibling::td//text()')).strip().title()
        name = json_content['result']['data']['product']['title']
        productType = json_content['result']['data']['product']['productType']
        color = json_content['result']['data']['product']['description']
        certifications = ['Climate Neutral Certified']
        match = re.search(r'[^,]*CERTIFIED', Features)
        certified_value = match.group().strip().title() if match else None
        if certified_value:
            certifications.append(certified_value)
        for variant in variants:
            variant_price = variant['price']
            variant_url = url + f"?variant={variant['shopifyId'].split('/')[-1].strip()}"
            try:
                variant_image = tree.xpath('(//div[@class="swiper-wrapper"])[1]/div//img/@data-src')
            except:
                variant_image = []
            if not variant_image:
                variant_image = tree.xpath(
                    "//div[@class='StyledBox-sc-13pk1d4-0 cbapkj wallcoverings-hero-image-container']/img/@data-src")
            if not variant_image:
                try:
                    variant_image = tree.xpath("//div[contains(@class, 'image-container')]/img/@src")
                except:
                    variant_image = []
            variant_title = variant['title']
            variant_size = variant_title.split(' / ')[1].split('-')[0].strip().replace('"', '').lower()
            dimensions = {
                "width": None,
                "length": None,
                "thickness": None,
                "units": "in"
            }
            pattern = {
                "type": None,
                "repeatHorizontal": None,
                "repeatVertical": None,
                "match": None
            }
            price_Unit = None
            if 'sample' in variant_title.lower():
                price_Unit = 'per sample'
            if 'gallon ' in variant_title.lower() or 'gallon' in variant_title.lower():
                price_Unit = 'per gallon'
            if 'roll ' in variant_title.lower() or 'roll' in variant_title.lower():
                price_Unit = 'per roll'
            additionalData = {"priceUnit": price_Unit,
                              "panelCount": None,
                              "pricedByTheYard": None,
                              "coverage": Coverage,
                              "raw_text": raw_text
                              }
            if price_Unit != 'per gallon':
                additionalData['coverage'] = None
            specifications = {
                "application": None,
                "performance": None,
                "dimensions": dimensions,
                "pattern": pattern,
                "care": None
            }
            Id = f'{id_}-{variant_size.replace(" ", "-").lower()}'.replace('"', '').replace("'", '')
            collection = f'{productType} {subCategory}'
            variantGroup = name.replace(' ', '-').lower()
            if not Paint_Type:
                Paint_Type = 'Non-woven paper'
            if not finish:
                finish = None
            item = {"id": Id,
                    "name": name,
                    "vendor": "Backdrop Home",
                    "category": "Wall Finishes",
                    "subcategory": subCategory,
                    "description": description,
                    "imageUrl": variant_image,
                    "url": variant_url,
                    "material": Paint_Type,
                    "useCase": None,
                    "leadTime": None,
                    "price": variant_price,
                    "sustainability": "Climate Neutral Certified",
                    "certifications": certifications,
                    "documents": [],
                    "location": "USA",
                    "collection": collection,
                    "variantGroup": variantGroup,
                    "storedImagePath": None,
                    "color": color,
                    "finish": finish,
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
            if 'wallcoverings' in link:
                collection = json_content['result']['data']['product']['seo']['title'].split('-')[0].strip()
                variantGroup = name.replace(' - ', '-').replace(' ', '-').lower()
                color = name.split('-')[-1].strip().title()
                item['collection'] = collection
                item['variantGroup'] = variantGroup
                item['color'] = color
                additionalData['pricedByTheYard'] = ''.join(
                    tree.xpath("//span[contains(text(),'PRICED BY THE YARD:')]/following-sibling::text() | //span[contains(text(),'PRICED BY THE PANEL:')]/following-sibling::text()")).strip()
                try:
                    width = float(''.join(tree.xpath(
                        "//span[contains(text(),'HORZ. REPEAT:')]/following-sibling::text()[1]")).strip().replace('"',
                                                                                                                  ''))
                except:
                    width = None
                try:
                    length = float(''.join(tree.xpath(
                        "//span[contains(text(),'VERT. REPEAT:')]/following-sibling::text()[1]")).strip().replace('"',
                                                                                                                  ''))
                except:
                    length = None
                try:
                    match = ''.join(
                        tree.xpath("//span[contains(text(),'MATCH:')]/following-sibling::text()[1]")).strip().title()
                except:
                    match = None
                try:
                    care = ''.join(
                        tree.xpath(
                            "//span[contains(text(),'CARE INSTRUCTIONS:')]/following-sibling::text()[1]")).strip()
                except:
                    care = None
                if not care:
                    care = None
                try:
                    match_digit = re.search(r'\b\d+\b', additionalData['pricedByTheYard'])
                except:
                    match_digit = None
                if not match_digit:
                    # Try to extract word-based number from text
                    try:
                        # Extract possible number word phrase using regex (basic word range)
                        match_word = re.search(
                            r'\b(?:zero|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty(?:[- ]one|[- ]two|[- ]three)?)\b',
                            additionalData['pricedByTheYard'], re.IGNORECASE)
                        if match_word:
                            additionalData['panelCount'] = w2n.word_to_num(match_word.group())
                    except:
                        additionalData['panelCount'] = None
                else:
                    try:
                        panel_count = int(match_digit.group())
                    except:
                        panel_count = None
                    if panel_count:
                        additionalData['panelCount'] = panel_count
                if 'panel' in additionalData['pricedByTheYard'].lower():
                    item['subcategory'] = 'Wall Mural'
                else:
                    item['subcategory'] = 'Wallpaper'
                specifications['dimensions']['width'] = width
                specifications['dimensions']['length'] = length
                specifications['pattern']['type'] = match
                specifications['pattern']['repeatHorizontal'] = width
                specifications['pattern']['repeatVertical'] = length
                specifications['care'] = care
            await Actor.push_data(item)