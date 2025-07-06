from apify import Actor

async def main():
    async with Actor:
        Actor.log.info("Running vendor scraper...")


        result = [
            {
                "name": "Sample Product",
                "vendor": "Vendor Name",
                "category": "Tile",
                "subcategory": "Handpainted",
                "material": "Ceramic",
                "color": "Charcoal",
                "imageUrl": "https://example.com/image.jpg",
                "productPageUrl": "https://example.com/product",
                "price": "$10.00",
                "size": "4x4 in",
                "leadTime": "2-3 weeks"
                # Add more fields based on vendor_schema.md
            }
        ]

        await Actor.push_data(result)
