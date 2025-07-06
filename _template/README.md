# Vendor Scraper Template

This folder serves as a starting point for scraping a vendor's product catalog using Apify.

## Required files:
- `main.py` – the Python scraper logic
- `apify.json` – Apify metadata (memory, timeout, etc.)
- `requirements.txt` – Python dependencies
- `sample_output.json` – (add one!) example output following our schema

## Guidelines:
- Use the Apify SDK: `from apify import Actor`
- Push data with: `await Actor.push_data(data)`
- Output must match the structure in `/vendor_schema.md`

## Starting a New Vendor

1. Copy the `_template` folder to `/vendors/{vendor_name}/`
2. Update all files as needed for the new vendor.
3. Add a `sample_output.json` showing a real output example for this vendor.
4. See `the vendor brief and schema` for required fields and structure.
