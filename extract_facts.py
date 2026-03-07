#!/usr/bin/env python3
"""Extract key info from fund fact sheet PDFs and generate fund_facts.js"""

import os
import re
import json
import pdfplumber

FACT_DIR = '/Users/naizhong/Documents/project/fund_price/fact_sheet/'
OUTPUT_FILE = '/Users/naizhong/Documents/project/fund_price/fund_facts.js'


def extract_text(pdf_path):
    """Extract all text from PDF."""
    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
    return '\n\n'.join(pages)


def extract_fund_size(text):
    """Extract fund size / net assets / AUM."""
    patterns = [
        r'(?:Fund [Ss]ize|Net Assets|AUM)[:\s]*(?:\([^)]*\)\s*)?(?:US\s*\$|USD|SGD|EUR|GBP|JPY)[\s]*([0-9,]+\.?\d*)\s*(Million|Billion|m|bn|M|B)',
        r'(?:Fund [Ss]ize|Net Assets|AUM)[:\s]*(?:\([^)]*\)\s*)?(US\s*\$|USD|SGD|EUR|GBP|JPY)[\s]*([0-9,]+\.?\d*)\s*(Million|Billion|m|bn|M|B)',
        r'(?:Fund [Ss]ize|Net Assets|AUM)[:\s]*(?:\([^)]*\)\s*)?(US\$|USD|SGD|EUR|GBP|JPY)([0-9,]+\.?\d*)(m|bn|M|B)',
        r'Net Assets\s+(?:US\s*\$|USD|SGD|EUR|GBP)[\s]*\$?([0-9,]+\.?\d*)\s*(Million|Billion|m)',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(0).strip()
    return None


def extract_inception_date(text):
    """Extract fund inception / launch date."""
    patterns = [
        r'(?:Fund\s+)?(?:Inception|Launch)\s*(?:Date)?[:\s]*(\d{1,2}[./]\d{1,2}[./]\d{2,4})',
        r'(?:Fund\s+)?(?:Inception|Launch)\s*(?:Date)?[:\s]*(\d{1,2}\s+\w+[,\s]+\d{4})',
        r'(?:Fund\s+)?(?:Inception|Launch)\s*(?:Date)?[:\s]*(\w+\s+\d{1,2}[,\s]+\d{4})',
        r'(?:Fund\s+)?(?:Inception|launch)\s+date[:\s]*(\d{2}\.\d{2}\.\d{4})',
        r'(?:Fund\s+)?[Ii]nception[:\s]*(\d{1,2}/\d{1,2}/\d{4})',
        r'Fund\s+Inception\s+Date[:\s]*(?:Class\s+\w+\s+)?(\d{1,2}\s+\w+\s+\d{4})',
        r'Inception\s+Date[:\s]*(\d{1,2}\s+\w+[,]?\s+\d{4})',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None


def extract_fund_manager(text):
    """Extract fund manager name(s)."""
    patterns = [
        r'(?:Fund\s+[Mm]anager|Portfolio\s+Manager|Portfolio\s+Management)[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+(?:,\s*(?:CFA|PhD|MBA))?(?:\s*(?:,|;|&|and)\s*[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+(?:,\s*(?:CFA|PhD|MBA))?)*)',
        r'(?:Fund\s+[Mm]anager)[:\s]+([^\n]+)',
        r'(?:Portfolio\s+Management\s*(?:&\s*Experience)?)\s*\+?\s*([A-Z][a-z]+[^+\n]*)',
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            result = m.group(1).strip()
            # Clean up
            result = re.sub(r'\s*Fund management.*$', '', result, flags=re.IGNORECASE)
            result = re.sub(r'\s*(?:Schroder|Fidelity|BlackRock|JPMorgan).*$', '', result)
            if len(result) > 100:
                result = result[:100]
            return result if len(result) > 2 else None
    return None


def extract_objective(text):
    """Extract fund objective / description."""
    patterns = [
        r'(?:Fund\s+)?(?:Objective|Investment\s+Objective|Objective\s*(?:&|and)\s*Strategy)[:\s]*\n?(.*?)(?=\n\s*(?:Fund\s+(?:Information|Details|Facts)|PROFILE|Performance|CUMULATIVE|Share\s+class))',
        r'(?:Fund\s+objectives?\s+and\s+investment\s+policy)[:\s]*\n?(.*?)(?=\n\s*(?:Past\s+Performance|Share\s+class|Performance|This\s+fund\s+may))',
        r'(?:OBJECTIVE\s*(?:&|AND)\s*STRATEGY)[:\s]*\n?(.*?)(?=\n\s*(?:PROFILE|CUMULATIVE|Fund\s+Inception))',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.DOTALL | re.IGNORECASE)
        if m:
            result = m.group(1).strip()
            # Clean up chart/performance data that may have leaked in
            result = re.sub(r'\d+\.\d+\s*$', '', result)
            result = re.sub(r'\s+', ' ', result)
            if len(result) > 800:
                result = result[:800] + '...'
            return result if len(result) > 20 else None
    return None


def extract_top_holdings(text):
    """Extract top holdings."""
    holdings = []
    # Blacklist: common metadata that should NOT be treated as holdings
    blacklist = {'total', 'source', 'benchmark', 'offer price', 'bid price',
                 'dealing frequency', 'fund management fee', 'net asset value',
                 'subscription mode', 'redemption', 'risk category', 'fund code',
                 'premium charge', 'fund currency', 'fund manager', 'fund size',
                 'fund information', 'fund objective', 'cpf classification',
                 'unit nav', 'management fee'}

    def is_valid_holding(name):
        name_lower = name.lower().strip()
        if len(name) < 3:
            return False
        for bl in blacklist:
            if bl in name_lower:
                return False
        return True

    # Look for the top holdings section
    patterns = [
        r'(?:Top\s+(?:Ten|10|Five|5)\s+Holdings?|Largest\s+Holdings?|Top\s+Holdings?)(.*?)(?=\n\s*(?:Source|Total\s|Note|Sector|Country|Region|Asset\s+[Aa]llocation|Credit|Net Currency|Risk|Learn|Portfolio\s+Statistics|Disclaimer|Important|Contact|The\s+information))',
        r'(?:Top\s+(?:Ten|10|Five|5)\s+Holdings?|Largest\s+Holdings?|Top\s+Holdings?)(.*?)(?=\n\n)',
    ]

    for pat in patterns:
        m = re.search(pat, text, re.DOTALL | re.IGNORECASE)
        if m:
            block = m.group(1)
            lines = block.split('\n')
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                # Match: holding name followed by percentage (with %)
                hm = re.match(r'^(.+?)\s+(\d+\.\d+)\s*%\s*$', line)
                if hm:
                    name = hm.group(1).strip()
                    pct = float(hm.group(2))
                    if pct < 50 and is_valid_holding(name):
                        holdings.append({'name': name, 'pct': pct})
                        continue
                # Match: holding name followed by percentage (without %)
                hm2 = re.match(r'^(.{8,}?)\s+(\d{1,2}\.\d{1,2})\s*$', line)
                if hm2:
                    name = hm2.group(1).strip()
                    pct = float(hm2.group(2))
                    if 0.3 < pct < 50 and is_valid_holding(name):
                        holdings.append({'name': name, 'pct': pct})
            if holdings:
                break

    # Fallback: look for "Holding name %" pattern within known holdings section
    if not holdings:
        # Find section with "Holding" header and look for lines with %
        m = re.search(r'(?:Holding\s+name|Holdings?)\s*%?\s*\n(.*?)(?=\n\s*(?:Source|Total|Note|Learn|Contact|Risk|Important))',
                      text, re.DOTALL | re.IGNORECASE)
        if m:
            block = m.group(1)
            for line in block.split('\n'):
                line = line.strip()
                hm = re.match(r'^(.{5,}?)\s+(\d{1,2}\.\d{1,2})\s*%?\s*$', line)
                if hm:
                    name = hm.group(1).strip()
                    pct = float(hm.group(2))
                    if 0.3 < pct < 50 and is_valid_holding(name):
                        holdings.append({'name': name, 'pct': pct})

    return holdings[:10] if holdings else None


def extract_sector_allocation(text):
    """Extract sector/asset allocation."""
    sectors = []
    m = re.search(r'(?:Sector\s+(?:Allocation|Breakdown)|Asset\s+[Aa]llocation|Industry\s+Allocation)(.*?)(?=\n\s*(?:Country|Geographic|Region|Top|Source|Net|Credit|Learn|Risk|Currency|Holding|Portfolio))',
                  text, re.DOTALL | re.IGNORECASE)
    if m:
        block = m.group(1)
        lines = block.split('\n')
        for line in lines:
            line = line.strip()
            if not line:
                continue
            # Match: sector name   percentage
            sm = re.match(r'^([A-Za-z][A-Za-z\s&/,-]+?)\s+(\d+\.\d+)', line)
            if sm:
                name = sm.group(1).strip()
                pct = float(sm.group(2))
                if 0.1 < pct < 100 and len(name) > 2 and name not in ('Fund', 'Benchmark', 'Total'):
                    sectors.append({'name': name, 'pct': pct})
    return sectors[:15] if sectors else None


def extract_geographic_allocation(text):
    """Extract geographic/country allocation."""
    countries = []
    m = re.search(r'(?:Country\s+(?:Allocation|Breakdown)|Geographic(?:al)?\s+(?:[Bb]reakdown|Allocation)|Region\s+(?:Allocation|Breakdown))(.*?)(?=\n\s*(?:Sector|Top|Source|Net|Credit|Learn|Risk|Currency|Holding|Region|Asset|Portfolio|Important|Disclaimer|Contact))',
                  text, re.DOTALL | re.IGNORECASE)
    if m:
        block = m.group(1)
        lines = block.split('\n')
        for line in lines:
            line = line.strip()
            if not line:
                continue
            cm = re.match(r'^([A-Za-z][A-Za-z\s(),.-]+?)\s+(\d+\.\d+)', line)
            if cm:
                name = cm.group(1).strip()
                pct = float(cm.group(2))
                if 0.1 < pct < 100 and len(name) > 1 and name not in ('Fund', 'Benchmark', 'Total', 'Other'):
                    countries.append({'name': name, 'pct': pct})
    return countries[:15] if countries else None


def extract_performance(text):
    """Extract annualized performance numbers."""
    perf = {}
    # Look for common performance patterns
    # YTD, 1Y, 3Y, 5Y, 10Y, Since Inception
    patterns = [
        (r'(?:YTD|Year\s*to\s*Date)[:\s]*([+-]?\d+\.?\d*)%?', 'ytd'),
        (r'1\s*(?:Y(?:ear)?|yr)[:\s]*([+-]?\d+\.?\d*)%?', '1y'),
        (r'3\s*(?:Y(?:ears?)?|yr)[:\s]*([+-]?\d+\.?\d*)%?', '3y'),
        (r'5\s*(?:Y(?:ears?)?|yr)[:\s]*([+-]?\d+\.?\d*)%?', '5y'),
        (r'10\s*(?:Y(?:ears?)?|yr)[:\s]*([+-]?\d+\.?\d*)%?', '10y'),
        (r'Since\s+(?:Inception|Launch)[:\s]*([+-]?\d+\.?\d*)%?', 'since_inception'),
    ]
    for pat, key in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                perf[key] = float(m.group(1))
            except ValueError:
                pass
    return perf if perf else None


def extract_currency(text):
    """Extract base currency."""
    patterns = [
        r'(?:Base|Fund)\s+[Cc]urrency[:\s]*(USD|SGD|EUR|GBP|JPY|AUD)',
        r'(?:Reference)\s+[Cc]urrency[:\s]*(USD|SGD|EUR|GBP|JPY|AUD)',
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            return m.group(1)
    return None


def extract_management_fee(text):
    """Extract management fee."""
    patterns = [
        r'(?:Annual\s+)?[Mm]anagement\s+[Ff]ee[:\s]*(\d+\.?\d*)\s*%',
        r'(?:Management\s+Fee)[:\s]*(?:Currently\s+)?(\d+\.?\d*)\s*%',
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            return float(m.group(1))
    return None


def get_fund_name_from_filename(filename):
    """Extract fund name from PDF filename."""
    # Remove "-Provider's Factsheet.pdf" suffix
    name = re.sub(r"-Provider.s Factsheet\.pdf$", '', filename)
    # Handle parentheses that were replaced
    name = name.replace('H2-SGD', '(H2-SGD)')
    return name


def process_all_pdfs():
    """Process all fact sheet PDFs and extract data."""
    files = sorted([f for f in os.listdir(FACT_DIR) if f.endswith('.pdf')])
    fund_facts = {}

    for i, filename in enumerate(files):
        pdf_path = os.path.join(FACT_DIR, filename)
        fund_name = get_fund_name_from_filename(filename)

        print(f"[{i+1}/{len(files)}] Processing: {fund_name}")

        try:
            full_text = extract_text(pdf_path)
            if not full_text:
                print(f"  WARNING: No text extracted")
                continue

            info = {
                'objective': extract_objective(full_text),
                'fundSize': extract_fund_size(full_text),
                'inceptionDate': extract_inception_date(full_text),
                'fundManager': extract_fund_manager(full_text),
                'baseCurrency': extract_currency(full_text),
                'managementFee': extract_management_fee(full_text),
                'topHoldings': extract_top_holdings(full_text),
                'sectorAllocation': extract_sector_allocation(full_text),
                'geoAllocation': extract_geographic_allocation(full_text),
                'performance': extract_performance(full_text),
                'fullText': full_text[:5000],  # Store first 5000 chars of text
            }

            # Count extracted fields
            extracted = sum(1 for k, v in info.items() if v and k != 'fullText')
            print(f"  Extracted {extracted}/10 fields")

            fund_facts[fund_name] = info

        except Exception as e:
            print(f"  ERROR: {e}")

    return fund_facts


def write_js_file(fund_facts):
    """Write fund_facts.js file."""
    # Custom JSON serialization
    js_content = 'const FUND_FACTS = ' + json.dumps(fund_facts, ensure_ascii=False, indent=2) + ';\n'

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(js_content)

    print(f"\nWritten {OUTPUT_FILE}")
    print(f"Total funds: {len(fund_facts)}")

    # Stats
    fields = ['objective', 'fundSize', 'inceptionDate', 'fundManager', 'baseCurrency',
              'managementFee', 'topHoldings', 'sectorAllocation', 'geoAllocation', 'performance']
    for field in fields:
        count = sum(1 for f in fund_facts.values() if f.get(field))
        print(f"  {field}: {count}/{len(fund_facts)}")


if __name__ == '__main__':
    fund_facts = process_all_pdfs()
    write_js_file(fund_facts)
