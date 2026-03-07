#!/usr/bin/env python3
"""
Great Eastern Prestige Portfolio Fund Price Downloader
=====================================================
Downloads 1-year historical prices for all Prestige Portfolio funds
from the FE fundinfo API and saves them as CSV files.

Usage:
    python3 download_prices.py

Output:
    - Individual CSV files in price{YYYYMMDD}/ folder
    - Combined fund_data.js for the HTML analysis tool
"""

import requests
import json
import os
import sys
import time
import openpyxl
import csv
import io
from datetime import datetime

# ============================================================
# Configuration
# ============================================================
BASE_URL = "https://digital.feprecisionplus.com/greateasternlife/en-GB/GreatEastern_V2/DownloadTool/GetPriceHistoryFile"

MODEL = {
    "GrsProjectId": "95400076",
    "ProjectName": "greateasternlife",
    "ToolId": 16,
    "LanguageId": "1",
    "LanguageCode": "en-GB",
    "OverrideDocumentCountryCode": None,
    "forSaleIn": "",
    "FSIexclCT": ""
}

TIME_PERIOD = "12"  # 1 year. Options: 1(1m), 3(3m), 6(6m), 12(1yr), 36(3yrs), 60(5yrs), 120(10yrs), max

# ============================================================
# Fund List - Prestige Portfolio Funds
# Each entry: (code, name, sectorCode, citiCode, typeCode, currency, priceType)
# ============================================================
FUNDS = [
    ("H767", "AB Emerging Markets Debt Portfolio A2 USD", "SG", "H767", "FSG:H767", "USD", 2),
    ("IST3", "AB Global High Yield Portfolio A2 SGD", "SG", "IST3", "FSG:IST3", "SGD", 2),
    ("AII4", "AB Sustainable Global Thematic Fund A SGD", "SG", "AII4", "FSG:AII4", "SGD", 2),
    ("A214", "abrdn All China Sustainable Equity SGD", "SG", "A214", "FSG:A214", "SGD", 2),
    ("Z755", "abrdn Asian Smaller Companies SGD", "SG", "Z755", "FSG:AFSASC", "SGD", 2),
    ("OO94", "abrdn Global Emerging Markets SGD", "SG", "OO94", "FSG:IVGEM", "SGD", 2),
    ("A215", "abrdn Global Sustainable Equity SGD", "SG", "A215", "FSG:A215", "SGD", 2),
    ("ZQ91", "abrdn India Opportunities SGD", "SG", "ZQ91", "FSG:ZQ91", "SGD", 2),
    ("A218", "abrdn Malaysian Equity SGD", "SG", "A218", "FSG:A218", "SGD", 2),
    ("AP45", "abrdn Pacific Equity SGD", "SG", "AP45", "FSG:AP45", "SGD", 2),
    ("AP47", "abrdn Singapore Equity SGD", "SG", "AP47", "FSG:AP47", "SGD", 2),
    ("AP49", "abrdn Thailand Equity SGD", "SG", "AP49", "FSG:AP49", "SGD", 2),
    ("FENE", "Allianz Europe Equity Growth AT NAV EUR", "SG", "FENE", "FSG:FENE", "EUR", 2),
    ("G1R6", "Allianz Hong Kong Equity AT NAV SGD", "SG", "G1R6", "FSG:G1R6", "SGD", 2),
    ("F9CO", "Allianz Income and Growth AMi3 (H2-SGD)", "SG", "F9CO", "FSG:F9CO", "SGD", 2),
    ("FEQU", "Allianz US Equity AT NAV SGD", "SG", "FEQU", "FSG:FEQU", "SGD", 2),
    ("G09M", "Allianz US High Yield AM (H2-SGD) NAV SGD", "SG", "G09M", "FSG:G09M", "SGD", 2),
    ("MO20", "BlackRock GF Asian Tiger Bond A2 USD", "SG", "MO20", "FSG:MO20", "USD", 2),
    ("EGC3", "BlackRock GF China A2 Hedged SGD", "SG", "EGC3", "FSG:EGC3", "SGD", 2),
    ("QSNI", "Blackrock GF ESG Multi-Asset Fund SGD Hedged", "SG", "QSNI", "FSG:QSNI", "SGD", 2),
    ("OT85", "BlackRock GF Global Dynamic Equity A2 USD", "SG", "OT85", "FSG:OT85", "USD", 2),
    ("FB1J", "BlackRock GF Global Multi-Asset Income A6 Hedged SGD", "SG", "FB1J", "FSG:FB1J", "SGD", 2),
    ("MO74", "BlackRock GF Latin American A2 USD", "SG", "MO74", "FSG:MO74", "USD", 2),
    ("QC4N", "BlackRock GF Sustainable Energy Fund A2 SGD-H", "SG", "QC4N", "FSG:QC4N", "SGD", 2),
    ("WB22", "BlackRock GF United Kingdom A2 GBP", "SG", "WB22", "FSG:WB22", "GBP", 2),
    ("EGH1", "BlackRock GF World Energy A2 Hedged SGD", "SG", "EGH1", "FSG:EGH1", "SGD", 2),
    ("EGH0", "BlackRock GF World Gold A2 Hedged SGD", "SG", "EGH0", "FSG:EGH0", "SGD", 2),
    ("FJCC", "BlackRock GF World Healthscience A2 Hedged SGD", "SG", "FJCC", "FSG:FJCC", "SGD", 2),
    ("BMT0", "BlackRock GF World Mining A2 Hedged SGD", "SG", "BMT0", "FSG:BMT0", "SGD", 2),
    ("D821", "Fidelity America A Acc USD", "SG", "D821", "FSG:D821", "USD", 2),
    ("FJ42", "Fidelity Australian Diversified Equity A AUD", "SG", "FJ42", "FSG:FJ42", "AUD", 2),
    ("F465", "Fidelity Emerging Markets A Acc USD", "SG", "F465", "FSG:F465", "USD", 2),
    ("FS9L", "Fidelity European Dynamic Growth A SGD", "SG", "FS9L", "FSG:FS9L", "SGD", 2),
    ("JF8U", "Fidelity Global Multi Asset Income A Acc USD", "SG", "JF8U", "FSG:JF8U", "USD", 2),
    ("FJ3R", "Fidelity Global Technology A Acc USD", "SG", "FJ3R", "FSG:FJ3R", "USD", 2),
    ("FUL0", "Fidelity Greater China A SGD", "SG", "FUL0", "FSG:FUL0", "SGD", 2),
    ("A9J4", "Fidelity India Focus A SGD", "SG", "A9J4", "FSG:A9J4", "SGD", 2),
    ("F5PG", "Fidelity Sustainable Consumer Brands A Acc USD", "SG", "F5PG", "FSG:F5PG", "USD", 2),
    ("FW0A", "Fidelity World A Acc SGD", "SG", "FW0A", "FSG:FW0A", "SGD", 2),
    ("Z933", "Franklin Templeton Western Asset Global Bond Trust A Acc SGD", "SG", "Z933", "FSG:Z933", "SGD", 2),
    ("BDS7", "Franklin U.S. Opportunities A Acc SGD", "SG", "BDS7", "FSG:BDS7", "SGD", 2),
    ("MUCV", "FTGF Brandywine Global Income Optimiser Fund (SGD Hedged)", "SG", "MUCV", "FSG:MUCV", "SGD", 2),
    ("FOF5", "FTGF Mason Western Asset Global Multi Strategy A (M) Hedged Plus Dis SGD", "SG", "FOF5", "FSG:FOF5", "SGD", 2),
    ("MJT7", "FTGF Western Asset Asian Opportunities A (M) Hedged Plus Dis SGD", "SG", "MJT7", "FSG:MJT7", "SGD", 2),
    ("CBY9", "GreatLink ASEAN Growth", "SI", "CBY9", "FSI:CBY9", "SGD", 2),
    ("ATEL9", "GreatLink Asia Dividend Advantage", "SI", "ATEL9", "FSI:ATEL9", "SGD", 2),
    ("ATEL8", "GreatLink Asia High Dividend Equity", "SI", "ATEL8", "FSI:ATEL8", "SGD", 2),
    ("CBY7", "GreatLink Asia Pacific Equity", "SI", "CBY7", "FSI:CBY7", "SGD", 2),
    ("CBZ1", "GreatLink Cash", "SI", "CBZ1", "FSI:CBZ1", "SGD", 2),
    ("CCB2", "GreatLink China Growth", "SI", "CCB2", "FSI:CCB2", "SGD", 2),
    ("Q61M", "GreatLink Diversified Growth Portfolio", "SI", "Q61M", "FSI:Q61M", "SGD", 2),
    ("BY0ML", "GreatLink Dynamic Balanced Portfolio", "SI", "BY0ML", "FSI:BY0ML", "SGD", 2),
    ("BY0MN", "GreatLink Dynamic Growth Portfolio", "SI", "BY0MN", "FSI:BY0MN", "SGD", 2),
    ("BY0MK", "GreatLink Dynamic Secure Portfolio", "SI", "BY0MK", "FSI:BY0MK", "SGD", 2),
    ("CBZ2", "GreatLink European Sustainable Equity Fund", "SI", "CBZ2", "FSI:CBZ2", "SGD", 2),
    ("CBZ3", "GreatLink Far East Ex Japan Equities", "SI", "CBZ3", "FSI:CBZ3", "SGD", 2),
    ("CBZ5", "GreatLink Global Bond", "SI", "CBZ5", "FSI:CBZ5", "SGD", 2),
    ("UFX3", "GreatLink Global Disruptive Innovation Fund", "SI", "UFX3", "FSI:UFX3", "SGD", 2),
    ("I8IT", "GreatLink Global Emerging Markets Equity", "SI", "I8IT", "FSI:I8IT", "SGD", 2),
    ("CBZ4", "GreatLink Global Equity", "SI", "CBZ4", "FSI:CBZ4", "SGD", 2),
    ("M6B5", "GreatLink Global Equity Alpha", "SI", "M6B5", "FSI:M6B5", "SGD", 2),
    ("M6B6", "GreatLink Global Perspective", "SI", "M6B6", "FSI:M6B6", "SGD", 2),
    ("CBZ9", "GreatLink Global Real Estate Securities", "SI", "CBZ9", "FSI:CBZ9", "SGD", 2),
    ("CCA0", "GreatLink Global Supreme", "SI", "CCA0", "FSI:CCA0", "SGD", 2),
    ("CCA1", "GreatLink Global Technology", "SI", "CCA1", "FSI:CCA1", "SGD", 2),
    ("HUTV", "GreatLink Income Bond", "SI", "HUTV", "FSI:HUTV", "SGD", 2),
    ("CCB4", "GreatLink Income Focus", "SI", "CCB4", "FSI:CCB4", "SGD", 2),
    ("ULTC", "GreatLink International Health Care Fund", "SI", "ULTC", "FSI:ULTC", "SGD", 2),
    ("CCA5", "GreatLink LifeStyle Balanced Portfolio", "SI", "CCA5", "FSI:CCA5", "SGD", 2),
    ("CCA7", "GreatLink LifeStyle Dynamic Portfolio", "SI", "CCA7", "FSI:CCA7", "SGD", 2),
    ("CCA6", "GreatLink LifeStyle Progressive Portfolio", "SI", "CCA6", "FSI:CCA6", "SGD", 2),
    ("CCA3", "GreatLink LifeStyle Secure Portfolio", "SI", "CCA3", "FSI:CCA3", "SGD", 2),
    ("CCA4", "GreatLink LifeStyle Steady Portfolio", "SI", "CCA4", "FSI:CCA4", "SGD", 2),
    ("CCB5", "GreatLink Lion Asian Balanced", "SI", "CCB5", "FSI:CCB5", "SGD", 2),
    ("CCB3", "GreatLink Lion India", "SI", "CCB3", "FSI:CCB3", "SGD", 2),
    ("CCB1", "GreatLink Lion Japan Growth", "SI", "CCB1", "FSI:CCB1", "SGD", 2),
    ("CCB6", "GreatLink Lion Vietnam", "SI", "CCB6", "FSI:CCB6", "SGD", 2),
    ("V24J", "GreatLink Multi-Sector Income", "SI", "V24J", "FSI:V24J", "SGD", 2),
    ("V24K", "GreatLink Multi-Theme Equity", "SI", "V24K", "FSI:V24K", "SGD", 2),
    ("CCB0", "GreatLink Short Duration Bond", "SI", "CCB0", "FSI:CCB0", "SGD", 2),
    ("CCA9", "GreatLink Singapore Equities", "SI", "CCA9", "FSI:CCA9", "SGD", 2),
    ("CAIFI", "GreatLink Singapore Physical Gold Fund", "SI", "CAIFI", "FSI:CAIFI", "SGD", 2),
    ("CBZ6", "GreatLink Sustainable Global Thematic Fund", "SI", "CBZ6", "FSI:CBZ6", "SGD", 2),
    ("ALIPX", "GreatLink US Income and Growth Fund (Dis)", "SI", "ALIPX", "FSI:ALIPX", "SGD", 2),
    ("E7CA", "Janus Henderson Horizon Asia-Pacific Property Income A3 Inc SGD", "SG", "E7CA", "FSG:E7CA", "SGD", 2),
    ("CRW2", "Janus Henderson Horizon China Opportunties A2 Acc SGD", "SG", "CRW2", "FSG:CRW2", "SGD", 2),
    ("H008", "Janus Henderson Horizon Global Property Equities A2 Acc USD", "SG", "H008", "FSG:H008", "USD", 2),
    ("E7CD", "Janus Henderson Horizon Global Technology Leaders A2 Acc SGD", "SG", "E7CD", "FSG:E7CD", "SGD", 2),
    ("HY11", "Janus Henderson Horizon Japan Opportunities A2 Acc USD", "SG", "HY11", "FSG:HY11", "USD", 2),
    ("KOS8", "JPM ASEAN Equity A Acc NAV SGD", "SG", "KOS8", "FSG:KOS8", "SGD", 2),
    ("FCEG", "JPM Asia Pacific Equity A Acc NAV SGD", "SG", "FCEG", "FSG:FCEG", "SGD", 2),
    ("F6OG", "JPM Asia Pacific Income A Hedged Mth NAV SGD", "SG", "F6OG", "FSG:F6OG", "SGD", 2),
    ("F5CP", "JPM Emerging Markets Opportunities A Hedged Acc NAV SGD", "SG", "F5CP", "FSG:F5CP", "SGD", 2),
    ("F7PL", "JPM Global Income A Hedged Mth NAV SGD", "SG", "F7PL", "FSG:F7PL", "SGD", 2),
    ("FI1T", "JPM Japan Equity A Dis NAV SGD", "SG", "FI1T", "FSG:FI1T", "SGD", 2),
    ("FKJW", "JPM US Value A Acc NAV SGD", "SG", "FKJW", "FSG:FKJW", "SGD", 2),
    ("I0S7", "LionGlobal Asia Bond SGD", "SG", "I0S7", "FSG:I0S7", "SGD", 2),
    ("H28R", "LionGlobal Asia High Dividend Equity G Dis SGD", "SG", "H28R", "FSG:H28R", "SGD", 2),
    ("A5B6", "LionGlobal Asia Pacific Acc SGD", "SG", "A5B6", "FSG:A5B6", "SGD", 2),
    ("A5E4", "LionGlobal China Growth Acc SGD", "SG", "A5E4", "FSG:A5E4", "SGD", 2),
    ("BXZL", "LionGlobal Disruptive Innovation A", "SG", "BXZL", "FSG:BXZL", "SGD", 2),
    ("A5C7", "LionGlobal India Acc SGD", "SG", "A5C7", "FSG:A5C7", "SGD", 2),
    ("K8OU", "LionGlobal Japan Growth Hedged SGD", "SG", "K8OU", "FSG:K8OU", "SGD", 2),
    ("A5D3", "LionGlobal Korea Acc SGD", "SG", "A5D3", "FSG:A5D3", "SGD", 2),
    ("A5D5", "LionGlobal Malaysia Acc SGD", "SG", "A5D5", "FSG:A5D5", "SGD", 2),
    ("A5G1", "LionGlobal SGD Money Market Acc SGD", "SG", "A5G1", "FSG:A5G1", "SGD", 2),
    ("A5E8", "LionGlobal Short Duration Bond A Dis SGD", "SG", "A5E8", "FSG:A5E8", "SGD", 2),
    ("A5F5", "LionGlobal Singapore Balanced SGD", "SG", "A5F5", "FSG:A5F5", "SGD", 2),
    ("A5F7", "LionGlobal Singapore Fixed Income Investment A", "SG", "A5F7", "FSG:A5F7", "SGD", 2),
    ("A5F9", "LionGlobal Singapore Trust Acc SGD", "SG", "A5F9", "FSG:A5F9", "SGD", 2),
    ("A5B8", "LionGlobal South East Asia SGD", "SG", "A5B8", "FSG:A5B8", "SGD", 2),
    ("A5G7", "LionGlobal Taiwan Acc SGD", "SG", "A5G7", "FSG:A5G7", "SGD", 2),
    ("A5G9", "LionGlobal Thailand Acc SGD", "SG", "A5G9", "FSG:A5G9", "SGD", 2),
    ("A5H1", "LionGlobal Vietnam Acc SGD", "SG", "A5H1", "FSG:A5H1", "SGD", 2),
    ("HW12", "Pimco GIS Emerging Markets Bond E Hedged Acc SGD", "SG", "HW12", "FSG:HW12", "SGD", 2),
    ("JN58", "Pimco GIS Global Real Return E Acc USD", "SG", "JN58", "FSG:JN58", "USD", 2),
    ("HW10", "Pimco GIS Total Return Bond E Hedged Acc SGD", "SG", "HW10", "FSG:HW10", "SGD", 2),
    ("AP43", "Schroder Asian Growth SGD", "SG", "AP43", "FSG:SCHAG", "SGD", 2),
    ("A5J3", "Schroder BIC", "SG", "A5J3", "FSG:A5J3", "SGD", 2),
    ("A5J5", "Schroder Emerging Markets", "SG", "A5J5", "FSG:A5J5", "SGD", 2),
    ("M5K0", "Schroder Global Emerging Market Opportunities", "SG", "M5K0", "FSG:M5K0", "SGD", 2),
    ("FCAP", "Schroder ISF Global Climate Change Equity A Acc NAV SGD", "SG", "FCAP", "FSG:FCAP", "SGD", 2),
    ("F9FW", "Schroder ISF Global Equity Alpha A Acc NAV USD", "SG", "F9FW", "FSG:F9FW", "USD", 2),
    ("MK74", "Schroder ISF Global Gold A Hedged Acc NAV SGD", "SG", "MK74", "FSG:MK74", "SGD", 2),
    ("F6CZ", "Schroder ISF Greater China A Acc NAV USD", "SG", "F6CZ", "FSG:F6CZ", "USD", 2),
    ("M5K2", "Schroder Multi Asset Revolution", "SG", "M5K2", "FSG:M5K2", "SGD", 2),
    ("A5K7", "Schroder Singapore Fixed Income A Acc", "SG", "A5K7", "FSG:A5K7", "SGD", 2),
    ("AP51", "Schroder Singapore Trust A SGD", "SG", "AP51", "FSG:AP51", "SGD", 2),
    ("MO22", "Templeton Asian Bond A (H1) MDis SGD", "SG", "MO22", "FSG:MO22", "SGD", 2),
    ("MS71", "Templeton Asian Growth A Acc SGD", "SG", "MS71", "FSG:MS71", "SGD", 2),
    ("GT14", "Templeton Asian Smaller Companies A Acc SGD", "SG", "GT14", "FSG:GT14", "SGD", 2),
    ("MO58", "Templeton Frontier Markets A Acc SGD", "SG", "MO58", "FSG:MO58", "SGD", 2),
    ("MO50", "Templeton Global Bond A (H1) MDis SGD", "SG", "MO50", "FSG:MO50", "SGD", 2),
    ("MO56", "Templeton Global Total Return A (H1) MDis SGD", "SG", "MO56", "FSG:MO56", "SGD", 2),
    ("MW17", "Templeton Latin America A Acc SGD", "SG", "MW17", "FSG:MW17", "SGD", 2),
    ("OWJ0", "UTI India Dynamic Equity Fund SGD", "SG", "OWJ0", "FSG:OWJ0", "SGD", 2),
]


def download_fund_price(fund_info, output_dir, session):
    """Download price history for a single fund."""
    code, name, sector_code, citi_code, type_code, currency, price_type = fund_info

    filters = {
        "FundName": name,
        "SectorClassCode": sector_code,
        "CitiCode": citi_code,
        "TypeCode": type_code,
        "BaseCurrency": currency,
        "PriceType": price_type,
        "TimePeriod": TIME_PERIOD,
        "StartDate": "",
        "EndDate": ""
    }

    params = {
        "modelString": json.dumps(MODEL),
        "filtersString": json.dumps(filters)
    }

    try:
        resp = session.get(BASE_URL, params=params, timeout=30)
        if resp.status_code != 200:
            return None, f"HTTP {resp.status_code}"

        # Parse XLSX content
        wb = openpyxl.load_workbook(io.BytesIO(resp.content))
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))

        if len(rows) < 2:
            return None, "No price data"

        # Save as CSV
        safe_name = name.replace("/", "-").replace("\\", "-").replace(":", "-")
        csv_filename = f"{safe_name}.csv"
        csv_path = os.path.join(output_dir, csv_filename)

        with open(csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(rows)

        # Parse data for JSON output
        prices = []
        for row in rows[1:]:  # Skip header
            try:
                date_str = str(row[0])
                price_val = float(row[1])
                curr = str(row[2])
                prices.append({
                    "date": date_str,
                    "price": price_val,
                    "currency": curr
                })
            except (ValueError, TypeError, IndexError):
                continue

        return prices, None

    except requests.exceptions.Timeout:
        return None, "Timeout"
    except Exception as e:
        return None, str(e)


def main():
    today = datetime.now().strftime("%Y%m%d")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, f"price{today}")

    os.makedirs(output_dir, exist_ok=True)

    print(f"=" * 70)
    print(f"Great Eastern Prestige Portfolio Fund Price Downloader")
    print(f"=" * 70)
    print(f"Date:        {today}")
    print(f"Output:      {output_dir}")
    print(f"Funds:       {len(FUNDS)}")
    print(f"Time Period: {TIME_PERIOD} months")
    print(f"=" * 70)

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    })

    all_fund_data = {}
    success_count = 0
    fail_count = 0

    for i, fund in enumerate(FUNDS):
        code, name = fund[0], fund[1]
        print(f"[{i+1:3d}/{len(FUNDS)}] Downloading: {name}...", end=" ", flush=True)

        prices, error = download_fund_price(fund, output_dir, session)

        if error:
            print(f"FAILED ({error})")
            fail_count += 1
        else:
            print(f"OK ({len(prices)} prices)")
            success_count += 1
            all_fund_data[name] = {
                "code": code,
                "currency": fund[5],
                "prices": prices
            }

        # Small delay to be respectful to the server
        time.sleep(0.3)

    # Generate combined JS data file for the HTML analysis tool
    js_path = os.path.join(script_dir, "fund_data.js")
    with open(js_path, 'w') as f:
        f.write(f"// Fund price data generated on {today}\n")
        f.write(f"// Total funds: {success_count}\n")
        f.write(f"const FUND_DATA = ")
        json.dump(all_fund_data, f, indent=None)  # compact for performance
        f.write(";\n")

    print(f"\n{'=' * 70}")
    print(f"Download Complete!")
    print(f"  Success: {success_count}")
    print(f"  Failed:  {fail_count}")
    print(f"  CSV files: {output_dir}/")
    print(f"  JS data:   {js_path}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
