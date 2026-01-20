import re
from datetime import datetime
from collections import defaultdict
import os

from utils.file_handler import (
    read_sales_data,
    parse_transactions,
)
from utils.data_processor import (
    calculate_total_revenue,
    region_wise_sales,
    top_selling_products,
    customer_analysis,
    daily_sales_trend,
    find_peak_sales_day,
    low_performing_products,
)
from utils.api_handler import (
    fetch_all_products,
    create_product_mapping,
    enrich_sales_data,
)

def clean_sales_data(file_path):
    total_records = 0
    invalid_records = 0
    valid_records = []

    with open(file_path, "r", encoding="latin-1") as file:
        header = file.readline().strip().split("|")

        for line in file:
            line = line.strip()

            # Skip empty lines
            if not line:
                continue

            total_records += 1

            parts = line.split("|")

            # Skip rows with missing or extra fields
            if len(parts) != len(header):
                invalid_records += 1
                continue

            record = dict(zip(header, parts))

            try:
                # ---------------- VALIDATION RULES ----------------
                transaction_id = record["TransactionID"]
                customer_id = record["CustomerID"].strip()
                region = record["Region"].strip()

                if not transaction_id.startswith("T"):
                    invalid_records += 1
                    continue

                if customer_id == "" or region == "":
                    invalid_records += 1
                    continue

                # ---------------- CLEANING ----------------
                # Remove commas from ProductName
                record["ProductName"] = record["ProductName"].replace(",", "")

                # Clean Quantity
                quantity = int(record["Quantity"])
                if quantity <= 0:
                    invalid_records += 1
                    continue
                record["Quantity"] = quantity

                # Clean UnitPrice (remove commas)
                unit_price = float(record["UnitPrice"].replace(",", ""))
                if unit_price <= 0:
                    invalid_records += 1
                    continue
                record["UnitPrice"] = unit_price

                valid_records.append(record)

            except Exception:
                invalid_records += 1

    # ---------------- OUTPUT ----------------
    print(f"Total records parsed: {total_records}")
    print(f"Invalid records removed: {invalid_records}")
    print(f"Valid records after cleaning: {len(valid_records)}")

    return valid_records


def generate_sales_report(transactions, enriched_transactions, output_file='output/sales_report.txt'):
    """
    Generates a comprehensive formatted text report
    """

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_records = len(transactions)

    # ---------------- OVERALL SUMMARY ----------------
    total_revenue = sum(tx["Quantity"] * tx["UnitPrice"] for tx in transactions)
    avg_order_value = total_revenue / total_records if total_records else 0

    dates = sorted(tx["Date"] for tx in transactions)
    date_range = f"{dates[0]} to {dates[-1]}" if dates else "N/A"

    # ---------------- REGION-WISE PERFORMANCE ----------------
    region_data = defaultdict(lambda: {"revenue": 0.0, "count": 0})

    for tx in transactions:
        revenue = tx["Quantity"] * tx["UnitPrice"]
        region_data[tx["Region"]]["revenue"] += revenue
        region_data[tx["Region"]]["count"] += 1

    region_rows = []
    for region, data in region_data.items():
        percentage = (data["revenue"] / total_revenue) * 100 if total_revenue else 0
        region_rows.append((region, data["revenue"], percentage, data["count"]))

    region_rows.sort(key=lambda x: x[1], reverse=True)

    # ---------------- TOP 5 PRODUCTS ----------------
    product_data = defaultdict(lambda: {"qty": 0, "revenue": 0.0})

    for tx in transactions:
        product_data[tx["ProductName"]]["qty"] += tx["Quantity"]
        product_data[tx["ProductName"]]["revenue"] += tx["Quantity"] * tx["UnitPrice"]

    top_products = sorted(
        product_data.items(),
        key=lambda x: x[1]["qty"],
        reverse=True
    )[:5]

    # ---------------- TOP 5 CUSTOMERS ----------------
    customer_data = defaultdict(lambda: {"spent": 0.0, "count": 0})

    for tx in transactions:
        customer_data[tx["CustomerID"]]["spent"] += tx["Quantity"] * tx["UnitPrice"]
        customer_data[tx["CustomerID"]]["count"] += 1

    top_customers = sorted(
        customer_data.items(),
        key=lambda x: x[1]["spent"],
        reverse=True
    )[:5]

    # ---------------- DAILY SALES TREND ----------------
    daily_data = defaultdict(lambda: {"revenue": 0.0, "count": 0, "customers": set()})

    for tx in transactions:
        daily_data[tx["Date"]]["revenue"] += tx["Quantity"] * tx["UnitPrice"]
        daily_data[tx["Date"]]["count"] += 1
        daily_data[tx["Date"]]["customers"].add(tx["CustomerID"])

    daily_rows = sorted(daily_data.items())

    # ---------------- PRODUCT PERFORMANCE ----------------
    best_day = max(daily_rows, key=lambda x: x[1]["revenue"])

    low_products = [
        (p, d["qty"], d["revenue"])
        for p, d in product_data.items()
        if d["qty"] < 10
    ]

    avg_tx_value_region = {
        r: d["revenue"] / d["count"]
        for r, d in region_data.items()
    }

    # ---------------- API ENRICHMENT SUMMARY ----------------
    enriched_count = sum(1 for tx in enriched_transactions if tx.get("API_Match"))
    enrichment_rate = (enriched_count / len(enriched_transactions)) * 100 if enriched_transactions else 0

    unenriched_products = sorted({
        tx["ProductName"]
        for tx in enriched_transactions
        if not tx.get("API_Match")
    })

    # ---------------- WRITE REPORT ----------------
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("=" * 44 + "\n")
        f.write("           SALES ANALYTICS REPORT\n")
        f.write(f"         Generated: {now}\n")
        f.write(f"         Records Processed: {total_records}\n")
        f.write("=" * 44 + "\n\n")

        f.write("OVERALL SUMMARY\n")
        f.write("-" * 44 + "\n")
        f.write(f"Total Revenue:        ₹{total_revenue:,.2f}\n")
        f.write(f"Total Transactions:   {total_records}\n")
        f.write(f"Average Order Value:  ₹{avg_order_value:,.2f}\n")
        f.write(f"Date Range:           {date_range}\n\n")

        f.write("REGION-WISE PERFORMANCE\n")
        f.write("-" * 44 + "\n")
        f.write(f"{'Region':10}{'Sales':15}{'% of Total':12}{'Transactions'}\n")
        for r, rev, pct, cnt in region_rows:
            f.write(f"{r:10}₹{rev:,.0f}{'':5}{pct:6.2f}%{'':6}{cnt}\n")
        f.write("\n")

        f.write("TOP 5 PRODUCTS\n")
        f.write("-" * 44 + "\n")
        f.write(f"{'Rank':5}{'Product':20}{'Qty':6}{'Revenue'}\n")
        for i, (p, d) in enumerate(top_products, 1):
            f.write(f"{i:<5}{p:20}{d['qty']:<6}₹{d['revenue']:,.2f}\n")
        f.write("\n")

        f.write("TOP 5 CUSTOMERS\n")
        f.write("-" * 44 + "\n")
        f.write(f"{'Rank':5}{'Customer':15}{'Spent':15}{'Orders'}\n")
        for i, (c, d) in enumerate(top_customers, 1):
            f.write(f"{i:<5}{c:15}₹{d['spent']:,.2f}{'':3}{d['count']}\n")
        f.write("\n")

        f.write("DAILY SALES TREND\n")
        f.write("-" * 44 + "\n")
        f.write(f"{'Date':12}{'Revenue':15}{'Txns':8}{'Customers'}\n")
        for date, d in daily_rows:
            f.write(f"{date:12}₹{d['revenue']:,.2f}{'':2}{d['count']:<8}{len(d['customers'])}\n")
        f.write("\n")

        f.write("PRODUCT PERFORMANCE ANALYSIS\n")
        f.write("-" * 44 + "\n")
        f.write(f"Best Selling Day: {best_day[0]} (₹{best_day[1]['revenue']:,.2f})\n\n")

        if low_products:
            f.write("Low Performing Products:\n")
            for p, q, r in low_products:
                f.write(f"- {p}: Qty={q}, Revenue=₹{r:,.2f}\n")
        else:
            f.write("No low performing products found.\n")

        f.write("\nAverage Transaction Value per Region:\n")
        for r, v in avg_tx_value_region.items():
            f.write(f"- {r}: ₹{v:,.2f}\n")

        f.write("\nAPI ENRICHMENT SUMMARY\n")
        f.write("-" * 44 + "\n")
        f.write(f"Total Enriched Records: {enriched_count}\n")
        f.write(f"Success Rate: {enrichment_rate:.2f}%\n")
        f.write("Unenriched Products:\n")
        for p in unenriched_products:
            f.write(f"- {p}\n")

    print(f"📄 Sales report generated at: {output_file}")

def validate_and_filter(transactions, region=None, min_amount=None, max_amount=None):
    """
    Validates transactions and applies optional filters

    Returns:
    - valid_transactions: list
    - invalid_count: int
    - filter_summary: dict
    """

    valid_transactions = []
    invalid_count = 0

    filter_summary = {
        "total_input": len(transactions),
        "invalid": 0,
        "filtered_by_region": 0,
        "filtered_by_amount": 0,
        "final_count": 0,
    }

    # Display available regions
    available_regions = sorted({tx.get("Region") for tx in transactions if tx.get("Region")})
    print(f"Available Regions: {', '.join(available_regions)}")

    # Display transaction amount range
    amounts = [
        tx["Quantity"] * tx["UnitPrice"]
        for tx in transactions
        if isinstance(tx.get("Quantity"), int) and isinstance(tx.get("UnitPrice"), (int, float))
    ]

    if amounts:
        print(f"Transaction Amount Range: ₹{min(amounts):,.0f} - ₹{max(amounts):,.0f}")

    for tx in transactions:
        try:
            # ---------------- VALIDATION RULES ----------------
            if not all(k in tx for k in [
                "TransactionID", "ProductID", "CustomerID",
                "Quantity", "UnitPrice", "Region"
            ]):
                invalid_count += 1
                continue

            if not tx["TransactionID"].startswith("T"):
                invalid_count += 1
                continue

            if not tx["ProductID"].startswith("P"):
                invalid_count += 1
                continue

            if not tx["CustomerID"].startswith("C"):
                invalid_count += 1
                continue

            if tx["Quantity"] <= 0 or tx["UnitPrice"] <= 0:
                invalid_count += 1
                continue

            # ---------------- FILTERING ----------------
            amount = tx["Quantity"] * tx["UnitPrice"]

            if region and tx["Region"] != region:
                filter_summary["filtered_by_region"] += 1
                continue

            if min_amount is not None and amount < min_amount:
                filter_summary["filtered_by_amount"] += 1
                continue

            if max_amount is not None and amount > max_amount:
                filter_summary["filtered_by_amount"] += 1
                continue

            valid_transactions.append(tx)

        except Exception:
            invalid_count += 1

    filter_summary["invalid"] = invalid_count
    filter_summary["final_count"] = len(valid_transactions)

    print(f"Records after validation: {filter_summary['total_input'] - invalid_count}")
    print(f"Final valid records: {filter_summary['final_count']}")

    return valid_transactions, invalid_count, filter_summary


def main():
    """
    Main execution function
    """
    try:
        print("=" * 40)
        print("SALES ANALYTICS SYSTEM")
        print("=" * 40)
        print()

        # 1. Read sales data
        print("[1/10] Reading sales data...")
        raw_lines = read_sales_data("data/sales_data.txt")
        print(f"✓ Successfully read {len(raw_lines)} transactions\n")

        # 2. Parse and clean
        print("[2/10] Parsing and cleaning data...")
        transactions = parse_transactions(raw_lines)
        print(f"✓ Parsed {len(transactions)} records\n")

        # 3. Display filter options
        regions = sorted({tx["Region"] for tx in transactions})
        amounts = [tx["Quantity"] * tx["UnitPrice"] for tx in transactions]

        print("[3/10] Filter Options Available:")
        print(f"Regions: {', '.join(regions)}")
        print(f"Amount Range: ₹{min(amounts):,.0f} - ₹{max(amounts):,.0f}\n")

        apply_filter = input("Do you want to filter data? (y/n): ").strip().lower()

        region_filter = None
        min_amount = None
        max_amount = None

        if apply_filter == "y":
            region_filter = input("Enter region (or press Enter to skip): ").strip()
            if region_filter == "":
                region_filter = None

            min_val = input("Enter minimum amount (or press Enter to skip): ").strip()
            max_val = input("Enter maximum amount (or press Enter to skip): ").strip()

            min_amount = float(min_val) if min_val else None
            max_amount = float(max_val) if max_val else None

        print()

        # 4. Validate & filter
        print("[4/10] Validating transactions...")
        valid_transactions, invalid_count, summary = validate_and_filter(
            transactions,
            region=region_filter,
            min_amount=min_amount,
            max_amount=max_amount,
        )
        print(f"✓ Valid: {len(valid_transactions)} | Invalid: {invalid_count}\n")

        # 5. Analysis
        print("[5/10] Analyzing sales data...")
        _ = calculate_total_revenue(valid_transactions)
        _ = region_wise_sales(valid_transactions)
        _ = top_selling_products(valid_transactions)
        _ = customer_analysis(valid_transactions)
        _ = daily_sales_trend(valid_transactions)
        _ = find_peak_sales_day(valid_transactions)
        _ = low_performing_products(valid_transactions)
        print("✓ Analysis complete\n")

        # 6. Fetch API data
        print("[6/10] Fetching product data from API...")
        api_products = fetch_all_products()
        print(f"✓ Fetched {len(api_products)} products\n")

        # 7. Enrich data
        print("[7/10] Enriching sales data...")
        product_mapping = create_product_mapping(api_products)
        enriched_transactions = enrich_sales_data(valid_transactions, product_mapping)

        enriched_count = sum(1 for tx in enriched_transactions if tx["API_Match"])
        success_rate = (enriched_count / len(enriched_transactions)) * 100 if enriched_transactions else 0

        print(f"✓ Enriched {enriched_count}/{len(enriched_transactions)} "
              f"transactions ({success_rate:.1f}%)\n")

        # 8. Save enriched data (already done inside enrich function)
        print("[8/10] Saving enriched data...")
        print("✓ Saved to: data/enriched_sales_data.txt\n")

        # 9. Generate report
        print("[9/10] Generating report...")
        generate_sales_report(valid_transactions, enriched_transactions)
        print("✓ Report saved to: output/sales_report.txt\n")

        # 10. Done
        print("[10/10] Process Complete!")
        print("=" * 40)

    except Exception as e:
        print("\n❌ An unexpected error occurred.")
        print(f"Details: {e}")
        print("Please check input files or try again.")


if __name__ == "__main__":
    main()