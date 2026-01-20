from collections import defaultdict


def read_sales_data(filename):
    """
    Reads sales data from file handling encoding issues
    Returns list of raw lines (strings)
    """
    encodings = ["utf-8", "latin-1", "cp1252"]

    for enc in encodings:
        try:
            with open(filename, "r", encoding=enc) as file:
                lines = file.readlines()
                break
        except UnicodeDecodeError:
            continue
        except FileNotFoundError:
            print(f"Error: File '{filename}' not found.")
            return []
    else:
        print("Error: Unable to read file with supported encodings.")
        return []

    # Skip header and remove empty lines
    raw_lines = []
    for line in lines[1:]:
        line = line.strip()
        if line:
            raw_lines.append(line)

    return raw_lines


def parse_transactions(raw_lines):
    """
    Parses raw lines into clean list of dictionaries
    """
    transactions = []
    headers = [
        "TransactionID",
        "Date",
        "ProductID",
        "ProductName",
        "Quantity",
        "UnitPrice",
        "CustomerID",
        "Region",
    ]

    for line in raw_lines:
        parts = line.split("|")

        # Skip rows with incorrect number of fields
        if len(parts) != len(headers):
            continue

        try:
            transaction = {}

            transaction["TransactionID"] = parts[0].strip()
            transaction["Date"] = parts[1].strip()
            transaction["ProductID"] = parts[2].strip()

            # Remove commas inside ProductName
            transaction["ProductName"] = parts[3].replace(",", "").strip()

            # Convert Quantity
            transaction["Quantity"] = int(parts[4].replace(",", "").strip())

            # Convert UnitPrice
            transaction["UnitPrice"] = float(parts[5].replace(",", "").strip())

            transaction["CustomerID"] = parts[6].strip()
            transaction["Region"] = parts[7].strip()

            transactions.append(transaction)

        except (ValueError, IndexError):
            continue

    return transactions


def validate_and_filter(transactions, region=None, min_amount=None, max_amount=None):
    """
    Validates transactions and applies optional filters
    """
    valid_transactions = []
    invalid_count = 0

    summary = {
        "total_input": len(transactions),
        "invalid": 0,
        "filtered_by_region": 0,
        "filtered_by_amount": 0,
        "final_count": 0,
    }

    # Show available regions
    regions = sorted({tx["Region"] for tx in transactions if tx.get("Region")})
    print(f"Available Regions: {regions}")

    # Show transaction amount range
    amounts = [
        tx["Quantity"] * tx["UnitPrice"]
        for tx in transactions
        if tx.get("Quantity") and tx.get("UnitPrice")
    ]
    if amounts:
        print(f"Transaction Amount Range: {min(amounts)} - {max(amounts)}")

    for tx in transactions:
        try:
            # ---------------- VALIDATION ----------------
            if tx["Quantity"] <= 0:
                invalid_count += 1
                continue

            if tx["UnitPrice"] <= 0:
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

            # ---------------- FILTERS ----------------
            amount = tx["Quantity"] * tx["UnitPrice"]

            if region and tx["Region"] != region:
                summary["filtered_by_region"] += 1
                continue

            if min_amount is not None and amount < min_amount:
                summary["filtered_by_amount"] += 1
                continue

            if max_amount is not None and amount > max_amount:
                summary["filtered_by_amount"] += 1
                continue

            valid_transactions.append(tx)

        except KeyError:
            invalid_count += 1

    summary["invalid"] = invalid_count
    summary["final_count"] = len(valid_transactions)

    print(f"Records after validation: {summary['total_input'] - invalid_count}")
    print(f"Records after region filter: {summary['total_input'] - invalid_count - summary['filtered_by_region']}")
    print(f"Final valid records: {summary['final_count']}")

    return valid_transactions, invalid_count, summary
