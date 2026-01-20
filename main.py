import re


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


def main():
    file_path = "sales_data.txt"
    clean_sales_data(file_path)


if __name__ == "__main__":
    main()
