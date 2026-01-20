import requests
import os


def fetch_all_products():
    """
    Fetches all products from DummyJSON API
    """
    url = "https://dummyjson.com/products"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()
        products = data.get("products", [])

        api_products = []
        for p in products:
            api_products.append({
                "id": p.get("id"),
                "title": p.get("title"),
                "category": p.get("category"),
                "brand": p.get("brand"),
                "price": p.get("price"),
                "rating": p.get("rating"),
            })

        print(f" Successfully fetched {len(api_products)} products from API")
        return api_products

    except requests.exceptions.RequestException as e:
        print(f" Failed to fetch products from API: {e}")
        return []


def create_product_mapping(api_products):
    """
    Creates a mapping of product IDs to product info
    """
    product_mapping = {}

    for product in api_products:
        product_id = product.get("id")
        if product_id is not None:
            product_mapping[product_id] = {
                "title": product.get("title"),
                "category": product.get("category"),
                "brand": product.get("brand"),
                "rating": product.get("rating"),
            }

    return product_mapping


def enrich_sales_data(transactions, product_mapping):
    """
    Enriches transaction data with API product information
    """
    enriched_transactions = []

    for tx in transactions:
        enriched_tx = tx.copy()

        # Default enrichment values
        enriched_tx["API_Category"] = None
        enriched_tx["API_Brand"] = None
        enriched_tx["API_Rating"] = None
        enriched_tx["API_Match"] = False

        try:
            # Extract numeric product ID (P101 -> 101)
            product_id_str = tx.get("ProductID", "")
            numeric_id = int("".join(filter(str.isdigit, product_id_str)))

            if numeric_id in product_mapping:
                api_product = product_mapping[numeric_id]

                enriched_tx["API_Category"] = api_product.get("category")
                enriched_tx["API_Brand"] = api_product.get("brand")
                enriched_tx["API_Rating"] = api_product.get("rating")
                enriched_tx["API_Match"] = True

        except Exception:
            # Keep defaults if anything fails
            pass

        enriched_transactions.append(enriched_tx)

    # Save to file
    save_enriched_data(enriched_transactions)

    print(f" Enriched {len(enriched_transactions)} transactions")
    return enriched_transactions


def save_enriched_data(enriched_transactions, filename="data/enriched_sales_data.txt"):
    """
    Saves enriched transactions back to file
    """
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    headers = [
        "TransactionID",
        "Date",
        "ProductID",
        "ProductName",
        "Quantity",
        "UnitPrice",
        "CustomerID",
        "Region",
        "API_Category",
        "API_Brand",
        "API_Rating",
        "API_Match",
    ]

    with open(filename, "w", encoding="utf-8") as file:
        file.write("|".join(headers) + "\n")

        for tx in enriched_transactions:
            row = []
            for h in headers:
                value = tx.get(h)
                if value is None:
                    row.append("")
                else:
                    row.append(str(value))
            file.write("|".join(row) + "\n")

    print(f" Enriched data saved to '{filename}'")
