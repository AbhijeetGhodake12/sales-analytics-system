from collections import defaultdict
from datetime import datetime


def calculate_total_revenue(transactions):
    """
    Calculates total revenue from all transactions
    """
    total_revenue = 0.0

    for tx in transactions:
        total_revenue += tx["Quantity"] * tx["UnitPrice"]

    return round(total_revenue, 2)


def region_wise_sales(transactions):
    """
    Analyzes sales by region
    """
    region_data = defaultdict(lambda: {"total_sales": 0.0, "transaction_count": 0})
    grand_total = 0.0

    for tx in transactions:
        revenue = tx["Quantity"] * tx["UnitPrice"]
        region = tx["Region"]

        region_data[region]["total_sales"] += revenue
        region_data[region]["transaction_count"] += 1
        grand_total += revenue

    # Calculate percentage contribution
    for region in region_data:
        region_data[region]["percentage"] = round(
            (region_data[region]["total_sales"] / grand_total) * 100, 2
        )

    # Sort by total_sales descending
    sorted_regions = dict(
        sorted(
            region_data.items(),
            key=lambda x: x[1]["total_sales"],
            reverse=True,
        )
    )

    return sorted_regions


def top_selling_products(transactions, n=5):
    """
    Finds top n products by total quantity sold
    """
    product_data = defaultdict(lambda: {"quantity": 0, "revenue": 0.0})

    for tx in transactions:
        product = tx["ProductName"]
        qty = tx["Quantity"]
        revenue = qty * tx["UnitPrice"]

        product_data[product]["quantity"] += qty
        product_data[product]["revenue"] += revenue

    # Sort by quantity sold descending
    sorted_products = sorted(
        product_data.items(),
        key=lambda x: x[1]["quantity"],
        reverse=True,
    )

    result = []
    for product, data in sorted_products[:n]:
        result.append(
            (product, data["quantity"], round(data["revenue"], 2))
        )

    return result


def customer_analysis(transactions):
    """
    Analyzes customer purchase patterns
    """
    customer_data = defaultdict(
        lambda: {
            "total_spent": 0.0,
            "purchase_count": 0,
            "products_bought": set(),
        }
    )

    for tx in transactions:
        customer = tx["CustomerID"]
        revenue = tx["Quantity"] * tx["UnitPrice"]

        customer_data[customer]["total_spent"] += revenue
        customer_data[customer]["purchase_count"] += 1
        customer_data[customer]["products_bought"].add(tx["ProductName"])

    # Final formatting
    final_data = {}
    for customer, data in customer_data.items():
        avg_order_value = data["total_spent"] / data["purchase_count"]

        final_data[customer] = {
            "total_spent": round(data["total_spent"], 2),
            "purchase_count": data["purchase_count"],
            "avg_order_value": round(avg_order_value, 2),
            "products_bought": sorted(list(data["products_bought"])),
        }

    # Sort by total_spent descending
    sorted_customers = dict(
        sorted(
            final_data.items(),
            key=lambda x: x[1]["total_spent"],
            reverse=True,
        )
    )

    return sorted_customers


def daily_sales_trend(transactions):
    """
    Analyzes sales trends by date
    """
    daily_data = defaultdict(
        lambda: {
            "revenue": 0.0,
            "transaction_count": 0,
            "unique_customers": set(),
        }
    )

    for tx in transactions:
        date = tx["Date"]
        revenue = tx["Quantity"] * tx["UnitPrice"]

        daily_data[date]["revenue"] += revenue
        daily_data[date]["transaction_count"] += 1
        daily_data[date]["unique_customers"].add(tx["CustomerID"])

    # Final formatting and sorting by date
    final_data = {}
    for date in sorted(daily_data.keys()):
        final_data[date] = {
            "revenue": round(daily_data[date]["revenue"], 2),
            "transaction_count": daily_data[date]["transaction_count"],
            "unique_customers": len(daily_data[date]["unique_customers"]),
        }

    return final_data


def find_peak_sales_day(transactions):
    """
    Identifies the date with highest revenue
    """
    daily_revenue = defaultdict(lambda: {"revenue": 0.0, "count": 0})

    for tx in transactions:
        date = tx["Date"]
        revenue = tx["Quantity"] * tx["UnitPrice"]

        daily_revenue[date]["revenue"] += revenue
        daily_revenue[date]["count"] += 1

    peak_day = max(
        daily_revenue.items(),
        key=lambda x: x[1]["revenue"]
    )

    return (
        peak_day[0],
        round(peak_day[1]["revenue"], 2),
        peak_day[1]["count"],
    )


def low_performing_products(transactions, threshold=10):
    """
    Identifies products with low sales
    """
    product_data = defaultdict(lambda: {"quantity": 0, "revenue": 0.0})

    for tx in transactions:
        product = tx["ProductName"]
        qty = tx["Quantity"]
        revenue = qty * tx["UnitPrice"]

        product_data[product]["quantity"] += qty
        product_data[product]["revenue"] += revenue

    low_products = []

    for product, data in product_data.items():
        if data["quantity"] < threshold:
            low_products.append(
                (product, data["quantity"], round(data["revenue"], 2))
            )

    # Sort by quantity ascending
    low_products.sort(key=lambda x: x[1])

    return low_products
