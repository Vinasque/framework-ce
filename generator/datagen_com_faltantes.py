import json
import random
from faker import Faker
from datetime import datetime, timedelta

faker = Faker()
Faker.seed(42)

def generate_flight_orders(n, error_probability=0.01):
    NAO_RECEBIDO = "DADO_NAO_RECEBIDO"
    orders = []

    for _ in range(n):
        status = random.choice(["pending", "confirmed", "cancelled"])
        
        flight_id = f"AAA-{random.randint(1, 30)}"
        user_id = str(random.randint(10000, 99999))
        customer_name = faker.name()

        if status in ["pending", "cancelled"] and random.random() < error_probability:
            seat = NAO_RECEBIDO if random.random() > 0.5 else f"{random.randint(1, 40)}{random.choice(['A', 'B', 'C', 'D', 'E', 'F'])}"
            payment_method = NAO_RECEBIDO if random.random() > 0.5 else random.choice(["credit_card", "debit_card", "paypal"])
            reservation_time = NAO_RECEBIDO if random.random() > 0.5 else faker.date_time_between(start_date="-60d", end_date="now").isoformat()
            price = 0 if random.random() > 0.5 else round(random.uniform(200.0, 2000.0), 2)
        else:
            seat = f"{random.randint(1, 40)}{random.choice(['A', 'B', 'C', 'D', 'E', 'F'])}"
            payment_method = random.choice(["credit_card", "debit_card", "paypal"])
            reservation_time = faker.date_time_between(start_date="-60d", end_date="now").isoformat()
            price = round(random.uniform(200.0, 2000.0), 2)

        order = {
            "flight_id": flight_id,
            "seat": seat,
            "user_id": user_id,
            "customer_name": customer_name,
            "status": status,
            "payment_method": payment_method,
            "reservation_time": reservation_time,
            "price": price
        }

        orders.append(order)

    return orders

orders = generate_flight_orders(1000000, error_probability=0.1)

with open("ordersUmMilhao.json", "w") as f:
    json.dump(orders, f, indent=2, default=str)