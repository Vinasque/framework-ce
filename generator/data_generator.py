import json
import csv
import random
from faker import Faker
from datetime import datetime, timedelta

faker = Faker()
Faker.seed(42)

def generate_flight_data(n):
    flights = []
    header = ["flight_id", "origin", "destination", "flight_date"]
    flights.append(header)

    for i in range(n):
        # Escolhendo dados aleatoriamente
        flight_id = f"AAA-{i}"
        origin = faker.country()
        destination = faker.country()
        while destination == origin:  
            destination = faker.country()
        flight_datetime = faker.date_time_between(start_date="now", end_date="+600d")
        flight_datetime = flight_datetime.replace(minute=0, second=0, microsecond=0)
        flight_date = flight_datetime.isoformat()

        flight = [flight_id, origin, destination, flight_date]

        flights.append(flight)

    return flights

def generate_flight_orders(n, num_flights):
    orders = []

    for _ in range(n):
        # Escolhendo dados aleatoriamente
        flight_id = random.choice([f"AAA-{i}" for i in range(1, num_flights)])
        seat = f"{random.randint(1, 40)}{random.choice(['A', 'B', 'C', 'D', 'E', 'F'])}"
        user_id = str(random.randint(10000, 99999))
        customer_name = faker.name()
        status = random.choice(["pending", "confirmed", "cancelled"])
        payment_method = random.choice(["credit_card", "debit_card", "paypal"])
        reservation_time = faker.date_time_between(start_date = "-600d", end_date = "now").isoformat()
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

num_flights = 100
flights = generate_flight_data(num_flights)
with open("flightsCem.csv", "w", newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(flights)

orders = generate_flight_orders(100000, num_flights) 
with open("ordersCemMil.json", "w") as f:
    json.dump(orders, f, indent=2)