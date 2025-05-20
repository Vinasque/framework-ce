import pandas as pd
import random
import event_pb2
from faker import Faker

faker = Faker()

df_users = pd.read_csv("generator/users.csv")
user_map = df_users.set_index("user_id")["username"].to_dict()
user_ids = list(user_map.keys())

df_seats = pd.read_csv("generator/flights_seats.csv")
df_available_seats = df_seats[df_seats["taken"] == 0].copy()

assert len(df_available_seats) > 0, "Não há assentos disponíveis!"

def generate_random_event():
    seat_row = df_available_seats.sample(n=1).iloc[0]
    flight_id = f"AAA-{seat_row['flight_id']}"
    seat = seat_row["seat"]
    price = round(seat_row["price"], 2)

    user_id = random.choice(user_ids)
    customer_name = user_map[user_id]

    status = random.choices(
        ["pending", "confirmed", "cancelled"],
        weights=[0.05, 0.90, 0.05],
        k=1
    )[0]

    payment_method = random.choices(
        ["credit_card", "debit_card", "pix", "paypal"],
        weights=[0.46, 0.34, 0.17, 0.03],
        k=1
    )[0]

    reservation_time = faker.date_time_between(start_date="-600d", end_date="now").isoformat()

    return 0