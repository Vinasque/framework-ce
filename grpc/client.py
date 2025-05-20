import grpc
import time
import random
import event_pb2
import event_pb2_grpc
import pandas as pd
from faker import Faker
from concurrent import futures

faker = Faker()

df_users = pd.read_csv("generator/users.csv")
user_map = df_users.set_index("user_id")["username"].to_dict()
user_ids = list(user_map.keys())

df_seats = pd.read_csv("generator/flights_seats.csv")
df_available_seats = df_seats[df_seats["taken"] == 0].copy()

assert len(df_available_seats) > 0, "Não há assentos disponíveis!"

# Gera um evento aleatório com base nos dados "reais"
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

    # Monta o evento no formato gRPC
    return event_pb2.Event(
        flight_id=flight_id,
        seat=seat,
        user_id=str(user_id),
        customer_name=customer_name,
        status=status,
        payment_method=payment_method,
        reservation_time=reservation_time,
        price=str(price)
    )

# Representa um cliente que envia vários eventos
def run(client_id=0, repetitions=5, sleep_between=1):
    channel = grpc.insecure_channel('localhost:50051') # endereço padrão do servidor
    stub = event_pb2_grpc.EventServiceStub(channel)

    for i in range(repetitions):
        event = generate_random_event()
        response = stub.SendEvent(event)
        print(f"[Client {client_id}] Received: {response.message}")
        time.sleep(sleep_between) # pausa entre eventos

# Executando múltiplos clientes em paralelo
if __name__ == '__main__':
    import sys
    import threading

    # Nº de clientes e nº de eventos por cliente são argumentos opcionais
    num_clients = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    events_per_client = int(sys.argv[2]) if len(sys.argv) > 2 else 5

    threads = []
    for i in range(num_clients):
        t = threading.Thread(target=run, args=(i, events_per_client))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()