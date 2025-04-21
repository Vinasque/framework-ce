import random
from faker import Faker
import csv

# Inicializando o Faker
fake = Faker()

# Função para gerar os dados do hotel
def gerar_hoteis(n):
    hoteis = []
    cidades = ["Porto Alegre", "Rio de Janeiro", "São Paulo", "Curitiba", "Belém"]
    ratings_prob = [0.5, 0.3, 0.1, 0.07, 0.03]  # Probabilidade para as classificações de estrelas (1-5)
    
    for i in range(n):
        hotel_id = i + 1
        location = random.choices(cidades, weights=[0.2, 0.4, 0.15, 0.15, 0.1])[0]
        remaining_rooms = random.randint(10, 50)
        rating = random.choices([1, 2, 3, 4, 5], weights=ratings_prob)[0]
        
        hoteis.append([hotel_id, location, remaining_rooms, rating])
    
    return hoteis

# Função para gerar os dados dos quartos
def gerar_quartos(hoteis):
    quartos = []
    
    for hotel in hoteis:
        hotel_id = hotel[0]
        rating = hotel[3]
        
        # Definir faixa de preço dependendo do rating
        if rating == 1:
            price_range = (100, 250)
        elif rating == 2:
            price_range = (200, 400)
        elif rating == 3:
            price_range = (300, 800)
        elif rating == 4:
            price_range = (500, 2000)
        else:  # rating == 5
            price_range = (1000, 10000)
        
        room_id = 1
        for _ in range(hotel[2]):  # Para o número de quartos disponíveis
            cost = random.randint(*price_range)
            room_capacity = random.randint(1, 8)
            
            # Determinar o tipo de quarto
            if room_capacity == 1:
                room_type = "single"
            elif room_capacity in [2, 3]:
                room_type = "partners"
            else:
                room_type = "family"
            
            # Adicionar o quarto à lista
            quartos.append([hotel_id, room_id, 0, cost, room_type, room_capacity])
            room_id += 1
    
    return quartos

# Gerar os hoteis e quartos
hoteis = gerar_hoteis(1000)
quartos = gerar_quartos(hoteis)

# Salvar hotel.txt
with open('generator/hotel.txt', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file, delimiter=';')
    writer.writerow(["hotel_id", "location", "remaining_rooms", "rating"])
    writer.writerows(hoteis)

# Salvar hotel_rooms.txt
with open('generator/hotel_rooms.txt', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file, delimiter=';')
    writer.writerow(["hotel_id", "room_id", "taken", "cost", "type", "room_capacity"])
    writer.writerows(quartos)

print("Arquivos hotel.txt e hotel_rooms.txt gerados com sucesso!")
