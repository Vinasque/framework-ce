import pandas as pd
from faker import Faker
import random

# Inicializando o Faker
fake = Faker()

# Lista de países (com a maioria do Brasil)
countries = ['Brazil', 'USA', 'Argentina', 'Germany', 'Italy', 'France', 'Canada', 'UK', 'Japan', 'Australia']

# Função para gerar dados dos usuários
def gerar_dados_usuarios(n=10000):
    dados = []
    for user_id in range(1, n + 1):
        username = fake.user_name()  # Nome de usuário aleatório
        email = fake.email()  # E-mail aleatório
        airmiles = random.randint(1000, 100000)  # Milhas aleatórias
        # A maioria dos usuários será do Brasil
        country = 'Brazil' if random.random() < 0.8 else random.choice(countries[1:])  # 80% brasileiros
        dados.append([user_id, username, email, airmiles, country])

    # Criando o DataFrame
    df = pd.DataFrame(dados, columns=['user_id', 'username', 'email', 'airmiles', 'country'])

    # Salvando em um arquivo CSV
    df.to_csv('generator/users.csv', index=False)
    
    return df

# Gerando os dados e criando o CSV
dados_usuarios = gerar_dados_usuarios()
