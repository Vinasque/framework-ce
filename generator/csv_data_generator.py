import pandas as pd
from faker import Faker
import random

# Inicializando o Faker
fake = Faker()

# Função para gerar dados dos usuários
def gerar_dados_usuarios(n=1000):
    dados = []
    for user_id in range(1, n+1):
        username = fake.user_name()  # Nome de usuário aleatório
        email = fake.email()  # E-mail aleatório
        airmiles = random.randint(1000, 100000)  # Milhas aleatórias
        dados.append([user_id, username, email, airmiles])

    # Criando o DataFrame
    df = pd.DataFrame(dados, columns=['user_id', 'username', 'email', 'airmiles'])

    # Salvando em um arquivo CSV
    df.to_csv('users.csv', index=False)
    
    return df

# Gerando os dados e criando o CSV
dados_usuarios = gerar_dados_usuarios()
