import sqlite3
import plotly.express as px
import pandas as pd
import streamlit as st

# Função para ler dados do SQLite e retornar como DataFrames
def read_data_from_sqlite(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT payment_method, price FROM faturamentoMetodo_orders_4")
    payment_rows = cursor.fetchall()

    cursor.execute("SELECT reservation_time, price FROM faturamento_orders_4")
    date_rows = cursor.fetchall()

    cursor.execute("SELECT user_country, price FROM faturamentoPaisUsuario_orders_4")
    userC_rows = cursor.fetchall()

    cursor.execute("SELECT seat_type, price FROM faturamentoTipoAssento_orders_4")
    seatT_rows = cursor.fetchall()

    cursor.execute("SELECT flight_number, reservation_count FROM flight_stats_orders_4")
    flight_stats = cursor.fetchall()

    cursor.execute("SELECT destination, reservation_count FROM destination_stats_orders_4")
    destination_stats = cursor.fetchall()

    conn.close()

    payment_df = pd.DataFrame(payment_rows, columns=["payment_method", "price"])
    date_df = pd.DataFrame(date_rows, columns=["reservation_time", "price"])
    userC_df = pd.DataFrame(userC_rows, columns=["user_country", "price"])
    seatT_df = pd.DataFrame(seatT_rows, columns=["seat_type", "price"])
    flight_df = pd.DataFrame(flight_stats, columns=["flight_number", "reservation_count"])
    destination_df = pd.DataFrame(destination_stats, columns=["destination", "reservation_count"])

    # Top 10 voos com mais reservas
    top_flights = flight_df.sort_values(by="reservation_count", ascending=False).head(10)

    # Arredondar com 2 casas decimais
    for df in [payment_df, date_df, userC_df, seatT_df]:
        df["price"] = df["price"].astype(float).round(2)
    for df in [flight_df, destination_df, top_flights]:
        df["reservation_count"] = df["reservation_count"].astype(float).round(2)

    return payment_df, date_df, userC_df, seatT_df, top_flights, destination_df

# Função auxiliar para formatar valores em real com pontos a cada milhar
def format_brl(value):
    return "R$ " + "{:,.2f}".format(value).replace(",", "X").replace(".", ",").replace("X", ".")

def plot_revenue(payment_df, date_df, userC_df, seatT_df, top_flights, destination_df):
    date_df['reservation_time'] = pd.to_datetime(date_df['reservation_time'])
    date_df = date_df.sort_values(by="reservation_time")  # Garantir que as datas estão ordenadas
    date_df = date_df.iloc[1:-1]
    
    st.subheader("📈 Receita Diária")
    st.markdown("Evolução da receita ao longo do tempo, considerando todas as vendas de passagem.")
    fig1 = px.line(date_df, x="reservation_time", y="price", title="Receita Diária")
    fig1.update_traces(mode='lines+markers', hovertemplate='Data: %{x}<br>Receita: %{y:,.2f} R$')
    st.plotly_chart(fig1)

    st.divider()

    st.subheader("💳 Receita por Método de Pagamento")
    st.markdown("Total de receita agrupado por método de pagamento utilizado pelos clientes.")
    payment_df = payment_df.sort_values(by="price", ascending=False)
    fig2 = px.bar(payment_df, x="payment_method", y="price", title="Receita por Método de Pagamento")
    fig2.update_traces(hovertemplate='Método: %{x}<br>Receita: %{y:,.2f} R$')
    st.plotly_chart(fig2)

    st.divider()

    st.subheader("🌍 Receita por País do Usuário")
    st.markdown("Receita total gerada, agrupada por país do usuário.")
    userC_df = userC_df.sort_values(by="price", ascending=False)
    fig3 = px.bar(userC_df, x="user_country", y="price", title="Receita por País do Usuário")
    fig3.update_traces(hovertemplate='País: %{x}<br>Receita: %{y:,.2f} R$')
    st.plotly_chart(fig3)

    st.divider()

    st.subheader("🪑 Receita por Tipo de Assento")
    st.markdown("Receita total por tipo de assento reservado (Econômico ou Primeira Classe).")
    seatT_df = seatT_df.sort_values(by="price", ascending=False)
    fig4 = px.bar(seatT_df, x="seat_type", y="price", title="Receita por Tipo de Assento")
    fig4.update_traces(hovertemplate='Assento: %{x}<br>Receita: %{y:,.2f} R$')
    st.plotly_chart(fig4)

    st.divider()

    st.subheader("✈️ Top 10 Voos com Mais Reservas")
    st.markdown("Lista dos 10 voos mais populares com base no número de reservas.")
    top_flights = top_flights.sort_values(by="reservation_count", ascending=False)
    top_flights["flight_number"] = "Voo " + top_flights["flight_number"].astype(str)
    fig5 = px.bar(top_flights, x="flight_number", y="reservation_count", title="Top 10 Voos com Mais Reservas")
    fig5.update_traces(hovertemplate='%{x}<br>Reservas: %{y:,.0f}')
    st.plotly_chart(fig5)

    st.divider()

    st.subheader("📍 Destinos Mais Populares")
    st.markdown("Quantidade de reservas por destino final (país de chegada do avião).")
    destination_df = destination_df.sort_values(by="reservation_count", ascending=False)
    fig6 = px.bar(destination_df, x="destination", y="reservation_count", title="Destinos Mais Populares")
    fig6.update_traces(hovertemplate='Destino: %{x}<br>Reservas: %{y:,.0f}')
    st.plotly_chart(fig6)

def main():
    st.set_page_config(page_title="Dashboard de Vendas de Voos")

    st.markdown("""
        <div style="text-align: center;">
            <h4>FGV - Computação Escalável</h4>
            <h1 style="margin-top: 20px;">MICRO-FRAMEWORK: <br> VENDA DE PASSAGENS AÉREAS</h1>
            <h6 style="margin-top: 20px;">Guilherme Buss</h6>
            <h6>Guilherme Carvalho</h6>
            <h6>Gustavo Bianchi</h6>
            <h6>João Gabriel</h6>
            <h6>Vinícius Nascimento</h6>
        </div>
    """, unsafe_allow_html=True)

    st.title("📊 Dashboard de Análise de Vendas de Voos")
    st.markdown("Este painel interativo mostra estatísticas detalhadas sobre as vendas de voos, "
                "permitindo analisar receitas, preferências dos clientes e desempenho de rotas. "
                "Todos gráficos são iterativos!")

    db_path = '../databases/Database.db'

    try:
        payment_df, date_df, userC_df, seatT_df, top_flights, destination_df = read_data_from_sqlite(db_path)
        plot_revenue(payment_df, date_df, userC_df, seatT_df, top_flights, destination_df)
    except Exception as e:
        st.error(f"Erro ao carregar os dados do banco de dados: {e}")

if __name__ == "__main__":
    main()
