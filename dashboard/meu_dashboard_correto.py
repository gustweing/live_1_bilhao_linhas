import streamlit as st
import duckdb 
import pandas as pd

def load_data():
    con = duckdb.connect()
    df = con.execute("SELECT * FROM 'data\measurements_summary.parquet' ").df()
    con.close()
    return df 

# Criando o Dashboard
def main():
    st.title("Weather Station Summary")
    st.write("This dashboard shows the summary of weather station")

    #Carregar os dados
    data = load_data()

    #Exibir os dados
    st.dataframe(data)


if __name__ == "__main__":
    main()