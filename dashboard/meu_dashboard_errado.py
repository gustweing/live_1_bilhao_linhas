import streamlit as st
import duckdb 
import pandas as pd

def create_duckdb():
    result = duckdb.sql("""
    SELECT 
        cidade, 
        min(temperatura) as temperatura_minima, 
        avg(temperatura) as temperatura_avg, 
        max(temperatura) as temperatura_maxima
    FROM read_csv(
               "data/measurements.txt", 
               AUTO_DETECT=FALSE, sep = ';', 
               columns = {
                'cidade':VARCHAR, 
                'temperatura':'DECIMAL(3,1)'
               }
            )
    GROUP BY cidade
    ORDER BY cidade""").show()

    # Convertendo para dataframe
    df = result.df() 
    return df

# Criando o Dashboard
def main():
    st.title("Weather Station Summary")
    st.write("This dashboard shows the summary of weather station")

    #Carregar os dados
    data = create_duckdb()

    #Exibir os dados
    st.dataframe(data)


if __name__ == "__main__":
    main()