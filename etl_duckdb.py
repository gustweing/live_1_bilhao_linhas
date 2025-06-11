import duckdb 
import time 

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
    ORDER BY cidade""")

    result.show()

    #Save results to a parquet file 
    result.write_parquet('data/measurements_summary.parquet')


if __name__ == '__main__':
    import time
    start_time = time.time()
    create_duckdb()
    took = time.time() - start_time
    print(f"Duckdb time: {took:.2f} segundos")