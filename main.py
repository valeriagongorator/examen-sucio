from src.etl_process import get_spark_session, clean_sales_data
from src.queries import run_queries

def main():
    # Inicializar Spark
    spark = get_spark_session()
    
    # Ruta al dataset
    data_path = "data/ventas_colombia_sucio.csv"
    
    # Ejecutar proceso ETL
    print("Iniciando proceso ETL...")
    df_clean = clean_sales_data(spark, data_path)
    
    # Ejecutar consultas del examen
    run_queries(df_clean)
    
    # Detener sesión de Spark
    spark.stop()

if __name__ == "__main__":
    main()