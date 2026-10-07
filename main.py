from src.etl_process import get_spark_session, clean_sales_data
from src.queries import run_queries

def main():
    # 1. Iniciar la sesión de Spark en el equipo local
    spark = get_spark_session()
    
    # 2. Definir la ruta relativa de la base de datos
    data_path = "data/ventas_colombia_sucio.csv"
    
    # 3. Ejecutar la función de limpieza ETL
    print("Iniciando proceso ETL...")
    df_clean = clean_sales_data(spark, data_path)
    
    # 4. Pasar los datos limpios a las consultas para calcular las respuestas
    run_queries(df_clean)
    
    # 5. Cerrar la sesión de Spark para liberar memoria del sistema
    spark.stop()

if __name__ == "__main__":
    main()