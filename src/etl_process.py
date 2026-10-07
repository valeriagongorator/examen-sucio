import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def get_spark_session(app_name="ETL_Ventas_Colombia"):
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.ansi.enabled", "false") \
        .getOrCreate()

def clean_sales_data(spark, file_path):
    # 1. Cargar datos
    df_raw = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .csv(file_path)

    # 2. Aplicar Reglas de Calidad con conversion tolerante de fecha
    df_clean = df_raw \
        .filter(F.col("order_id").isNotNull()) \
        .withColumn("order_date_ts", F.to_timestamp(F.col("order_date"))) \
        .filter(F.col("order_date_ts").isNotNull() & (F.year(F.col("order_date_ts")) == 2025)) \
        .filter(F.col("city").isNotNull() & F.col("product").isNotNull()) \
        .filter((F.col("quantity") > 0) & (F.col("quantity") <= 20)) \
        .filter((F.col("unit_price_cop") > 0) & (F.col("unit_price_cop") < 10000000)) \
        .filter((F.col("customer_age") >= 18) & (F.col("customer_age") <= 100)) \
        .filter((F.col("shipping_days") >= 1) & (F.col("shipping_days") <= 15)) \
        .filter((F.col("returned_qty") >= 0) & (F.col("returned_qty") <= F.col("quantity"))) \
        .dropDuplicates(["order_id"])

    # 3. Normalización de Cadenas de Texto
    df_clean = df_clean \
        .withColumn("product", F.trim(F.col("product"))) \
        .withColumn("city", F.trim(F.col("city")))

    # Mapeo de variaciones conocidas
    df_clean = df_clean.withColumn("product", 
        F.when(F.lower(F.col("product")) == "smartphone x", "Smartphone X")
         .when(F.lower(F.col("product")) == "mouse inalambrico", "Mouse Inalámbrico")
         .when(F.col("product") == "Laptop Pro14", "Laptop Pro 14")
         .when(F.col("product") == "Audifonos Bluetooth", "Audífonos Bluetooth")
         .otherwise(F.col("product"))
    )

    df_clean = df_clean.withColumn("city", 
        F.when(F.lower(F.col("city")) == "bogota", "Bogotá")
         .when(F.lower(F.col("city")) == "cali", "Cali")
         .when(F.lower(F.col("city")) == "medellin", "Medellín")
         .when(F.lower(F.col("city")) == "cartagena", "Cartagena")
         .when(F.lower(F.col("city")) == "barranquilla", "Barranquilla")
         .when(F.lower(F.col("city")) == "pasto", "Pasto")
         .otherwise(F.col("city"))
    )

    # Columna calculada de Ingreso Bruto por Pedido
    df_clean = df_clean.withColumn("total_order_val", F.col("quantity") * F.col("unit_price_cop"))

    return df_clean