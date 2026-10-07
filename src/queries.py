from pyspark.sql import functions as F

def run_queries(df_clean):
    print("\n" + "="*50)
    print(" RESULTADOS DEL EXAMEN EVALUATIVO ETL")
    print("="*50 + "\n")

    # Pedidos únicamente entregados
    df_entregados = df_clean.filter(F.col("status") == "Entregado")

    # 1. Producto con mayor cantidad vendida en Entregados
    p1 = df_entregados.groupBy("product").agg(F.sum("quantity").alias("total_qty")).orderBy(F.desc("total_qty")).first()
    print(f"1. Producto con mayor cantidad vendida (Entregados): {p1['product']} ({p1['total_qty']} unidades)")

    # 2. Promedio de unidades devueltas en estado Devuelto
    p2 = df_clean.filter(F.col("status") == "Devuelto").agg(F.avg("returned_qty").alias("avg_dev")).first()
    print(f"2. Promedio unidades devueltas por pedido devuelto: {p2['avg_dev']:.2f}")

    # 3. Ciudad con mayor número de pedidos Entregados
    p3 = df_entregados.groupBy("city").count().orderBy(F.desc("count")).first()
    print(f"3. Ciudad con mayor número de pedidos Entregados: {p3['city']} ({p3['count']} pedidos)")

    # 4. Categoría que genera mayores ingresos en Entregados
    p4 = df_entregados.groupBy("category").agg(F.sum("total_order_val").alias("ingresos")).orderBy(F.desc("ingresos")).first()
    print(f"4. Categoría con mayores ingresos: {p4['category']} (${p4['ingresos']:,.0f} COP)")

    # 5. Canal de venta con mayor cantidad de pedidos Entregados
    p5 = df_entregados.groupBy("channel").count().orderBy(F.desc("count")).first()
    print(f"5. Canal de venta principal: {p5['channel']} ({p5['count']} pedidos)")

    # 6. Promedio del valor de venta por pedido Entregado
    p6 = df_entregados.agg(F.avg("total_order_val").alias("avg_val")).first()
    print(f"6. Promedio valor de venta por pedido: ${p6['avg_val']:,.0f} COP")

    # 7. Departamento con mayor cantidad de unidades vendidas en Entregados
    p7 = df_entregados.groupBy("department").agg(F.sum("quantity").alias("total_qty")).orderBy(F.desc("total_qty")).first()
    print(f"7. Departamento con mayor unidades vendidas: {p7['department']} ({p7['total_qty']} unidades)")

    # 8. Método de pago más usado en Entregados
    p8 = df_entregados.groupBy("payment_method").count().orderBy(F.desc("count")).first()
    print(f"8. Método de pago principal: {p8['payment_method']} ({p8['count']} pedidos)")

    # 9. Promedio de días de envío para pedidos Entregados
    p9 = df_entregados.agg(F.avg("shipping_days").alias("avg_days")).first()
    print(f"9. Promedio días de envío: {p9['avg_days']:.2f} días")

    # 10. Tipo de cliente que genera mayores ingresos en Entregados
    p10 = df_entregados.groupBy("customer_type").agg(F.sum("total_order_val").alias("ingresos")).orderBy(F.desc("ingresos")).first()
    print(f"10. Tipo de cliente con mayores ingresos: {p10['customer_type']} (${p10['ingresos']:,.0f} COP)\n")