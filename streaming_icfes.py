# -*- coding: utf-8 -*-
"""
Spark Structured Streaming: consume el topic saber11_resultados de Kafka.
Uso:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 streaming_icfes.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

TOPIC = "saber11_resultados"

spark = (SparkSession.builder
         .appName("Tarea3_ICFES_Streaming")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

esquema = StructType([
    StructField("estu_consecutivo", StringType()),
    StructField("departamento", StringType()),
    StructField("naturaleza", StringType()),
    StructField("area", StringType()),
    StructField("estrato", StringType()),
    StructField("internet", StringType()),
    StructField("punt_lectura_critica", IntegerType()),
    StructField("punt_matematicas", IntegerType()),
    StructField("punt_c_naturales", IntegerType()),
    StructField("punt_sociales_ciudadanas", IntegerType()),
    StructField("punt_ingles", IntegerType()),
    StructField("punt_global", IntegerType()),
    StructField("timestamp", StringType()),
])

crudo = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("subscribe", TOPIC)
         .option("startingOffsets", "latest")
         .load())

eventos = (crudo.select(F.from_json(F.col("value").cast("string"), esquema).alias("e"))
           .select("e.*")
           .withColumn("ts", F.to_timestamp("timestamp")))

# Consulta 1: estudiantes y promedio por naturaleza en ventanas de 1 minuto
por_naturaleza = (eventos.withWatermark("ts", "2 minutes")
                  .groupBy(F.window("ts", "1 minute"), "naturaleza")
                  .agg(F.count("*").alias("estudiantes"),
                       F.round(F.avg("punt_global"), 1).alias("promedio_global"),
                       F.round(F.avg("punt_matematicas"), 1).alias("prom_matematicas"),
                       F.round(F.avg("punt_ingles"), 1).alias("prom_ingles"))
                  .select(F.col("window.start").alias("inicio"),
                          F.col("window.end").alias("fin"),
                          "naturaleza", "estudiantes", "promedio_global",
                          "prom_matematicas", "prom_ingles"))

q1 = (por_naturaleza.writeStream.outputMode("update").format("console")
      .option("truncate", False).trigger(processingTime="10 seconds")
      .queryName("por_naturaleza").start())

# Consulta 2: promedio acumulado por estrato
por_estrato = (eventos.groupBy("estrato")
               .agg(F.count("*").alias("estudiantes"),
                    F.round(F.avg("punt_global"), 1).alias("promedio_global"))
               .orderBy("estrato"))

q2 = (por_estrato.writeStream.outputMode("complete").format("console")
      .option("truncate", False).trigger(processingTime="10 seconds")
      .queryName("por_estrato").start())

# Consulta 3: alertas de puntajes altos (>= 400)
alertas = (eventos.filter(F.col("punt_global") >= 400)
           .select("ts", "estu_consecutivo", "departamento", "naturaleza",
                   "estrato", "punt_global"))

q3 = (alertas.writeStream.outputMode("append").format("console")
      .option("truncate", False).queryName("alertas").start())

spark.streams.awaitAnyTermination()
