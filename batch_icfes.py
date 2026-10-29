# -*- coding: utf-8 -*-
"""
Tarea 3 - Procesamiento de Datos con Apache Spark (Big Data UNAD 202016911)
Procesamiento en batch: Resultados únicos Saber 11 (ICFES), periodo 2022 (datos.gov.co, kgxf-xxbe)

Problema: medir las brechas en el puntaje global según naturaleza del colegio,
zona, estrato, acceso a internet y departamento.

Uso:
    spark-submit batch_icfes.py [ruta_csv]
    (por defecto lee hdfs://localhost:9000/Tarea3/saber11_2022.csv)
"""
import os
import re
import sys
import unicodedata
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RUTA = sys.argv[1] if len(sys.argv) > 1 else "hdfs://localhost:9000/Tarea3/saber11_2022.csv"
CARPETA_GRAFICAS = "graficas"

spark = (SparkSession.builder
         .appName("Tarea3_ICFES_Batch")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def titulo(texto):
    print("\n" + "=" * 70)
    print(texto)
    print("=" * 70)


def normalizar(nombre):
    """'PUNT_GLOBAL' o 'Punt Global' -> 'punt_global' (sin tildes ni espacios)."""
    n = unicodedata.normalize("NFKD", nombre).encode("ascii", "ignore").decode()
    return re.sub(r"[^0-9a-z]+", "_", n.strip().lower()).strip("_")


# ---------------------------------------------------------------------------
# 1. CARGA
# ---------------------------------------------------------------------------
titulo("1. Carga del conjunto de datos")
df = (spark.read
      .option("header", True)
      .option("escape", '"')
      .csv(RUTA))
df = df.toDF(*[normalizar(c) for c in df.columns])
total_original = df.count()
print(f"Registros cargados: {total_original:,}")
print(f"Columnas: {len(df.columns)}")

CATEGORICAS = ["cole_naturaleza", "cole_area_ubicacion", "cole_jornada",
               "cole_depto_ubicacion", "fami_estratovivienda",
               "fami_tieneinternet", "estu_genero"]
PUNTAJES = ["punt_lectura_critica", "punt_matematicas", "punt_c_naturales",
            "punt_sociales_ciudadanas", "punt_ingles", "punt_global"]
OBLIGATORIAS = ["cole_naturaleza", "cole_area_ubicacion", "cole_depto_ubicacion",
                "fami_estratovivienda", "punt_global"]

faltantes = [c for c in OBLIGATORIAS if c not in df.columns]
if faltantes:
    print("Faltan columnas obligatorias:", faltantes)
    print("Columnas disponibles:", df.columns)
    spark.stop()
    sys.exit(1)

categoricas = [c for c in CATEGORICAS if c in df.columns]
puntajes = [c for c in PUNTAJES if c in df.columns]
extra = ["estu_consecutivo"] if "estu_consecutivo" in df.columns else []
datos = df.select(*(extra + categoricas + puntajes))

# ---------------------------------------------------------------------------
# 2. LIMPIEZA Y TRANSFORMACIÓN
# ---------------------------------------------------------------------------
titulo("2. Limpieza y transformación")
print("Valores nulos por columna antes de limpiar:")
datos.select([F.sum(F.col(c).isNull().cast("int")).alias(c)
              for c in categoricas + puntajes]).show(truncate=False, vertical=True)

for c in categoricas:
    datos = datos.withColumn(c, F.upper(F.trim(F.col(c))))
    datos = datos.withColumn(
        c, F.when(F.col(c).isNull() | (F.col(c) == ""), "SIN DATO").otherwise(F.col(c)))

for c in puntajes:
    datos = datos.withColumn(c, F.regexp_replace(F.col(c), ",", ".").cast("double"))

if extra:
    datos = datos.dropDuplicates(["estu_consecutivo"])

datos = datos.filter(F.col("punt_global").isNotNull() &
                     F.col("punt_global").between(0, 500))
datos.cache()
total_limpio = datos.count()
print(f"Registros después de limpiar: {total_limpio:,} "
      f"(se descartaron {total_original - total_limpio:,})")

# ---------------------------------------------------------------------------
# 3. ANÁLISIS EXPLORATORIO
# ---------------------------------------------------------------------------
titulo("3. Estadísticas del puntaje global")
datos.select("punt_global").describe().show()
q = datos.approxQuantile("punt_global", [0.25, 0.5, 0.75], 0.01)
print(f"P25: {q[0]}  Mediana: {q[1]}  P75: {q[2]}")


def promedio_por(col):
    return (datos.groupBy(col)
            .agg(F.count("*").alias("estudiantes"),
                 F.round(F.avg("punt_global"), 1).alias("promedio_global"))
            .withColumn("pct_estudiantes",
                        F.round(F.col("estudiantes") * 100 / total_limpio, 2)))


titulo("4. Puntaje global por naturaleza del colegio")
por_naturaleza = promedio_por("cole_naturaleza").orderBy(F.desc("promedio_global"))
por_naturaleza.show(truncate=False)

titulo("5. Puntaje global por zona (urbana / rural)")
por_area = promedio_por("cole_area_ubicacion").orderBy(F.desc("promedio_global"))
por_area.show(truncate=False)

titulo("6. Puntaje global por estrato")
por_estrato = promedio_por("fami_estratovivienda").orderBy("fami_estratovivienda")
por_estrato.show(truncate=False)

if "fami_tieneinternet" in categoricas:
    titulo("7. Puntaje global según acceso a internet")
    promedio_por("fami_tieneinternet").orderBy(F.desc("promedio_global")).show(truncate=False)

titulo("8. Departamentos con mayor y menor promedio")
por_depto = (datos.groupBy("cole_depto_ubicacion")
             .agg(F.count("*").alias("estudiantes"),
                  F.round(F.avg("punt_global"), 1).alias("promedio_global"))
             .filter(F.col("estudiantes") >= 100))
print("Top 10:")
por_depto.orderBy(F.desc("promedio_global")).show(10, truncate=False)
print("Últimos 10:")
por_depto.orderBy("promedio_global").show(10, truncate=False)

titulo("9. Brecha por prueba: no oficial vs oficial")
pruebas = [c for c in puntajes if c != "punt_global"]
brecha = (datos.filter(F.col("cole_naturaleza").isin("OFICIAL", "NO OFICIAL"))
          .groupBy("cole_naturaleza")
          .agg(*[F.round(F.avg(c), 1).alias(c) for c in pruebas]))
brecha.show(truncate=False)

# ---------------------------------------------------------------------------
# 10. MISMO CONTEO CON RDD (map + reduceByKey)
# ---------------------------------------------------------------------------
titulo("10. Estudiantes por estrato usando RDD (map + reduceByKey)")
rdd_estrato = (datos.select("fami_estratovivienda").rdd
               .map(lambda fila: (fila[0], 1))
               .reduceByKey(add))
for estrato, n in sorted(rdd_estrato.collect()):
    print(f"{estrato:<20} {n:>10,}")

# ---------------------------------------------------------------------------
# 11. GRÁFICAS
# ---------------------------------------------------------------------------
titulo("11. Gráficas")
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    os.makedirs(CARPETA_GRAFICAS, exist_ok=True)

    def barras(pdf, x, y, titulo_g, archivo, horizontal=False):
        fig, ax = plt.subplots(figsize=(10, 6))
        if horizontal:
            ax.barh(pdf[x], pdf[y], color="#378ADD")
            ax.set_xlabel("Puntaje global promedio")
        else:
            ax.bar(pdf[x], pdf[y], color="#378ADD")
            ax.set_ylabel("Puntaje global promedio")
            plt.xticks(rotation=30, ha="right")
        ax.set_title(titulo_g)
        plt.tight_layout()
        plt.savefig(os.path.join(CARPETA_GRAFICAS, archivo), dpi=120)
        plt.close()
        print("Guardada:", archivo)

    barras(por_naturaleza.toPandas(), "cole_naturaleza", "promedio_global",
           "Puntaje global por naturaleza del colegio", "1_naturaleza.png")
    barras(por_area.toPandas(), "cole_area_ubicacion", "promedio_global",
           "Puntaje global por zona", "2_zona.png")
    barras(por_estrato.toPandas(), "fami_estratovivienda", "promedio_global",
           "Puntaje global por estrato", "3_estrato.png")
    deptos = por_depto.orderBy("promedio_global").toPandas()
    barras(deptos, "cole_depto_ubicacion", "promedio_global",
           "Puntaje global promedio por departamento", "4_departamentos.png",
           horizontal=True)

    fraccion = min(1.0, 200000 / max(total_limpio, 1))
    muestra = datos.select("punt_global").sample(fraccion, seed=42).toPandas()
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(muestra["punt_global"], bins=50, color="#1D9E75")
    ax.set_xlabel("Puntaje global")
    ax.set_ylabel("Estudiantes (muestra)")
    ax.set_title("Distribución del puntaje global")
    plt.tight_layout()
    plt.savefig(os.path.join(CARPETA_GRAFICAS, "5_distribucion.png"), dpi=120)
    plt.close()
    print("Guardada: 5_distribucion.png")
except ImportError:
    print("matplotlib no está instalado: pip3 install matplotlib pandas")

spark.stop()
