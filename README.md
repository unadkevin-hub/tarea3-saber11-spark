# Tarea 3 – Procesamiento de Datos con Apache Spark

**Curso:** Big Data (202016911) – Universidad Nacional Abierta y a Distancia (UNAD)
**Estudiante:** Kevin Andrés Oliveros Canchila – Grupo 202016911_16

## Problema

Identificar las brechas en el puntaje global de Saber 11 según el tipo de colegio (oficial o no oficial), la zona (urbana o rural), el estrato, el acceso a internet en el hogar y el departamento.

## Conjunto de datos

**Resultados únicos Saber 11** del ICFES, publicado en Datos Abiertos Colombia (ID `kgxf-xxbe`), filtrado al periodo **2022** (`20224`): 1.065.888 registros y 51 columnas (534 MB).
https://www.datos.gov.co/Educaci-n/Resultados-nicos-Saber-11/kgxf-xxbe

## Archivos

| Archivo | Descripción |
|---|---|
| `batch_icfes.py` | Procesamiento en batch con PySpark: carga desde HDFS, limpieza, análisis exploratorio, conteo con RDD (map + reduceByKey) y gráficas |
| `productor_icfes.py` | Productor de Kafka que simula resultados de estudiantes en tiempo real |
| `streaming_icfes.py` | Consumidor con Spark Structured Streaming: promedios por ventana de 1 minuto, acumulado por estrato y alertas de puntajes ≥ 400 |

## Entorno

Ubuntu 22.04 · Java 11 · Hadoop 3.3.6 · Spark 3.5.8 (Scala 2.12) · Kafka 3.9.2 · Python 3.10

```bash
pip3 install pandas matplotlib kafka-python
```

## 1. Procesamiento en batch

```bash
start-all.sh
wget -O saber11_2022.csv 'https://www.datos.gov.co/resource/kgxf-xxbe.csv?$where=periodo=%2720224%27&$limit=2000000'
hdfs dfs -mkdir -p /Tarea3
hdfs dfs -put -f saber11_2022.csv /Tarea3/
spark-submit batch_icfes.py 2>&1 | tee resultado_batch.txt
```

Las gráficas se guardan en la carpeta `graficas/`.

### Resultados principales

- Se encontraron registros duplicados: después de la limpieza quedaron **532.792 estudiantes**.
- Puntaje global promedio: **250,1** (mediana 246).
- Colegios no oficiales: **272,6** · oficiales: **243,6** (brecha de 29 puntos).
- Zona urbana: **254,6** · rural: **228,4**.
- Con internet: **260,2** · sin internet: **228,4**.
- Departamento con mayor promedio: **Bogotá (270,2)** · menor: **Chocó (206,1)**.
- La mayor brecha por prueba entre oficiales y no oficiales está en **inglés** (9,5 puntos).

## 2. Procesamiento en tiempo real (Kafka + Spark Streaming)

```bash
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --replication-factor 1 --partitions 3 --topic saber11_resultados

# Terminal 1
python3 productor_icfes.py

# Terminal 2
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 streaming_icfes.py
```

Consola web de Spark: `http://<IP-de-la-VM>:4040`
