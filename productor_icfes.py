# -*- coding: utf-8 -*-
"""
Generador de datos para Kafka: simula resultados de Saber 11 que van llegando.
Uso: python3 productor_icfes.py
"""
import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

TOPIC = "saber11_resultados"
DEPARTAMENTOS = ["SUCRE", "BOLIVAR", "CORDOBA", "ATLANTICO", "ANTIOQUIA",
                 "BOGOTA", "VALLE", "SANTANDER", "CHOCO", "NARIÑO"]
ESTRATOS = ["ESTRATO 1", "ESTRATO 2", "ESTRATO 3", "ESTRATO 4",
            "ESTRATO 5", "ESTRATO 6"]

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)


def nota(base):
    return max(0, min(100, round(random.gauss(base, 12))))


consecutivo = 0
print(f"Enviando resultados al topic '{TOPIC}'... (Ctrl+C para detener)")
while True:
    consecutivo += 1
    naturaleza = random.choices(["OFICIAL", "NO OFICIAL"], weights=[75, 25])[0]
    area = random.choices(["URBANO", "RURAL"], weights=[80, 20])[0]
    estrato = random.choices(ESTRATOS, weights=[35, 35, 18, 6, 3, 3])[0]
    internet = random.choices(["SI", "NO"], weights=[70, 30])[0]

    base = 47
    base += 5 if naturaleza == "NO OFICIAL" else 0
    base -= 3 if area == "RURAL" else 0
    base += 2 * ESTRATOS.index(estrato)
    base += 2 if internet == "SI" else -2
    base += random.gauss(0, 12)

    lc, mat, cn, sc = nota(base), nota(base), nota(base), nota(base - 2)
    ing = nota(base + (4 if naturaleza == "NO OFICIAL" else -2))
    # Ponderación del examen: 3 para las cuatro pruebas y 1 para inglés
    glob = round(5 * (3 * (lc + mat + cn + sc) + ing) / 13)

    evento = {
        "estu_consecutivo": f"SIM{consecutivo:07d}",
        "departamento": random.choice(DEPARTAMENTOS),
        "naturaleza": naturaleza,
        "area": area,
        "estrato": estrato,
        "internet": internet,
        "punt_lectura_critica": lc,
        "punt_matematicas": mat,
        "punt_c_naturales": cn,
        "punt_sociales_ciudadanas": sc,
        "punt_ingles": ing,
        "punt_global": glob,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }
    producer.send(TOPIC, value=evento)
    print("Enviado:", evento["estu_consecutivo"], naturaleza, estrato, glob)
    time.sleep(0.5)
