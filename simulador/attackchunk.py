import csv
import time
import random
import multiprocessing
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

MONGO_URI = "mongodb://admin:testing@localhost:46100/?authSource=admin&directConnection=true"

NUM_WORKERS = 16
REGISTROS_A_CARGAR = 160_000
BATCH_SIZE = 1000


def trabajador_db(worker_id, chunk_datos, ops_array):

    client = MongoClient(MONGO_URI, maxPoolSize=50)
    db = client["SyntheticDB"]
    users_col = db["Users"]

    batch = []

    for fila in chunk_datos:

        nuevo_usuario = {
            "nombre": fila[0],
            "apellido_paterno": fila[1],
            "apellido_materno": fila[2],
            "curp": fila[3],
            "rfc": fila[4],
            "email": fila[5],
            "telefono": fila[6],
            "edad": int(fila[7]),
            "estatus": "activo"
        }

        batch.append(nuevo_usuario)

        if len(batch) >= BATCH_SIZE:
            try:
                users_col.insert_many(batch, ordered=False)
                ops_array[worker_id] += len(batch)
            except BulkWriteError as e:
                # contamos solo los insertados
                ops_array[worker_id] += e.details["nInserted"]

            batch = []

    # insertar lo que quedó
    if batch:
        try:
            users_col.insert_many(batch, ordered=False)
            ops_array[worker_id] += len(batch)
        except BulkWriteError as e:
            ops_array[worker_id] += e.details["nInserted"]


def monitor_metricas(ops_array, num_workers):

    conteos_anteriores = [0] * num_workers

    print("\n🚀 ¡Ataque iniciado! Calculando operaciones por segundo...\n")

    while True:

        time.sleep(1)

        conteos_actuales = list(ops_array)

        ops_por_segundo = sum(conteos_actuales) - sum(conteos_anteriores)

        print(f"🔥 Rendimiento: {ops_por_segundo:,} inserts/segundo")

        conteos_anteriores = conteos_actuales


def preparar_indices():

    print("Configurando índice compuesto en MongoDB...")

    client = MongoClient(MONGO_URI)
    db = client["SyntheticDB"]
    users_col = db["Users"]

    # eliminar índices previos
    users_col.drop_indexes()

    # índice compuesto para evitar duplicados exactos
    users_col.create_index(
        [
            ("curp", 1),
            ("rfc", 1),
            ("email", 1)
        ],
        unique=True
    )

    print("✅ Índice compuesto creado.\n")


def dividir_chunks(datos, num_workers):

    chunk_size = len(datos) // num_workers
    chunks = []

    for i in range(num_workers):

        start = i * chunk_size

        if i == num_workers - 1:
            end = len(datos)
        else:
            end = (i + 1) * chunk_size

        chunks.append(datos[start:end])

    return chunks


def iniciar_ataque():

    preparar_indices()

    archivo_csv = "usuarios_sinteticos.csv"

    print(f"Cargando {REGISTROS_A_CARGAR:,} registros desde el CSV...")

    datos_completos = []

    with open(archivo_csv, mode='r', encoding='utf-8') as f:

        reader = csv.reader(f)

        next(reader)

        for i, row in enumerate(reader):

            if i >= REGISTROS_A_CARGAR:
                break

            datos_completos.append(row)

    print("Registros cargados en RAM:", len(datos_completos))

    chunks = dividir_chunks(datos_completos, NUM_WORKERS)

    ops_array = multiprocessing.Array('i', NUM_WORKERS)

    procesos = []

    for i in range(NUM_WORKERS):

        p = multiprocessing.Process(
            target=trabajador_db,
            args=(i, chunks[i], ops_array)
        )

        procesos.append(p)

    monitor = multiprocessing.Process(
        target=monitor_metricas,
        args=(ops_array, NUM_WORKERS),
        daemon=True
    )

    monitor.start()

    for p in procesos:
        p.start()

    for p in procesos:
        p.join()

    print("\n✅ Prueba de estrés finalizada.")


if __name__ == "__main__":
    iniciar_ataque()