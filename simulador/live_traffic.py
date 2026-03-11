import csv
import time
import random
import multiprocessing
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

MONGO_URI = "mongodb://admin:testing@localhost:46100/?authSource=admin&directConnection=true"

NUM_WORKERS = 16
REGISTROS_A_CARGAR = 160_000
BATCH_SIZE = 500

ACCIONES = ["CREATE", "UPDATE", "DELETE"]
PESOS = [70, 20, 10]


def trabajador(worker_id, chunk_datos, ops_array):

    client = MongoClient(MONGO_URI, maxPoolSize=50)
    db = client["SyntheticDB"]
    col = db["Users"]

    ids_locales = []
    batch = []

    for fila in chunk_datos:

        accion = random.choices(ACCIONES, weights=PESOS)[0]

        if accion == "CREATE" or not ids_locales:

            usuario = {
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

            batch.append(usuario)

            if len(batch) >= BATCH_SIZE:

                try:
                    res = col.insert_many(batch, ordered=False)
                    ids_locales.extend(res.inserted_ids)
                    ops_array[worker_id] += len(res.inserted_ids)

                except BulkWriteError as e:
                    ops_array[worker_id] += e.details["nInserted"]

                batch = []

        elif accion == "UPDATE":

            id_random = random.choice(ids_locales)

            col.update_one(
                {"_id": id_random},
                {
                    "$set": {
                        "edad": random.randint(18, 90),
                        "estatus": "actualizado"
                    }
                }
            )

            ops_array[worker_id] += 1

        elif accion == "DELETE":

            id_random = random.choice(ids_locales)

            col.delete_one({"_id": id_random})

            ids_locales.remove(id_random)

            ops_array[worker_id] += 1


def monitor_metricas(ops_array, num_workers):

    prev = [0] * num_workers

    print("\n🚀 Generando tráfico CRUD...\n")

    while True:

        time.sleep(1)

        current = list(ops_array)

        ops = sum(current) - sum(prev)

        print(f"🔥 Throughput: {ops:,} ops/seg")

        prev = current


def preparar_indices():

    client = MongoClient(MONGO_URI)

    db = client["SyntheticDB"]

    col = db["Users"]

    col.drop_indexes()

    col.create_index(
        [
            ("curp", 1),
            ("rfc", 1),
            ("email", 1)
        ],
        unique=True
    )

    print("✅ Índice compuesto listo.\n")


def dividir_chunks(datos, workers):

    size = len(datos) // workers
    chunks = []

    for i in range(workers):

        start = i * size

        if i == workers - 1:
            end = len(datos)
        else:
            end = (i + 1) * size

        chunks.append(datos[start:end])

    return chunks


def iniciar():

    preparar_indices()

    archivo = "usuarios_sinteticos.csv"

    print(f"Cargando {REGISTROS_A_CARGAR:,} registros...\n")

    datos = []

    with open(archivo, encoding="utf-8") as f:

        reader = csv.reader(f)

        next(reader)

        for i, row in enumerate(reader):

            if i >= REGISTROS_A_CARGAR:
                break

            datos.append(row)

    print("Registros cargados:", len(datos))

    chunks = dividir_chunks(datos, NUM_WORKERS)

    ops_array = multiprocessing.Array('i', NUM_WORKERS)

    workers = []

    for i in range(NUM_WORKERS):

        p = multiprocessing.Process(
            target=trabajador,
            args=(i, chunks[i], ops_array)
        )

        workers.append(p)

    monitor = multiprocessing.Process(
        target=monitor_metricas,
        args=(ops_array, NUM_WORKERS),
        daemon=True
    )

    monitor.start()

    for w in workers:
        w.start()

    for w in workers:
        w.join()

    print("\n✅ Simulación de tráfico terminada.")


if __name__ == "__main__":
    iniciar()