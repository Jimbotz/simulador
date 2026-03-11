from pymongo import MongoClient

def limpiar_base_de_datos():
    print("Conectando a MongoDB para purgar los datos...")
    
    # Misma conexión directa que usamos en el simulador
    MONGO_URI = "mongodb://admin:testing@localhost:46100/?authSource=admin&directConnection=true"
    client = MongoClient(MONGO_URI)
    db = client["SyntheticDB"]
    users_col = db["Users"]
    
    # Contamos cuántos había antes de borrar
    total_antes = users_col.count_documents({})
    
    if total_antes == 0:
        print("La colección 'Users' ya está vacía. No hay nada que borrar.")
        return

    # Borramos todos los documentos (el diccionario vacío {} significa "sin filtros")
    resultado = users_col.delete_many({})
    
    print(f"✅ ¡Limpieza completada! Se eliminaron {resultado.deleted_count} registros.")
    print("La base de datos está lista para una nueva prueba de estrés.")

if __name__ == "__main__":
    limpiar_base_de_datos()