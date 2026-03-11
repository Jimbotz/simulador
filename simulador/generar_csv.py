import csv
import time
import random # Agregamos la librería random aquí
from faker_persona_mx import PersonaGenerator
import logging

# Silenciar logs
logging.getLogger("faker_persona_mx").setLevel(logging.CRITICAL)

def crear_dataset_masivo(total_registros=2_000_000, tamaño_lote=10_000):
    archivo_salida = "usuarios_sinteticos.csv"
    generator = PersonaGenerator()
    
    print(f"Iniciando generación de {total_registros:,} registros...")
    tiempo_inicio = time.time()
    
    with open(archivo_salida, mode='w', newline='', encoding='utf-8') as archivo:
        writer = csv.writer(archivo)
        # 1. Agregamos el encabezado "edad"
        writer.writerow(["nombre", "apellido_paterno", "apellido_materno", "curp", "rfc", "email", "telefono", "edad"])
        
        lotes_necesarios = total_registros // tamaño_lote
        
        for i in range(lotes_necesarios):
            lote_personas = generator.generate_batch(tamaño_lote)
            
            # 2. Añadimos el cálculo aleatorio directamente al momento de armar la fila
            filas = [
                [
                    p.nombre, p.apellido_paterno, p.apellido_materno, 
                    p.curp, p.rfc, p.email, p.telefono, 
                    random.randint(15, 90) # <-- Edad generada y guardada
                ]
                for p in lote_personas
            ]
            
            writer.writerows(filas)
            
            if (i + 1) % 10 == 0:
                print(f"Progreso: {(i + 1) * tamaño_lote:,} / {total_registros:,} registros guardados...")

    tiempo_total = time.time() - tiempo_inicio
    print(f"✅ ¡Completado! Archivo {archivo_salida} creado en {tiempo_total:.2f} segundos.")

if __name__ == "__main__":
    crear_dataset_masivo()