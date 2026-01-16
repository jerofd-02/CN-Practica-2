import boto3
import json
import time
from loguru import logger
import datetime

# Configuración
STREAM_NAME = 'flights-stream'      # Nombre del Kinesis Data Stream
REGION = 'us-east-1' # Cambia si usas otra región
INPUT_FILE = 'flights_sample_first_half_april_2020.json' # Dataset de entrada

# Cliente de Kinesis
kinesis = boto3.client('kinesis', region_name=REGION)

def load_data(file_path):
    """
    Carga el fichero JSON con los vuelos históricos
    """
    with open(file_path, 'r') as f:
        return json.load(f)

def run_producer():
    """
    Envía los registros de vuelos uno a uno al stream de Kinesis
    simulando un flujo en tiempo real.
    """
    data = load_data(INPUT_FILE)
    records_sent = 0

    logger.info(f"Iniciando transmisión al stream: {STREAM_NAME}...")

    # Iteramos sobre los vuelos del dataset
    for vuelo in data:

        # Payload enviado a Kinesis (evento individual)
        payload = {
            "flight_date": vuelo["FL_DATE"],
            "airline": vuelo["AIRLINE"],
            "airline_code": vuelo["AIRLINE_CODE"],
            "flight_number": vuelo["FL_NUMBER"],
            "origin": vuelo["ORIGIN"],
            "origin_city": vuelo["ORIGIN_CITY"],
            "destination": vuelo["DEST"],
            "destination_city": vuelo["DEST_CITY"],
            "scheduled_dep_time": vuelo["CRS_DEP_TIME"],
            "scheduled_arr_time": vuelo["CRS_ARR_TIME"],
            "distance": vuelo["DISTANCE"],
            "cancelled": vuelo["CANCELLED"],
            "cancellation_code": vuelo["CANCELLATION_CODE"],
            "diverted": vuelo["DIVERTED"]
        }

        # Envío del registro a Kinesis
        response = kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(payload),
            PartitionKey=vuelo["FL_DATE"]  # Clave de partición
        )

        records_sent += 1

        logger.info(
            f"Vuelo {vuelo['AIRLINE_CODE']}{vuelo['FL_NUMBER']} "
            f"{vuelo['ORIGIN']}->{vuelo['DEST']} "
            f"enviado al shard {response['ShardId']}"
        )

        # Pausa para simular streaming (no envío masivo)
        time.sleep(0.1)

    logger.info(f"Fin de la transmisión. Total registros enviados: {records_sent}")

if __name__ == '__main__':
    run_producer()
