# producer.py
import time
import json
import random
import csv
import os
from kafka import KafkaProducer

KAFKA_TOPIC = 'taxi_fare_topic'
KAFKA_BROKER = 'localhost:9092'
# Path relatif ke dataset dari lokasi script ini
DATASET_PATH = os.path.join('dataset_source', 'train.csv')

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5, # Coba lagi jika pengiriman gagal
            linger_ms=10 # Tunggu hingga 10ms untuk mengumpulkan batch sebelum mengirim
        )
        print(f"Kafka Producer connected to {KAFKA_BROKER}")
        return producer
    except Exception as e:
        print(f"ERROR: Could not connect Kafka Producer to {KAFKA_BROKER} - {e}")
        return None

def main():
    producer = create_producer()
    if not producer:
        return

    print(f"Starting producer for topic: {KAFKA_TOPIC}")
    print(f"Reading data from: {DATASET_PATH}")

    if not os.path.exists(DATASET_PATH):
        print(f"ERROR: Dataset file not found at {DATASET_PATH}. Please check the path.")
        return

    sent_count = 0
    skipped_count = 0
    try:
        with open(DATASET_PATH, mode='r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            print("Successfully opened dataset file. Starting to send messages...")
            
            for i, row_dict in enumerate(csv_reader):
                try:
                    # Data type conversions and sanity checks
                    # Pastikan kolom ada sebelum mencoba konversi
                    processed_row = {}
                    processed_row['key'] = row_dict.get('key', f'row_{i}') # Ambil key atau buat default

                    if 'fare_amount' in row_dict and row_dict['fare_amount']:
                        processed_row['fare_amount'] = float(row_dict['fare_amount'])
                    if 'pickup_datetime' in row_dict and row_dict['pickup_datetime']:
                         processed_row['pickup_datetime'] = str(row_dict['pickup_datetime']) # Kirim sebagai string
                    if 'pickup_longitude' in row_dict and row_dict['pickup_longitude']:
                        processed_row['pickup_longitude'] = float(row_dict['pickup_longitude'])
                    if 'pickup_latitude' in row_dict and row_dict['pickup_latitude']:
                        processed_row['pickup_latitude'] = float(row_dict['pickup_latitude'])
                    if 'dropoff_longitude' in row_dict and row_dict['dropoff_longitude']:
                        processed_row['dropoff_longitude'] = float(row_dict['dropoff_longitude'])
                    if 'dropoff_latitude' in row_dict and row_dict['dropoff_latitude']:
                        processed_row['dropoff_latitude'] = float(row_dict['dropoff_latitude'])
                    if 'passenger_count' in row_dict and row_dict['passenger_count']:
                        processed_row['passenger_count'] = int(row_dict['passenger_count'])
                    
                    # Hanya kirim jika data penting ada (misal, fare_amount dan koordinat)
                    # Ini opsional, tergantung kebutuhan
                    required_fields = ['fare_amount', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude', 'passenger_count', 'pickup_datetime']
                    if not all(field in processed_row for field in required_fields):
                        # print(f"Skipping row {i+1} due to missing required fields. Data: {row_dict}")
                        skipped_count +=1
                        if skipped_count % 500 == 0:
                             print(f"Skipped {skipped_count} rows so far due to missing fields.")
                        continue

                    producer.send(KAFKA_TOPIC, value=processed_row)
                    sent_count += 1
                    
                    if sent_count % 1000 == 0:
                        print(f"Sent {sent_count} messages...")
                    
                    time.sleep(random.uniform(0.001, 0.003)) # Sedikit lebih cepat

                    # UNCOMMENT untuk testing dengan data terbatas
                    # if sent_count >= 35000: # Misal 35 ribu data untuk 3 model @ 10k + sisa
                    #     print(f"Reached data limit of {sent_count} for testing. Stopping producer.")
                    #     break

                except (ValueError, TypeError) as e:
                    skipped_count += 1
                    if skipped_count % 500 == 0: # Log error sesekali agar tidak terlalu verbose
                        print(f"Warning: Skipping row {i+1} due to data conversion error: {e}. Original data: {row_dict}")
                    continue
        
        print(f"Finished reading dataset. Total messages sent: {sent_count}, Total skipped: {skipped_count}")

    except FileNotFoundError: # Sudah di-handle di atas, tapi jaga-jaga
        print(f"ERROR: Dataset file not found at {DATASET_PATH}.")
    except Exception as e:
        print(f"An unexpected error occurred in producer: {e}")
    finally:
        if producer:
            print("Flushing producer...")
            producer.flush(timeout=10) # Beri waktu untuk flush
            print("Closing producer...")
            producer.close(timeout=10)
            print("Producer closed.")

if __name__ == "__main__":
    main()
