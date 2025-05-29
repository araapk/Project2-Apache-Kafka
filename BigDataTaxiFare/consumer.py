# consumer.py
import json
import csv
import os
from kafka import KafkaConsumer

KAFKA_TOPIC = 'taxi_fare_topic'
KAFKA_BROKER = 'localhost:9092'
BATCH_SIZE = 10000  # Jumlah data per batch file
OUTPUT_DIR = 'data_batches'
CONSUMER_GROUP_ID = 'taxi-fare-consumer-group-main' # Ganti jika menjalankan beberapa instance untuk pengujian

def create_consumer():
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest',
            group_id=CONSUMER_GROUP_ID,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            consumer_timeout_ms=60000 # Timeout setelah 60 detik jika tidak ada pesan baru
        )
        print(f"Kafka Consumer connected to {KAFKA_BROKER}, group_id: {CONSUMER_GROUP_ID}")
        return consumer
    except Exception as e:
        print(f"ERROR: Could not connect Kafka Consumer - {e}")
        return None

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created directory: {OUTPUT_DIR}")

    consumer = create_consumer()
    if not consumer:
        return

    print(f"Starting consumer for topic: {KAFKA_TOPIC}, batch size: {BATCH_SIZE}")
    print(f"Will save batches to: {OUTPUT_DIR}")

    batch_data = []
    batch_count = 0
    message_count_in_batch = 0
    total_messages_consumed = 0

    try:
        for message in consumer:
            data_point = message.value
            # Validasi sederhana, pastikan data_point adalah dictionary
            if not isinstance(data_point, dict):
                print(f"Warning: Received non-dict message: {data_point}. Skipping.")
                continue

            batch_data.append(data_point)
            message_count_in_batch += 1
            total_messages_consumed += 1

            if total_messages_consumed % 2000 == 0: # Log lebih jarang
                print(f"Consumed {total_messages_consumed} messages so far...")

            if message_count_in_batch >= BATCH_SIZE:
                batch_count += 1
                file_path = os.path.join(OUTPUT_DIR, f'batch_{batch_count}.csv')
                
                if batch_data:
                    # Dapatkan header dari data pertama, pastikan konsisten
                    # Jika ada kemungkinan kunci berbeda, perlu penanganan lebih lanjut
                    try:
                        headers = list(batch_data[0].keys()) 
                        with open(file_path, mode='w', newline='', encoding='utf-8') as file:
                            writer = csv.DictWriter(file, fieldnames=headers, extrasaction='ignore') # ignore kunci ekstra
                            writer.writeheader()
                            writer.writerows(batch_data)
                        print(f"Saved batch {batch_count} to {file_path} ({len(batch_data)} records)")
                    except Exception as e_write:
                        print(f"Error writing batch {batch_count} to {file_path}: {e_write}")
                
                batch_data = []
                message_count_in_batch = 0
        
        print(f"Consumer timed out or topic depleted after consuming {total_messages_consumed} messages.")

    except KeyboardInterrupt:
        print("\nConsumer interrupted by user. Saving any remaining data...")
    except Exception as e:
        print(f"An unexpected error occurred in consumer: {e}")
    finally:
        if batch_data: # Simpan sisa data jika ada
            batch_count = batch_count + 1 if batch_count > 0 else 1 # Handle jika belum ada batch tersimpan
            final_file_path = os.path.join(OUTPUT_DIR, f'batch_{batch_count}_final.csv')
            try:
                if batch_data and isinstance(batch_data[0], dict):
                    headers = list(batch_data[0].keys())
                    with open(final_file_path, mode='w', newline='', encoding='utf-8') as file:
                        writer = csv.DictWriter(file, fieldnames=headers, extrasaction='ignore')
                        writer.writeheader()
                        writer.writerows(batch_data)
                    print(f"Saved final batch {batch_count} to {final_file_path} ({len(batch_data)} records)")
            except Exception as e_final_write:
                print(f"Error writing final batch to {final_file_path}: {e_final_write}")
        
        if consumer:
            print("Closing consumer...")
            consumer.close()
            print("Consumer closed.")
        
        print(f"Total messages consumed by this instance: {total_messages_consumed}")

if __name__ == "__main__":
    main()