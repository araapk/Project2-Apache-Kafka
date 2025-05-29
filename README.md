# Project2-Apache-Kafka
Anggota Kelompok:
- Rafika NRP
- Tika NRP
- Cal NRP 60

Struktur Folder Proyek di VS Code:

BigDataTaxiFare/
├── dataset_source/
│   └── train.csv         <-- TEMPATKAN FILE DATASET DI SINI
├── data_batches/         (Akan dibuat oleh consumer.py)
├── models/               (Akan dibuat oleh spark_trainer.py)
├── venv/                 (Virtual environment Anda)
├── producer.py
├── consumer.py
├── spark_trainer.py
├── api.py
└── requirements.txt      

Beberapa Data Batches ada disini: https://drive.google.com/drive/folders/19JQdQyY2JsZSizAFaaPBZn_iNfAluH5L?usp=sharing
Dataset yang digunakan (taruh di train.csv): https://www.kaggle.com/c/new-york-city-taxi-fare-prediction/data

Persiapan environment:
1. Buat file requirements.txt, isi dengan
```
kafka-python
pyspark
pandas
scikit-learn
flask
```
2. Instalasi Dependensi (di Terminal)
```
python -m venv venv
# Windows:
.\venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate
```
```
pip install -r requirements.txt
```
Buat file python (ada di file zip)

Cara Menjalankan di VS Code (Ringkasan):

1. Persiapan:
- Pastikan Java, Kafka, dan Spark sudah terinstal dan JAVA_HOME, SPARK_HOME (dan path bin-nya) sudah di-setting.
- Tempatkan train.csv di folder dataset_source.
- Buat virtual environment dan instal requirements.txt.
2. Mulai Zookeeper & Kafka Server: (Di terminal terpisah, di luar VS Code atau di terminal VS Code tambahan)
```
zookeeper-server-start.bat ... atau zookeeper-server-start.sh ...
```
```
kafka-server-start.bat ... atau kafka-server-start.sh ...
```
Kalau sudah install di docker, jalankan dengan
```
docker-compose up -d
```
```
docker ps
```
3. Buat Kafka Topic (jika belum): (Di terminal terpisah)
```
docker exec -it kafka kafka-topics --create --topic taxi_fare_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
4. Jalankan Consumer: (Di terminal VS Code, venv aktif)
```
python consumer.py
```
5. Jalankan Producer: (Di terminal VS Code BARU, venv aktif)
```
python producer.py
```
Tunggu sampai cukup data terkirim (misalnya, 30-35rb pesan). Lalu hentikan Producer (Ctrl+C), kemudian hentikan Consumer (Ctrl+C). Periksa folder data_batches.

6. Jalankan Spark Trainer: (Di terminal VS Code, venv aktif)
```
python spark_trainer.py (atau spark-submit spark_trainer.py jika perlu)
```
Ini akan memakan waktu. Periksa folder models setelah selesai.

7. Jalankan API Server: (Di terminal VS Code, venv aktif)
```
python api.py
```
8. Test API:
- Buka browser ke http://localhost:5000/status untuk melihat status.
- Gunakan curl atau Postman untuk mengirim request POST ke http://localhost:5000/predict/v1 (atau v2, v3) dengan JSON body seperti contoh sebelumnya.

Dokumentasi

Pembuatan topic
![image](https://github.com/user-attachments/assets/643e59e0-7b38-442e-8ad2-ad707b6cdb5b)

producer.py
![image](https://github.com/user-attachments/assets/168c325a-19a7-46cf-94b1-d992980359af)

consumer.py
![image](https://github.com/user-attachments/assets/886717e7-7f14-4822-8fa5-9b2056318281)

isi salah satu batch
![image](https://github.com/user-attachments/assets/daab7d92-ce41-4fd8-b346-0ebc8dbb5b33)


