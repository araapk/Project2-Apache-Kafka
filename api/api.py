import os
from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth, hour, dayofweek, udf
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, IntegerType
from pyspark.ml import PipelineModel
from math import radians, sin, cos, sqrt, atan2

# --- Inisialisasi Aplikasi Flask ---
app = Flask(__name__)

# --- Konfigurasi dan Setup Awal Spark ---
SPARK_SESSION = None
# Gunakan dictionary untuk menyimpan semua model yang berhasil dimuat
# Key: 'v1', 'v2', etc. Value: objek PipelineModel
LOADED_MODELS = {} 
BASE_MODEL_PATH = "models" # Path dasar ke folder models di dalam container

# --- Fungsi Helper (Tetap sama) ---
def haversine(lon1, lat1, lon2, lat2):
    R = 6371
    try:
        if any(v is None for v in [lon1, lat1, lon2, lat2]): return None
        lon1, lat1, lon2, lat2 = float(lon1), float(lat1), float(lon2), float(lat2)
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    except (ValueError, TypeError):
        return None 
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c
    return distance if distance < 200 else None

haversine_udf = udf(haversine, DoubleType())

# --- Fungsi untuk inisialisasi ---
def init_spark_and_load_models():
    """Inisialisasi SparkSession dan muat SEMUA model yang tersedia."""
    global SPARK_SESSION, LOADED_MODELS

    if SPARK_SESSION is None:
        try:
            SPARK_SESSION = SparkSession.builder \
                .appName("TaxiFarePredictionAPI_MultiModel") \
                .config("spark.driver.memory", "2g") \
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
                .getOrCreate()
            SPARK_SESSION.sparkContext.setLogLevel("ERROR")
            SPARK_SESSION.udf.register("haversine_udf", haversine, DoubleType())
            print(">>> SparkSession initialized successfully.")
        except Exception as e:
            print(f"!!! Error initializing SparkSession: {e}")
            return

    # Cari semua folder model di dalam BASE_MODEL_PATH
    if os.path.exists(BASE_MODEL_PATH):
        for model_folder_name in os.listdir(BASE_MODEL_PATH):
            model_path = os.path.join(BASE_MODEL_PATH, model_folder_name)
            if os.path.isdir(model_path) and model_folder_name.startswith("fare_model_"):
                # Ekstrak versi model, misal 'v1' dari 'fare_model_v1'
                version_key = model_folder_name.split('_')[-1]
                try:
                    print(f">>> Loading model '{version_key}' from {model_path}...")
                    model = PipelineModel.load(model_path)
                    LOADED_MODELS[version_key] = model
                    print(f">>> Model '{version_key}' loaded successfully.")
                except Exception as e:
                    print(f"!!! Failed to load model '{version_key}': {e}")
    else:
        print(f"!!! Warning: Model directory not found at '{BASE_MODEL_PATH}'")

# === ENDPOINT BARU 1: Menampilkan Model yang Tersedia ===
@app.route('/models', methods=['GET'])
def get_available_models():
    """Mengembalikan daftar model yang berhasil dimuat dan siap digunakan."""
    if not LOADED_MODELS:
        return jsonify({"error": "No models are currently loaded."}), 404
    
    available_models = list(LOADED_MODELS.keys())
    return jsonify({"available_models": sorted(available_models)})

# === ENDPOINT BARU 2: Health Check ===
@app.route('/health', methods=['GET'])
def health_check():
    """Memeriksa status API dan Spark Session."""
    status = {
        "status": "ok",
        "spark_session_active": SPARK_SESSION is not None,
        "loaded_models_count": len(LOADED_MODELS)
    }
    return jsonify(status)

# === ENDPOINT UTAMA (di-upgrade): Prediksi dengan Versi Model Tertentu ===
@app.route('/predict/<model_version>', methods=['POST'])
def predict(model_version):
    """Endpoint untuk prediksi menggunakan versi model tertentu."""
    # 1. Validasi
    if SPARK_SESSION is None:
        return jsonify({"error": "Service is not ready. Spark not initialized."}), 503

    if model_version not in LOADED_MODELS:
        return jsonify({
            "error": f"Model version '{model_version}' not found or not loaded.",
            "available_models": sorted(list(LOADED_MODELS.keys()))
        }), 404

    json_data = request.get_json()
    if not json_data:
        return jsonify({"error": "Invalid input. JSON data is required."}), 400

    # Pilih model yang sesuai dari dictionary
    prediction_model = LOADED_MODELS[model_version]

    # 2. Skema & Preprocessing (sama seperti sebelumnya)
    input_schema = StructType([
        StructField("pickup_datetime", StringType(), True),
        StructField("pickup_longitude", DoubleType(), True),
        StructField("pickup_latitude", DoubleType(), True),
        StructField("dropoff_longitude", DoubleType(), True),
        StructField("dropoff_latitude", DoubleType(), True),
        StructField("passenger_count", IntegerType(), True)
    ])
    
    try:
        input_df = SPARK_SESSION.createDataFrame([json_data], schema=input_schema)
        
        processed_df = input_df \
            .withColumn("pickup_datetime_ts", to_timestamp(col("pickup_datetime"))) \
            .withColumn("hour", hour(col("pickup_datetime_ts")).cast(IntegerType())) \
            .withColumn("day_of_week", dayofweek(col("pickup_datetime_ts")).cast(IntegerType())) \
            .withColumn("month", month(col("pickup_datetime_ts")).cast(IntegerType())) \
            .withColumn("distance_km", haversine_udf(
                col("pickup_longitude"), col("pickup_latitude"),
                col("dropoff_longitude"), col("dropoff_latitude")
            ))

        # 3. Prediksi dengan model yang dipilih
        prediction_result_df = prediction_model.transform(processed_df)
        prediction = prediction_result_df.select("prediction").first()[0]
        
        return jsonify({
            "model_version_used": model_version,
            "predicted_fare": round(prediction, 2)
        })

    except Exception as e:
        print(f"Error during prediction with model {model_version}: {e}")
        return jsonify({"error": "Failed to make prediction. Check server logs."}), 500

# --- Jalankan Aplikasi ---
if __name__ == '__main__':
    # Ganti nama fungsi startup
    init_spark_and_load_models()
    app.run(host='0.0.0.0', port=5001)