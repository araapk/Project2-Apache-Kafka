# api.py
from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel # Kita akan memuat PipelineModel yang disimpan
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DoubleType
import os
import logging

# Konfigurasi logging dasar untuk Flask
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

# Skema input data yang diharapkan oleh API (tanpa fare_amount)
# Harus sesuai dengan fitur yang dikirim oleh user
api_input_schema = StructType([
    StructField("pickup_datetime", StringType(), True), # Misal "2013-07-02 19:54:00 UTC"
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("passenger_count", IntegerType(), True)
])

def create_spark_session_for_api():
    try:
        spark = SparkSession.builder \
            .appName("TaxiFarePredictionAPI") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR") # Hanya error untuk API
        app.logger.info("SparkSession for API created successfully.")
        return spark
    except Exception as e:
        app.logger.error(f"Failed to create SparkSession for API: {e}")
        return None

spark_session_global = create_spark_session_for_api()
MODELS_DIR = 'models'
loaded_pipeline_models = {}

def load_all_pipeline_models():
    if not spark_session_global:
        app.logger.warning("SparkSession not available. Cannot load models.")
        return

    num_models_expected = 3 # Sesuai jumlah model yang dilatih
    for i in range(1, num_models_expected + 1):
        model_name = f"fare_model_v{i}"
        model_path = os.path.join(MODELS_DIR, model_name) # Ini adalah path direktori PipelineModel
        
        if os.path.exists(model_path) and os.path.isdir(model_path): # Pastikan itu direktori
            try:
                # Memuat PipelineModel lengkap (termasuk preprocessing)
                loaded_pipeline_models[model_name] = PipelineModel.load(model_path)
                app.logger.info(f"Successfully loaded PipelineModel: {model_name} from {model_path}")
            except Exception as e:
                app.logger.error(f"Error loading PipelineModel {model_name} from {model_path}: {e}")
        else:
            app.logger.warning(f"PipelineModel path not found or not a directory: {model_path} for model {model_name}")

@app.route('/predict/v<int:model_version>', methods=['POST'])
def predict_fare(model_version):
    if not spark_session_global:
        return jsonify({"error": "Prediction service (Spark) not available"}), 503

    model_name = f"fare_model_v{model_version}"
    if model_name not in loaded_pipeline_models:
        app.logger.warning(f"Prediction request for unloaded model: {model_name}")
        return jsonify({"error": f"Model {model_name} not loaded or does not exist"}), 404

    try:
        input_data_json = request.get_json()
        if not input_data_json:
            app.logger.error("Prediction request with no input data.")
            return jsonify({"error": "No input data provided"}), 400
        
        # Buat DataFrame dari input JSON tunggal (perlu list of dicts)
        input_df = spark_session_global.createDataFrame([input_data_json], schema=api_input_schema)
        
        # Dapatkan PipelineModel yang sudah dimuat
        pipeline_model_to_use = loaded_pipeline_models[model_name]
        
        # Lakukan prediksi menggunakan seluruh pipeline (preprocessing + model)
        prediction_df = pipeline_model_to_use.transform(input_df)
        
        # Ambil hasil prediksi (kolom "prediction" biasanya output dari model regresi)
        # Pastikan hasil ada sebelum mencoba mengaksesnya
        prediction_result = prediction_df.select("prediction").first()
        
        if prediction_result is None or prediction_result["prediction"] is None:
            app.logger.warning(f"Prediction for model {model_name} resulted in None. Input: {input_data_json}")
            # Ini bisa terjadi jika handleInvalid='skip' di assembler dan input menghasilkan nulls
            return jsonify({
                "model_version": model_name,
                "input_data": input_data_json,
                "warning": "Could not generate prediction, possibly due to invalid input features after preprocessing."
            }), 400 # Atau 200 dengan pesan warning

        predicted_fare = prediction_result["prediction"]
        
        app.logger.info(f"Prediction successful for {model_name}. Fare: {predicted_fare:.2f}")
        return jsonify({
            "model_version": model_name,
            "input_data": input_data_json,
            "predicted_fare": round(predicted_fare, 2)
        })

    except Exception as e:
        app.logger.error(f"Error during prediction with {model_name}: {e}", exc_info=True)
        return jsonify({"error": f"An internal server error occurred: {str(e)}"}), 500

# Endpoint untuk memeriksa status API dan model yang dimuat
@app.route('/status', methods=['GET'])
def api_status():
    models_status = {name: "Loaded" for name in loaded_pipeline_models.keys()}
    return jsonify({
        "status": "API is running",
        "spark_session": "Available" if spark_session_global else "Not Available",
        "loaded_models": models_status if models_status else "No models loaded"
    })


if __name__ == '__main__':
    if spark_session_global:
        load_all_pipeline_models()
    
    if not loaded_pipeline_models and spark_session_global:
        app.logger.warning("API starting, but no prediction models were loaded (Spark is running).")
    elif not spark_session_global:
        app.logger.critical("API starting, but SparkSession failed to initialize. Prediction endpoints will not be functional.")

    app.run(debug=False, host='0.0.0.0', port=5000) # Set debug=False untuk "produksi" mini