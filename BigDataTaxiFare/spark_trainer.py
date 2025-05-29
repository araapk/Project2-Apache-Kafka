# spark_trainer.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth, hour, dayofweek, udf, isnan, when
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType, TimestampType, DoubleType
from pyspark.ml.feature import VectorAssembler, Imputer, StandardScaler
from pyspark.ml.regression import LinearRegression, GBTRegressor # Coba GBT untuk hasil lebih baik
from pyspark.ml import Pipeline
import os
from math import radians, sin, cos, sqrt, atan2

# Fungsi Haversine untuk jarak (memastikan input adalah float)
def haversine(lon1, lat1, lon2, lat2):
    R = 6371  # Radius bumi dalam km
    try:
        # Handle None or non-numeric inputs robustly
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
    # Batasi jarak yang tidak masuk akal (misal > 200km untuk perjalanan taksi dalam kota)
    return distance if distance < 200 else None

haversine_udf = udf(haversine, DoubleType()) # Gunakan DoubleType untuk presisi lebih baik

# Definisikan skema CSV untuk pembacaan yang lebih terkontrol
# Sesuaikan dengan kolom yang DIKIRIM oleh producer dan DISIMPAN oleh consumer
# Pastikan nama kolom sama persis
csv_schema = StructType([
    StructField("key", StringType(), True),
    StructField("fare_amount", DoubleType(), True), # Target
    StructField("pickup_datetime", StringType(), True), # Akan dikonversi
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("passenger_count", IntegerType(), True)
])


def create_spark_session():
    try:
        spark = SparkSession.builder \
            .appName("TaxiFareModelTraining") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        print("SparkSession created successfully.")
        return spark
    except Exception as e:
        print(f"Error creating SparkSession: {e}")
        return None

def get_feature_engineering_pipeline(input_cols, output_col_features="features_unscaled"):
    # Tahap-tahap preprocessing yang akan dimasukkan ke pipeline
    # Imputasi untuk semua kolom fitur numerik
    imputers = [
        Imputer(inputCol=c, outputCol=f"{c}_imputed", strategy="mean")
        for c in input_cols
    ]
    imputed_cols = [f"{c}_imputed" for c in input_cols]
    
    # Vector Assembler
    assembler = VectorAssembler(inputCols=imputed_cols, outputCol=output_col_features, handleInvalid="skip")
    
    # Scaling (opsional, tapi sering membantu model linear)
    # scaler = StandardScaler(inputCol=output_col_features, outputCol="scaled_features", withStd=True, withMean=True)

    stages = imputers + [assembler] # Tambahkan scaler jika digunakan
    pipeline = Pipeline(stages=stages)
    return pipeline


def train_model(spark, data_df, model_name_suffix, base_models_dir):
    model_output_dir = os.path.join(base_models_dir, f"fare_model_v{model_name_suffix}")
    print(f"\n--- Preparing data for model: fare_model_v{model_name_suffix} ---")

    # 1. Preprocessing dasar (konversi tipe, fitur datetime, jarak)
    df = data_df
    df = df.withColumn("pickup_datetime_ts", to_timestamp(col("pickup_datetime"))) # Format default
    df = df.withColumn("hour", hour(col("pickup_datetime_ts")).cast(IntegerType()))
    df = df.withColumn("day_of_week", dayofweek(col("pickup_datetime_ts")).cast(IntegerType()))
    df = df.withColumn("month", month(col("pickup_datetime_ts")).cast(IntegerType()))
    
    df = df.withColumn("distance_km", haversine_udf(
        col("pickup_longitude"), col("pickup_latitude"),
        col("dropoff_longitude"), col("dropoff_latitude")
    ))

    # Filter data yang tidak valid (misal, fare <= 0, koordinat di luar batas wajar, jarak aneh)
    df = df.filter((col("fare_amount") > 0) & (col("fare_amount") < 500)) # Batas atas tarif
    df = df.filter((col("pickup_longitude") >= -75) & (col("pickup_longitude") <= -73))
    df = df.filter((col("pickup_latitude") >= 40) & (col("pickup_latitude") <= 42))
    df = df.filter((col("dropoff_longitude") >= -75) & (col("dropoff_longitude") <= -73))
    df = df.filter((col("dropoff_latitude") >= 40) & (col("dropoff_latitude") <= 42))
    df = df.filter(col("passenger_count").isNotNull() & (col("passenger_count") > 0) & (col("passenger_count") < 7))
    df = df.filter(col("distance_km").isNotNull() & (col("distance_km") > 0) & (col("distance_km") < 100)) # Jarak masuk akal
    df = df.na.drop(subset=["hour", "day_of_week", "month"]) # Pastikan fitur waktu tidak null

    feature_input_cols = [
        "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude",
        "passenger_count", "hour", "day_of_week", "month", "distance_km"
    ]
    
    # Hapus baris dengan null pada fitur input sebelum ke pipeline
    df_cleaned = df.select(["fare_amount"] + feature_input_cols).na.drop(subset=feature_input_cols)

    if df_cleaned.count() == 0:
        print(f"No valid data left after cleaning for model {model_name_suffix}. Skipping training.")
        return

    print(f"Data count for model {model_name_suffix} after cleaning: {df_cleaned.count()}")

    # 2. Dapatkan pipeline feature engineering
    feature_pipeline = get_feature_engineering_pipeline(feature_input_cols, output_col_features="features")
    
    # 3. Definisikan model regresi
    # lr = LinearRegression(featuresCol="features", labelCol="fare_amount", maxIter=10, regParam=0.1)
    gbt = GBTRegressor(featuresCol="features", labelCol="fare_amount", maxIter=20) # GBT biasanya lebih baik

    # 4. Gabungkan feature engineering dan model regresi ke dalam satu pipeline
    # Ini adalah model yang akan kita simpan, sehingga preprocessing konsisten di API
    full_pipeline = Pipeline(stages=[feature_pipeline, gbt]) # ganti gbt dengan lr jika mau

    print(f"Training full pipeline for model fare_model_v{model_name_suffix}...")
    try:
        pipeline_model = full_pipeline.fit(df_cleaned) # Latih pada data yang sudah dibersihkan
        
        print(f"Saving PipelineModel fare_model_v{model_name_suffix} to {model_output_dir}")
        pipeline_model.write().overwrite().save(model_output_dir)
        print(f"Model fare_model_v{model_name_suffix} saved successfully.")

    except Exception as e:
        print(f"Error during model training or saving for fare_model_v{model_name_suffix}: {e}")
        # df_cleaned.printSchema()
        # df_cleaned.show(5, truncate=False)

def main():
    spark = create_spark_session()
    if not spark:
        return

    DATA_BATCHES_DIR = 'data_batches'
    MODELS_DIR = 'models'
    if not os.path.exists(MODELS_DIR):
        os.makedirs(MODELS_DIR)

    num_models_to_train = 3 
    all_batch_files = sorted([
        os.path.join(DATA_BATCHES_DIR, f) 
        for f in os.listdir(DATA_BATCHES_DIR) 
        if f.startswith('batch_') and f.endswith('.csv')
    ])

    if not all_batch_files:
        print(f"No batch files found in {DATA_BATCHES_DIR}. Exiting.")
        spark.stop()
        return

    max_models = min(num_models_to_train, len(all_batch_files))
    print(f"Will attempt to train {max_models} cumulative models based on available data.")

    cumulative_df = None
    for i in range(max_models):
        model_version_suffix = i + 1
        current_batch_file = all_batch_files[i]
        
        print(f"\nProcessing data for Model v{model_version_suffix} using up to batch: {current_batch_file}")
        try:
            new_batch_df = spark.read.csv(current_batch_file, header=True, schema=csv_schema)
        except Exception as e_read:
            print(f"Error reading CSV {current_batch_file}: {e_read}. Skipping this batch.")
            continue

        if cumulative_df is None:
            cumulative_df = new_batch_df
        else:
            cumulative_df = cumulative_df.unionByName(new_batch_df, allowMissingColumns=True)
        
        print(f"Cumulative data size for Model v{model_version_suffix}: {cumulative_df.count()}")
        train_model(spark, cumulative_df.alias(f"data_v{model_version_suffix}"), model_version_suffix, MODELS_DIR)

    print("\nAll model training attempts completed.")
    spark.stop()
    print("SparkSession stopped.")

if __name__ == "__main__":
    main()