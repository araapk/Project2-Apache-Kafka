# File: api/Dockerfile

# 1. Gunakan base image yang sudah memiliki Spark dan Python
# Pilih versi yang sesuai dengan kebutuhanmu. bitnami/spark adalah pilihan yang baik.
FROM bitnami/spark:3.5

# 2. Set direktori kerja di dalam container
WORKDIR /app

# 3. Salin file-file yang dibutuhkan dari lokal ke dalam container
# Salin daftar library terlebih dahulu untuk optimasi layer caching Docker
COPY requirements.txt .

# 4. Install library Python yang dibutuhkan
RUN pip install --no-cache-dir -r requirements.txt

# 5. Salin sisa kode aplikasi kita
COPY api.py .

# 6. Perintah yang akan dijalankan saat container dinyalakan
# Menjalankan server Flask kita
CMD ["python", "api.py"]