# Studi Kasus Apache Kafka

## Anggota Kelompok
- Awang Fraditya (5027221055)
- Hafiz Akmaldi Santosa (5027221061)

## Topik: Pengumpulan Data Sensor IoT dengan Apache Kafka

### Latar Belakang Masalah:

Sebuah pabrik memiliki beberapa mesin yang dilengkapi sensor suhu. Data suhu dari setiap mesin perlu dipantau secara real-time untuk menghindari overheating. Setiap sensor akan mengirimkan data suhu setiap detik, dan pabrik membutuhkan sistem yang dapat mengumpulkan, menyimpan, dan menganalisis data suhu ini.

### Studi Kasus Sederhana:

- Pabrik membutuhkan aliran data sensor yang dapat diteruskan ke layanan analitik atau dashboard secara langsung.
- Apache Kafka akan digunakan untuk menerima dan mengalirkan data suhu, sementara PySpark akan digunakan untuk mengolah dan memfilter data tersebut.

### Tugas:
1. Buat Topik Kafka untuk Data Suhu:
    - Buat topik di Apache Kafka bernama "sensor-suhu" yang akan menerima data suhu dari sensor-sensor mesin.

2. Simulasikan Data Suhu dengan Producer:
    - Buat producer sederhana yang mensimulasikan data suhu dari beberapa sensor mesin (misalnya, 3 sensor berbeda).
    - Setiap data suhu berisi ID sensor dan suhu saat ini (misalnya, sensor_id: S1, suhu: 70°C), dan dikirim setiap detik ke topik "sensor-suhu".

3. Konsumsi dan Olah Data dengan PySpark:
    - Buat consumer di PySpark yang membaca data dari topik "sensor-suhu".
Filter data suhu yang berada di atas 80°C, sebagai indikator suhu yang perlu diperhatikan.

4. Output dan Analisis:
    - Cetak data yang suhu-nya melebihi 80°C sebagai tanda peringatan sederhana di console.