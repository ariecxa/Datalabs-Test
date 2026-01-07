# Datalabs-Test

Repository ini berisi **soal dan jawaban technical test** untuk **PT. Datalabs**, yang berfokus pada **perancangan data warehouse, ETL pipeline, dan simulasi streaming data menggunakan Python**.

Struktur repository dirancang untuk merepresentasikan **alur data end-to-end**, mulai dari **source system → data warehouse → ETL → streaming data**.

---

## 1. Struktur Folder

Repository ini terdiri dari **3 folder utama**:

Datalabs-Test/

│

├── ERD & FLOWCHART/

├── SQL & DDL/

└── SCRIPT/

---

## 2. ERD & FLOWCHART

Folder ini berisi **diagram arsitektur data dan alur proses** yang digunakan dalam pengerjaan test.

### ERD (Entity Relationship Diagram)

Terdapat **2 ERD**:

### ERD Source Table
ERD ini menggambarkan **hubungan antar tabel source** sesuai dengan soal yang diberikan.

**Tabel source (5 tabel):**
- `customers`
- `products`
- `marketing_campaigns`
- `transactions`
- `transactions_items`

**Relasi antar tabel:**
- `customers` → `transactions`  
  (one customer memiliki banyak transaksi)
- `transactions` → `transactions_items`  
  (one transaction memiliki banyak item transaksi)
- `products` → `transactions_items`  
  (one product bisa muncul di banyak item transaksi)

**Catatan:**  
Tabel `marketing_campaigns` tidak memiliki relasi langsung dengan tabel source lainnya.

---

### ERD Dimension & Fact Table
ERD ini menggambarkan **data warehouse schema** yang dibangun dari source table.

- Schema yang digunakan: **Star Schema**
- Alasan pemilihan:
  - Struktur data source tidak terlalu kompleks
  - Query analitik lebih cepat
  - Lebih mudah dipahami dibandingkan snowflake schema

Dimensi tambahan seperti `dim_city` atau `dim_category` dapat ditambahkan, namun tidak digunakan untuk menjaga kesederhanaan model.

**Tabel Data Warehouse:**

**Dimension Tables (4):**
- `dim_customer`  
  `(customer_id, name, email, city, signup_date)`
- `dim_product`  
  `(product_id, product_name, category, price)`
- `dim_date`  
  `(date_id, full_date, year, month, day, month_name, day_name)`
- `dim_campaign`  
  `(campaign_id, campaign_name, start_date, end_date, channel)`

**Fact Table (1):**
- `fact_sales`  
  `(transaction_item_id, customer_id, product_id, campaign_id, price, quantity, total_amount, transaction_date)`

**Fact Sales** berisi detail penjualan per item dan dapat digunakan sebagai source untuk fact summary (daily / monthly sales).

---

### FLOWCHART

Terdapat **2 Flowchart**:

#### Flowchart ETL Process
Menjelaskan alur ETL untuk membangun dimension dan fact table.

**Alur proses:**
1. Connect ke Database
2. Extract data dari 5 source table
3. Transform data:
   - Data mapping
   - Data cleaning (standarisasi huruf besar, perbaikan data tidak konsisten)
4. Load data ke dimension dan fact table

---

#### Flowchart Streaming with Python Loop
Menjelaskan simulasi streaming data menggunakan Python.

**Alur proses:**
- **Process 1 (API)**
  - API untuk meng-generate timestamp saat ini
  - Digunakan sebagai dummy streaming data

- **Process 2 (Streaming)**
  - Python melakukan request ke API secara looping
  - Data hasil request:
    - Disimpan ke log
    - Disimpan ke database
  - Interval request: delay 1–5 detik
  - Setelah 15 kali looping:
    - Sistem menampilkan report transaksi per menit

---

## 3. SQL & DDL

Folder ini berisi **script SQL** untuk pembuatan database, tabel, dan dummy data.

**File:**
- `create_database.sql`
- `customers.sql`
- `products.sql`
- `transactions.sql`
- `transactions_items.sql`
- `marketing_campaigns.sql`
- `dimension.sql`
- `fact.sql`

---

## SCRIPT

Folder ini berisi **script Python** yang digunakan dalam pengerjaan test.

**File utama:**
- `ETL_AIRFLOW`  
  Pipeline ETL menggunakan Python dan dijalankan dengan Apache Airflow
- `API_DATE`  
  Script API untuk meng-generate timestamp saat ini
- `PYTHON_STREAM`  
  Script untuk streaming data, logging, penyimpanan database, dan report per menit

---

## Setup & Environment

Test ini dikerjakan menggunakan stack berikut:

- **Database**
  MySQL : 10.4.32-MariaDB
- **Python**
  3.12
- **Airflow**  
  apache/airflow:latest-python3.12

## Catatan
Repository ini dibuat untuk menunjukkan:
- Implementasi Star Schema
- ETL menggunakan Airflow
- Simulasi streaming data menggunakan Python
