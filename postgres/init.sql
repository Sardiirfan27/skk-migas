-- Buat database bernama stock_db
CREATE DATABASE stock_db;

-- Gunakan database stock_db
\connect stock_db;

-- Buat tabel untuk menyimpan data stock NVIDIA
CREATE TABLE nvidia_stock_data (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    open_price NUMERIC(10, 2),
    high_price NUMERIC(10, 2),
    low_price NUMERIC(10, 2),
    close_price NUMERIC(10, 2),
    volume BIGINT
);

-- Masukkan data awal (opsional)
INSERT INTO nvidia_stock_data (date, open_price, high_price, low_price, close_price, volume) VALUES
('2024-11-01', 480.00, 485.00, 475.00, 482.50, 35000000),
('2024-11-02', 482.50, 490.00, 480.00, 487.00, 36000000),
('2024-11-03', 487.00, 495.00, 485.00, 493.00, 37000000);

-- Buat user dan grant akses (opsional jika ingin menambahkan user akses tambahan)
CREATE USER airflow_user WITH PASSWORD 'airflow_password';

-- Grant semua akses ke user airflow_user pada database stock_db
GRANT ALL PRIVILEGES ON DATABASE stock_db TO airflow_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow_user;
