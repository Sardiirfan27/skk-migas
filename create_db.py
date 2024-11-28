import pandas as pd
import numpy as np
import random
from sqlalchemy import create_engine
from datetime import datetime, timedelta

start_date = datetime(2020, 1, 1)
end_date = datetime(2022, 9, 30)
dates = pd.date_range(start=start_date, end=end_date, freq='D')

# Fungsi untuk menghasilkan data dummy dengan fluktuasi acak
def generate_data(date):
    # Produksi minyak (Barel/hari), dengan fluktuasi acak
    oil_production = random.randint(800000, 1000000) + random.uniform(-50000, 50000)
    
    # Produksi gas (MMSCFD), dengan fluktuasi acak
    gas_production = random.randint(200, 300) + random.uniform(-10, 10)
    
    # Harga minyak (USD/Barrel), dengan fluktuasi acak
    oil_price = 40 + random.uniform(10, 40)  # Harga minyak antara 40 - 80 USD
    
    # Harga gas (USD/MMBtu), dengan fluktuasi acak
    gas_price = 2 + random.uniform(0.5, 3.0)  # Harga gas antara 2 - 5 USD
    
    return {
        'Produksi Minyak (Barel/Hari)': oil_production,
        'Produksi Gas (MMSCFD)': gas_production,
        'Harga Minyak (USD/Barrel)': oil_price,
        'Harga Gas (USD/MMBtu)': gas_price
    }

# Membuat data dictionary untuk setiap tanggal
data_dict = {date: generate_data(date) for date in dates}

# Membuat DataFrame dari dictionary
df = pd.DataFrame.from_dict(data_dict, orient='index')

# Menambahkan missing values secara acak
missing_rate = 0.05  # 5% dari data akan hilang
n_missing = int(df.size * missing_rate)  # Jumlah missing values yang akan ditambahkan
for _ in range(n_missing):
    col = random.choice(list(df.columns))  # Pilih kolom acak, konversi ke list
    idx = random.choice(df.index.tolist())  # Pilih baris acak, convert ke list
    df.loc[idx, col] = np.nan  # Set nilai kolom dan baris yang dipilih menjadi NaN

# Menambahkan beberapa duplikasi secara acak
n_duplicates = 10  # 10 baris yang akan diduplikasi
for _ in range(n_duplicates):
    duplicate_row = random.choice(df.index.tolist())  # Pilih baris acak, convert ke list
    df = pd.concat([df, df.loc[[duplicate_row]]])  # Menambahkan baris duplikat

# Menambahkan beberapa error data (misalnya nilai negatif yang tidak valid)
error_indices = random.sample(list(df.index), 3)  # Pilih 3 baris untuk error
for idx in error_indices:
    df.loc[idx, 'Produksi Gas (MMSCFD)'] = -random.randint(1, 10)  # Negatif produksi gas
    df.loc[idx, 'Harga Minyak (USD/Barrel)'] = -random.uniform(10, 20)  # Harga minyak negatif

# Menampilkan data yang sudah dimodifikasi
print(f"Total rows: {len(df)}")

# Simpan ke file CSV
df.to_csv('time_series_data_with_issues.csv')

# Membuat koneksi ke PostgreSQL menggunakan SQLAlchemy
# Ganti parameter sesuai dengan konfigurasi database Anda
engine = create_engine('postgresql+psycopg2://airflow:airflow@localhost:5434/stock_db')

# Memasukkan data ke dalam database menggunakan to_sql()
df.to_sql('dummy_data_with_issues', con=engine, if_exists='replace', index_label='tanggal')
