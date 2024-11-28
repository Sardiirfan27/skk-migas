import pandas as pd
import numpy as np
import random
from sqlalchemy import create_engine
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Fungsi Extract: Mengambil data dari database
def extract_data():
    # Membuat koneksi ke PostgreSQL menggunakan SQLAlchemy
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/stock_db')

    # Mengambil data dari database
    query = "SELECT * FROM dummy_data_with_issues"  # Ganti dengan nama tabel yang sesuai
    df = pd.read_sql(query, con=engine)

    return df

# Fungsi Transform: Menangani missing values, duplikasi, dan data error
def transform_data(df):
    # 1. Menangani Missing Values
    df['Produksi Minyak (Barel/Hari)'].fillna(df['Produksi Minyak (Barel/Hari)'].mean(), inplace=True)
    df['Produksi Gas (MMSCFD)'].fillna(df['Produksi Gas (MMSCFD)'].mean(), inplace=True)
    df['Harga Minyak (USD/Barrel)'].fillna(df['Harga Minyak (USD/Barrel)'].median(), inplace=True)
    df['Harga Gas (USD/MMBtu)'].fillna(df['Harga Gas (USD/MMBtu)'].median(), inplace=True)

    # 2. Menangani Duplikasi
    df = df.drop_duplicates()

    # 3. Menangani Data Error
    df['Produksi Gas (MMSCFD)'] = df['Produksi Gas (MMSCFD)'].apply(lambda x: max(0, x))  # Set nilai negatif menjadi 0
    df['Harga Minyak (USD/Barrel)'] = df['Harga Minyak (USD/Barrel)'].apply(lambda x: max(0, x))  # Set nilai negatif menjadi 0

    # 4. Cek Missing Values Setelah Imputasi
    print("Missing values after transformation:", df.isnull().sum())

    return df

# Fungsi Load: Menyimpan data yang sudah dibersihkan ke CSV dan database
def load_data(df):
    # Simpan ke file CSV
    output_file = '/opt/airflow/dags/cleaned_data.csv'
    df.to_csv(output_file)

    print(f"Data successfully loaded into CSV")

# Mendefinisikan DAG Airflow
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'skk_migas_etl',
    default_args=default_args,
    description='ETL pipeline',
    schedule_interval='0 10 * * 1',  # Setiap minggu pada pukul 10 pagi
)

# Mendefinisikan tugas-tugas ETL
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_args=[extract_task.output],  # Menggunakan hasil dari extract_data
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_args=[transform_task.output],  # Menggunakan hasil dari transform_data
    dag=dag,
)

# Mengatur urutan tugas-tugas
extract_task >> transform_task >> load_task
