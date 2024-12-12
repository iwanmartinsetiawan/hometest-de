import os
import json
import pandas as pd
from datetime import datetime, timedelta
import pytz  # Library untuk timezone handling
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Direktori untuk menyimpan file Parquet
PARQUET_DIR = "/tmp/parquet_files"

# Fungsi untuk mengecek koneksi MongoDB
def check_mongo_connection():
    try:
        mongo_hook = MongoHook(conn_id="mongo_default")
        client = mongo_hook.get_conn()
        client.admin.command('ping')  # Ping MongoDB server
        print("MongoDB connection successful.")
        client.close()
    except Exception as e:
        print(f"MongoDB connection failed: {e}")
        raise

# Fungsi untuk mengecek koneksi MySQL
def check_mysql_connection():
    try:
        mysql_hook = MySqlHook(mysql_conn_id="mysql_default")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        print(f"MySQL connection successful. Result: {result}")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"MySQL connection failed: {e}")
        raise

# Fungsi untuk mengecek apakah tabel MySQL kosong
def is_mysql_table_empty():
    mysql_hook = MySqlHook(mysql_conn_id="mysql_default")
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    query = "SELECT COUNT(*) FROM patients"  # Cek jumlah data di tabel
    cursor.execute(query)
    row_count = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()
    return row_count == 0  # True jika tabel kosong, False jika ada data

# Fungsi ambil last updatedAt dari MySQL
def get_last_updatedat():
    # Define timezone
    local_tz = pytz.timezone("Asia/Jakarta")  # Sesuaikan timezone dengan kebutuhan

    mysql_hook = MySqlHook(mysql_conn_id="mysql_default")
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    query = "SELECT MAX(updatedAt) AS updatedAt FROM patients"  # Ambil nilai max dari kolom updatedAt
    cursor.execute(query)
    last_updated_at = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()
    
    if last_updated_at is None:
        print("Tabel kosong, tidak ada data di kolom updatedAt.")
        return None
    else:
        # Convert to desired format
        last_updated_at = last_updated_at.replace(tzinfo=pytz.UTC).astimezone(local_tz)
        formatted_date = last_updated_at.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+07:00"  # Truncate microseconds to 3 digits
        print(f"Last updatedAt: {formatted_date}")
        return formatted_date


# Fungsi untuk mengambil data dari MongoDB
def fetch_data_to_parquet(**kwargs):
    """
    Fetch data from MongoDB. 
    - Jika tabel MySQL kosong, ambil semua data.
    - Jika tabel tidak kosong, ambil data berdasarkan `updatedAt`.
    """
    mongo_hook = MongoHook(conn_id="mongo_default")
    client = mongo_hook.get_conn()
    db = client["training"]  # Nama database
    collection = db["vaccine"]  # Nama koleksi

    # Cek apakah tabel MySQL kosong
    table_empty = is_mysql_table_empty()
    if table_empty:
        query = {}  # Ambil semua data
        print("MySQL table is empty. Fetching all data from MongoDB.")
    else:
        # Ambil data berdasarkan `updatedAt`
        last_updated_at = get_last_updatedat()
        print(last_updated_at)
        query = {"updatedAt": {"$gt": last_updated_at}}
        print(f"MySQL table has data. Fetching incremental data (updatedAt > {last_updated_at}).")

    # Query MongoDB
    documents = list(collection.find(query))

    # Convert ke Pandas DataFrame dan pastikan semua kolom data nested menjadi JSON string
    if documents:
        # Flatten data
        flattened_data = []
        for doc in documents:
            vaccination = doc.get('vaccination', {})
            # Jika vaccination adalah list, ambil elemen pertama, jika ada
            if isinstance(vaccination, list):
                vaccination = vaccination[0] if vaccination else {}
            
            vaccine_location = vaccination.get('vaccineLocation', {})
            
            # Konversi vaccineStatus ke integer
            vaccine_status = vaccination.get('vaccineStatus', None)
            try:
                vaccine_status = int(vaccine_status) if vaccine_status is not None else None
            except ValueError:
                vaccine_status = None  # Jika gagal konversi, set ke None
            
            flattened_data.append({
                '_id': str(doc['_id']),
                'profession': doc.get('vaccinePatient', {}).get('profession', None),
                'fullName': doc.get('vaccinePatient', {}).get('fullName', None),
                'gender': doc.get('vaccinePatient', {}).get('gender', None),
                'bornDate': doc.get('vaccinePatient', {}).get('bornDate', None),
                'mobileNumber': doc.get('vaccinePatient', {}).get('mobileNumber', None),
                'nik': doc.get('vaccinePatient', {}).get('nik', None),
                'channel': doc.get('channel', None),
                'createdAt': doc.get('createdAt', None),
                'updatedAt': doc.get('updatedAt', None),
                'vaccineDate': vaccination.get('vaccineDate', None),
                'vaccineLocationName': vaccine_location.get('name', None),
                'faskesCode': vaccine_location.get('faskesCode', None),
                'vaccineCode': vaccination.get('vaccineCode', None),
                'vaccineStatus': vaccine_status,
                'type': vaccination.get('type', None),
            })
        
        df = pd.DataFrame(flattened_data)

        # Simpan ke file Parquet
        os.makedirs(PARQUET_DIR, exist_ok=True)
        parquet_file = os.path.join(PARQUET_DIR, f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")
        df.to_parquet(parquet_file, index=False)
        print(f"Data saved to Parquet file: {parquet_file}")

        # Simpan path file Parquet ke XCom
        kwargs['ti'].xcom_push(key='parquet_file', value=parquet_file)
        print(list(df.columns))
        print(df)
    else:
        print("No data found to save to Parquet.")
    client.close()

# Fungsi untuk membaca Parquet dan memasukkan data ke MySQL
def insert_data_from_parquet(**kwargs):
    parquet_file = kwargs['ti'].xcom_pull(key='parquet_file', task_ids='fetch_data_to_parquet')
    if not parquet_file:
        print("No Parquet file to process.")
        return

    # Baca Parquet ke Pandas DataFrame
    df = pd.read_parquet(parquet_file)

    # Insert data ke MySQL
    mysql_hook = MySqlHook(mysql_conn_id="mysql_default")
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO patients (_id, profession, fullName, gender, bornDate, mobileNumber, nik, channel, createdAt, updatedAt, vaccineDate, vaccineLocationName, faskesCode, vaccineCode, vaccineStatus, type)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        profession = VALUES(profession),
        fullName = VALUES(fullName),
        gender = VALUES(gender),
        bornDate = VALUES(bornDate),
        mobileNumber = VALUES(mobileNumber),
        nik = VALUES(nik),
        channel = VALUES(channel),
        createdAt = VALUES(createdAt),
        updatedAt = VALUES(updatedAt),
        vaccineDate = VALUES(vaccineDate),
        vaccineLocationName = VALUES(vaccineLocationName),
        faskesCode = VALUES(faskesCode),
        vaccineCode = VALUES(vaccineCode),
        vaccineStatus = VALUES(vaccineStatus),
        type = VALUES(type)
    """

    def convert_to_mysql_datetime(date_string):
        try:
            # Parse string to datetime object
            dt = datetime.fromisoformat(date_string.replace("Z", "+00:00"))
            # Return formatted datetime for MySQL
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None  # Return None if parsing fails

    for _, row in df.iterrows():
        # Convert `createdAt` and `updatedAt` to MySQL DATETIME format
        created_at = convert_to_mysql_datetime(row.get("createdAt"))
        updated_at = convert_to_mysql_datetime(row.get("updatedAt"))

        # Ganti None dengan nilai default untuk kolom yang bisa kosong
        values = (
            row["_id"] if row.get("_id") else "", 
            row.get("profession") if row.get("profession") else "",
            row.get("fullName") if row.get("fullName") else "",
            row.get("gender") if row.get("gender") else "",
            row.get("bornDate") if row.get("bornDate") else "",
            row.get("mobileNumber") if row.get("mobileNumber") else "",
            row.get("nik") if row.get("nik") else "",
            row.get("channel") if row.get("channel") else "",
            created_at,  # Gunakan hasil konversi untuk created_at
            updated_at,  # Gunakan hasil konversi untuk updated_at
            row.get("vaccineDate") if row.get("vaccineDate") else "",
            row.get("vaccineLocationName") if row.get("vaccineLocationName") else "",
            row.get("faskesCode") if row.get("faskesCode") else "",
            row.get("vaccineCode") if row.get("vaccineCode") else "",
            row.get("vaccineStatus") if row.get("vaccineStatus") else "",
            row.get("type") if row.get("type") else ""
        )

        print("Inserting or updating values:", values)  # Debug: Print the values that will be inserted/updated

        try:
            cursor.execute(insert_query, values)
        except Exception as e:
            print(f"Error inserting or updating data: {e}")
            continue  # Skip the row and continue with the next

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Data from Parquet file {parquet_file} has been inserted/updated into MySQL.")

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    'mongo_to_parquet_to_mysql',
    default_args=default_args,
    description='Pipeline to transfer data from MongoDB to Parquet and then to MySQL',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    # Task 1: Check MongoDB connection
    check_mongo = PythonOperator(
        task_id='check_mongo_connection',
        python_callable=check_mongo_connection,
    )

    # Task 2: Check MySQL connection
    check_mysql = PythonOperator(
        task_id='check_mysql_connection',
        python_callable=check_mysql_connection,
    )

    # Task 3: Fetch data from MongoDB and save to Parquet
    fetch_data = PythonOperator(
        task_id='fetch_data_to_parquet',
        python_callable=fetch_data_to_parquet,
        op_kwargs={'last_updated_at': datetime(2021, 8, 1)},  # Contoh timestamp
        provide_context=True,
    )
    
    # Task 4: Read Parquet and insert data to MySQL
    insert_data = PythonOperator(
        task_id='insert_data_from_parquet',
        python_callable=insert_data_from_parquet,
        provide_context=True,
    )
    
    # Task dependencies
    [check_mongo, check_mysql] >> fetch_data >> insert_data
