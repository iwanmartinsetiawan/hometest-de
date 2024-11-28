
# Home Test - Iwan Martin Setiawan

Ini adalah tools untuk kebutuhan Home Test Data Engineer


## Usage/Examples

```shell
docker compose up --build -d
```


## Demo

Setelah menjalankan command diatas, dan semua container sudah berjalan normal.
silahkan akses URL ini dibrowser :
http://localhost:48080/

Maka akan muncul tampilan login Airflow.
Gunakan login berikut : 
>
>**user/password : airflow**

Buat koneksi baru untuk databasenya.
Masuk ke menu Admin -> connection.

Klik icon tambah atau +, dan masukkan config berikut :

MongoDB:

    Connection ID: mongo_default
    Connection Type: MongoDB
    Host: localhost
    Port: 37017
    Extra: {"authSource": "admin"}

MySQL:

    Connection ID: mysql_default
    Connection Type: MySQL
    Host: localhost
    Port: 43306
    Schema: dev
    Login: root
    Password: root

