import pandas as pd
import requests

# Konfigurasi API
API_KEY = "xxxx"
BASE_URL = "http://api.weatherapi.com/v1/forecast.json"

# Fungsi untuk mengambil data cuaca dari API
def get_weather_data(city, days=1, aqi="no", alerts="no"):
    params = {
        "key": API_KEY,
        "q": city,
        "days": days,
        "aqi": aqi,
        "alerts": alerts,
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data for {city}: {response.status_code}")
        return None

# Membaca file CSV
file_path = "city_list.csv"  # Ganti dengan path file CSV Anda
df = pd.read_csv(file_path)

# Loop melalui daftar kota dan ambil data cuaca
weather_data = []
for city in df["city"]:
    print(f"Fetching weather data for {city}...")
    data = get_weather_data(city)
    if data:
        weather_data.append({
            "city": city,
            "temperature": data["current"]["temp_c"],
            "condition": data["current"]["condition"]["text"],
            "wind_kph": data["current"]["wind_kph"],
            "humidity": data["current"]["humidity"],
        })

# Menyimpan hasil ke file CSV
output_file = "hasil_cuaca.csv"
output_df = pd.DataFrame(weather_data)
output_df.to_csv(output_file, index=False)

print(f"Data cuaca berhasil disimpan ke {output_file}.")
