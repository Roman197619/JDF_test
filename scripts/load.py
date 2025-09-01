import json
import os
from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickhouseError
from dotenv import load_dotenv
import time
import json
import requests
from requests.exceptions import RequestException

load_dotenv()

CLICKHOUSE_CONFIG = {
    'host': os.getenv('CLICKHOUSE_HOST', 'localhost'),
    'port': int(os.getenv('CLICKHOUSE_PORT', 19000)),
    'user': os.getenv('CLICKHOUSE_USER', 'admin'),
    'password': os.getenv('CLICKHOUSE_PASSWORD', 'changeme'),
    'database': os.getenv('CLICKHOUSE_DB', 'JDF_DATABASE')
}

API_URL = os.getenv('API_URL', 'http://api.open-notify.org/astros.json')
MAX_RETRIES = int(os.getenv('MAX_RETRIES', 5))

try:
    client = Client(**CLICKHOUSE_CONFIG)
    client.execute('SELECT 1')
    print("Успешное подключение к ClickHouse.")
except ClickhouseError as e:
    print(f"Ошибка подключения к ClickHouse: {e}")
    exit(1)

def fetch_data(url, max_attempts=MAX_RETRIES):
    for attempt in range(1, max_attempts + 1):
        wait_time = 60 * attempt
        try:
            print(f"Попытка {attempt}/{max_attempts}...")
            response = requests.get(url, timeout=10)
            print(f"HTTP Status: {response.status_code}")
            
            if response.status_code in {400, 403, 404}:
                raise RequestException(f"Client error {response.status_code}. Stopping. Response: {response.text}")

            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    try:
                        wait_time = int(retry_after)
                    except ValueError:
                        pass  
                print(f"Rate limit exceeded. Retrying after {wait_time} seconds.")
                if attempt < max_attempts:
                    time.sleep(wait_time)
                    continue 
                else:
                    raise RequestException(f"429 Too Many Requests после {max_attempts} попыток.")

            response.raise_for_status()

            json_data = response.json()
            print("Данные успешно получены с API.")
            return json_data

        except RequestException as e:
            print(f"Ошибка сети/HTTP на попытке {attempt}: {e}")
            if attempt < max_attempts:
                print(f"Повторная попытка через {wait_time} сек...")
                time.sleep(wait_time)
            else:
                print("Все попытки исчерпаны.")
                raise

try:
    data = fetch_data(API_URL)

    if 'people' not in data or not isinstance(data['people'], list):
        raise ValueError("Непредвиденная структура ответа API: отсутствует ключ 'people' или это не список.")

    records = [(json.dumps(person),) for person in data['people']]

    if records: 
        client.execute(
            'INSERT INTO JDF_DATABASE.RAW_TABLE (raw_json) VALUES',
            records
        )
        print(f"Успешно вставлено {len(records)} записей в ClickHouse.")
    else:
        print("Нет данных для вставки.")

except (RequestException, ValueError, ClickhouseError) as e:
    print(f"Критическая ошибка: {e}")
    exit(1)
finally:
    client.disconnect()
    print("Работа скрипта завершена.")