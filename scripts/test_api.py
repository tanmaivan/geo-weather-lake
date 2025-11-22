import os
import requests
from dotenv import load_dotenv
import json
from pathlib import Path

load_dotenv(Path(__file__).resolve().parent.parent / ".env")
API_KEY = os.getenv("WEATHERBIT_API_KEY")

TEST_LOCATION = {"lat": 10.7769, "lon": 106.7009}

url = f"https://api.weatherbit.io/v2.0/current?lat={TEST_LOCATION['lat']}&lon={TEST_LOCATION['lon']}&key={API_KEY}&units=M"

resp = requests.get(url)
if resp.status_code == 200:
    data = resp.json()
    print(json.dumps(data, indent=4))
else:
    print("API call failed:", resp.status_code, resp.text)
