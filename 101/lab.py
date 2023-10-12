import httpx  # requests capability, but can work with async
from prefect import flow, task


@task
def fetch_weather(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
    )
    most_recent_temp = float(weather.json()["hourly"]["temperature_2m"][0])
    return most_recent_temp

@task
def save_weather(temp: float):
    with open("weather.csv", "w+") as w:
        w.write(str(temp))
    return "Successfully wrote temp"

@task
def fetch_cloudcover_mid(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    cloudcover_mid = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="cloudcover_mid"),
    )
    most_recent_cloudcover = float(cloudcover_mid.json()["hourly"]["cloudcover_mid"][0])
    return most_recent_cloudcover

@task
def save_cloudcover_mid(cloudcover_mid: float):
    with open("cloudcover_mid.csv", "w+") as w:
        w.write(str(cloudcover_mid))
    return "Successfully wrote cloudcover_mid"

@task
def fetch_precipitation(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    precipitation = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="precipitation"),
    )
    most_recent_precipitation = float(precipitation.json()["hourly"]["precipitation"][0])
    return most_recent_precipitation

@task
def save_precipitation(precipitation: float):
    with open("precipitation.csv", "w+") as w:
        w.write(str(precipitation))
    return "Successfully wrote precipitation"

@task
def fetch_rain(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    rain = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="rain"),
    )
    most_recent_rain = float(rain.json()["hourly"]["rain"][0])
    return most_recent_rain

@task
def save_rain(rain: float):
    with open("rain.csv", "w+") as w:
        w.write(str(rain))
    return "Successfully wrote rain"

@flow
def pipeline(lat: float = 38.9, lon: float = -77.0):
    temp = fetch_weather(lat, lon)
    result = save_weather(temp)
    cloudcover_mid = fetch_cloudcover_mid(lat, lon)
    result2 = save_cloudcover_mid(cloudcover_mid)
    precipitation = fetch_precipitation(lat, lon)
    result3 = save_precipitation(precipitation)
    rain = fetch_rain(lat, lon)
    result4 = save_rain(rain)
    return [result, result2, result3, result4]


if __name__ == "__main__":
    pipeline.serve(name='forecast-pipeline')
