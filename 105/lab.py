import httpx  # requests capability, but can work with async
from prefect import flow, task
from prefect.deployments import run_deployment


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
def weather_flow(lat: float, lon: float):
    temp = fetch_weather(lat, lon)
    result = save_weather(temp)
    return result

@flow
def rain_flow(lat: float, lon: float):
    rain = fetch_rain(lat, lon)
    result = save_rain(rain)
    return result

@flow(name="105-weather_pipeline")
def pipeline(lat: float = 38.9, lon: float = -77.0):
    weather = weather_flow(lat, lon)
    rain = rain_flow(lat, lon)
    animal_facts = run_deployment(name="animal-facts/default")

    return [weather, rain, animal_facts]


if __name__ == "__main__":
    pipeline()
