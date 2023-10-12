import httpx  # requests capability, but can work with async
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect.artifacts import create_markdown_artifact

@task(retries=2, retry_delay_seconds=1, cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=2))
def fetch_weather(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
    )
    most_recent_temp = float(weather.json()["hourly"]["temperature_2m"][0])
    return most_recent_temp

@task(retries=2, retry_delay_seconds=1, cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=2))
def save_weather(temp: float):
    with open("weather.csv", "w+") as w:
        w.write(str(temp))
    return "Successfully wrote temp"

@flow(retries=2, retry_delay_seconds=1)
def pipeline(lat: float = 38.9, lon: float = -77.0):
    temp = fetch_weather(lat, lon)
    result = save_weather(temp)
    return result

@task
def mark_it_down(temp):
    markdown_report = f"""# Weather Report
## Recent weather
| Time        | Temperature |
|:--------------|-------:|
| Now | {temp} |
"""

    create_markdown_artifact(
        key="weather-report",
        markdown=markdown_report,
        description="Very scientific weather report",
    )

if __name__ == "__main__":
    pipeline.serve('weather_flow')