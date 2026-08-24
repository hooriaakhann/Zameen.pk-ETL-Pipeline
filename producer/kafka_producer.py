import json
import time

import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "zameen_property_listings"
LISTINGS_URL = "https://www.zameen.com/Homes/Islamabad-3-1.html"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


def parse_listing(listing):
    try:
        title = listing.select_one("h2").get_text(strip=True)
        price = listing.select_one(".price").get_text(strip=True)
        location = listing.select_one(".location").get_text(strip=True)
        details = listing.select_one(".list-save-search").get_text("|", strip=True).split("|")

        area = next((item for item in details if "Marla" in item or "Kanal" in item), "")
        bedrooms = next((item for item in details if "Bed" in item), "0").split()[0]
        bathrooms = next((item for item in details if "Bath" in item), "0").split()[0]

        return {
            "title": title,
            "price": price,
            "location": location,
            "area": area,
            "bedrooms": bedrooms,
            "bathrooms": bathrooms,
            "source": "zameen.com",
        }
    except (AttributeError, ValueError, IndexError):
        return None


def scrape_and_publish():
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(LISTINGS_URL, headers=headers, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    listings = soup.select(".fc-container .ef447dde")

    sent = 0
    for listing in listings:
        data = parse_listing(listing)
        if not data:
            continue
        producer.send(KAFKA_TOPIC, value=data)
        sent += 1
        time.sleep(0.2)

    producer.flush()
    print(f"Published {sent} listings to {KAFKA_TOPIC}")


if __name__ == "__main__":
    scrape_and_publish()
