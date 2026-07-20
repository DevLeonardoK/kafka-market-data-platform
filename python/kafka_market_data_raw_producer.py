from kafka import KafkaProducer
import json
from time import sleep
import random
from randomtimestamp import randomtimestamp
from datetime import datetime

bootstrap_servers = [
    "kafka_broker_1:5091",
    "kafka_broker_2:5092",
    "kafka_broker_3:5093"
]

market_data_raw_topic = "market_data_raw_topic"

brokers_names = [
    "Charles Schwab",
    "Fidelity Investments",
    "Vanguard Group",
    "Interactive Brokers",
    "J.P. Morgan"
]

symbol_names = [
    "NVDA", "AAPL", "MSFT", "GOOGL", "META", "AVGO", "AMD", "INTC",
    "AMZN", "TSLA", "WMT", "DIS", "HD", "NKE", "NFLX", 
    "JPM", "V", "MA", "LLY", "JNJ", "PFE",
    "VALE", "PBR", "ITUB", "BBD", "ABEV", "NU", "ERJ", "MELI"
]

currency_names = ["BRL", "USD", "EUR"]

exchange_local_names = [
    "B3",          # Brasil (Brasil, Bolsa, Balcão)
    "NYSE",        # Estados Unidos (New York Stock Exchange)
    "NASDAQ",      # Estados Unidos
    "LSE",         # Reino Unido (London Stock Exchange)
    "TSE",         # Japão (Tokyo Stock Exchange)
    "HKEX",        # Hong Kong (Hong Kong Exchanges and Clearing)
    "TSX",         # Canadá (Toronto Stock Exchange)
    "FWB",         # Alemanha (Frankfurt Stock Exchange / Börse Frankfurt)
    "ASX",         # Austrália (Australian Securities Exchange)
    "Euronext"     # Europa (Bolsa pan-europeia: Paris, Amsterdã, Bruxelas, etc.)
]


kafka_market_data_producer = KafkaProducer(
        bootstrap_servers = bootstrap_servers,
        api_version=(4,3,1),
        value_serializer = lambda v: json.dumps(v).encode("utf-8"), #dict/object to string
        key_serializer = lambda k: k.encode("utf-8")
    )

def data_market_raw_generate():
    year = datetime.today().year
    data_market_raw = {
        "id": random.randrange(start=1, step=1, stop=10000000),
        "symbol": random.choice(symbol_names),
        "price": random.uniform(40, 300),
        "broker": random.choice(brokers_names),
        "quantity": random.randint(1,1000),
        "currency": random.choice(currency_names),
        "exchange": random.choice(exchange_local_names),
        "timestamp": datetime.now().timestamp()
    }

    return data_market_raw

if __name__ == "__main__":

    try:
        while True:
            
            data = data_market_raw_generate()

            kafka_market_data_producer.send(
                topic=market_data_raw_topic,
                key=data["symbol"],
                value=data,
            )

            sleep(35)

    except Exception:
        raise

    finally:
        kafka_market_data_producer.close()

