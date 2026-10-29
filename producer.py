import json
import time
from confluent_kafka import Producer
from faker import Faker

# 1. Inicjalizacja biblioteki Faker
fake = Faker()

# 2. Konfiguracja Producenta Confluent Kafka
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

TOPIC_NAME = 'clickstream-events'

print('🚀 Producent (Confluent) wystartował! Generuję zdarzenia clickstream...')


# Funkcja zwrotna (callback) potwierdzająca dostarczenie wiadomości do Kafki
def delivery_report(err, msg):
  if err is not None:
    print(f'❌ Błąd wysyłania wiadomości: {err}')
  else:
    print(
        f'✅ Dostarczono do Kafki -> Temat: {msg.topic()} | Partycja:'
        f' {msg.partition()} | Offset: {msg.offset()}'
    )


try:
  while True:
    # 3. Tworzenie pojedynczego zdarzenia
    click_event = {
        'user_id': f"user_{fake.random_int(min=1000, max=9999)}",
        'session_id': fake.uuid4(),
        'timestamp': fake.iso8601(),
        'page': fake.random_element(
            elements=['/home', '/products', '/cart', '/checkout', '/contact']
        ),
        'action': fake.random_element(
            elements=['click', 'view', 'scroll', 'add_to_cart']
        ),
        'ip_address': fake.ipv4(),
    }

    # 4. Wysłanie wiadomości do Kafki (zamiana słownika na JSON i bajty)
    producer.produce(
        TOPIC_NAME,
        value=json.dumps(click_event).encode('utf-8'),
        callback=delivery_report,
    )

    # Obsługa zdarzeń zwrotnych (callbacków) w tle
    producer.poll(0)

    print(
        f"Wysłano lokalnie -> Użytkownik: {click_event['user_id']} | Akcja:"
        f" {click_event['action']}"
    )

    # 5. Odczekanie 1 sekundy
    time.sleep(1)

except KeyboardInterrupt:
  print('\n🛑 Zatrzymywanie producenta...')
  # Opróżnienie bufora przed zamknięciem (upewniamy się, że wszystko poleciało)
  producer.flush()