import json
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from confluent_kafka import Consumer, KafkaError

TOPIC_NAME = 'clickstream-events'
BUCKET_NAME = 'clickstream-raw-bucket'

# Konfiguracja S3: styl ścieżkowy + polityka ponowień
s3_config = Config(
    s3={'addressing_style': 'path'},
    retries={'max_attempts': 3, 'mode': 'standard'},
)

# Inicjalizacja klienta S3
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',  # Port LocalStack
    aws_access_key_id='test',
    aws_secret_access_key='test',
    config=s3_config,
    region_name='us-east-1',
)

# Jawne utworzenie kubełka w S3 Mock przed startem konsumenta
try:
  s3_client.head_bucket(Bucket=BUCKET_NAME)
  print(f"📦 Kubełek '{BUCKET_NAME}' już istnieje w S3 Mock.")
except ClientError:
  try:
    s3_client.create_bucket(Bucket=BUCKET_NAME)
    print(f'📦 Utworzono nowy kubełek w S3 Mock: {BUCKET_NAME}')
  except Exception as e:
    print(f'⚠️ Błąd podczas tworzenia kubełka: {e}')

# Konfiguracja Konsumenta Confluent Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'clickstream-s3-sink-group',
    'auto.offset.reset': 'earliest',
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC_NAME])

print(
    '👀 Konsument (Confluent) wystartował! Nasłuchuję Kafki i zapisuję dane do'
    ' S3 Mock...'
)

try:
  while True:
    msg = consumer.poll(1.0)

    if msg is None:
      continue
    if msg.error():
      if msg.error().code() == KafkaError._PARTITION_EOF:
        continue
      else:
        print(f'❌ Błąd Kafka: {msg.error()}')
        break

    # Deserializacja zdarzenia z Kafki
    event = json.loads(msg.value().decode('utf-8'))
    user_id = event['user_id']
    timestamp = event['timestamp']

    # Zamiana dwukropków w timestampie na bezpieczne znaki w nazwie pliku
    safe_timestamp = timestamp.replace(':', '-')
    file_name = f'events/{safe_timestamp}_{user_id}.json'

    # Przygotowanie danych jako bajty (zapobiega zrywaniu połączenia przez S3 Mock)
    json_bytes = json.dumps(event, indent=2).encode('utf-8')

    # Zapis do S3 Mock
    try:
      s3_client.put_object(
          Bucket=BUCKET_NAME,
          Key=file_name,
          Body=json_bytes,
          ContentType='application/json',
      )
      print(
          f'📥 Odebrano z Kafki & ☁️ Zapisano do S3 -> Plik: {file_name} | Akcja:'
          f" {event['action']}"
      )
    except Exception as s3_err:
      print(f'❌ Błąd zapisu do S3 Mock: {s3_err}')

except KeyboardInterrupt:
  print('\n🛑 Zatrzymywanie konsumenta...')
finally:
  consumer.close()