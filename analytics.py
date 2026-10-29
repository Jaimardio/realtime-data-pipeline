import os
import io
import time
import boto3
import pandas as pd

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:4566")
BUCKET_NAME = os.getenv("BUCKET_NAME", "clickstream-raw-bucket")
QUEUE_NAME = "clickstream-events-queue"

# Inicjalizacja klientów boto3 dla LocalStack
s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)

sqs_client = boto3.client(
    "sqs",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)

def setup_s3_trigger():
    print("⚙️ Konfiguracja S3 Event Trigger (S3 ➔ SQS)...")
    try:
        # 1. Tworzenie kolejki SQS w LocalStack
        queue_response = sqs_client.create_queue(QueueName=QUEUE_NAME)
        queue_url = queue_response['QueueUrl']
        
        # Pobranie ARN kolejki
        queue_attrs = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['QueueArn']
        )
        queue_arn = queue_attrs['Attributes']['QueueArn']
        
        # 2. Konfiguracja powiadomień z S3 do SQS
        s3_client.put_bucket_notification_configuration(
            Bucket=BUCKET_NAME,
            NotificationConfiguration={
                'QueueConfigurations': [
                    {
                        'Id': 'NewJSONFileTrigger',
                        'QueueArn': queue_arn,
                        'Events': ['s3:ObjectCreated:*'],
                        'Filter': {
                            'Key': {
                                'FilterRules': [
                                    {
                                        'Name': 'suffix',
                                        'Value': '.json'
                                    }
                                ]
                            }
                        }
                    }
                ]
            }
        )
        print("✅ S3 Event Trigger skonfigurowany pomyślnie! Każdy nowy plik JSON wygeneruje zdarzenie w SQS.")
    except Exception as e:
        print(f"⚠️ Informacja przy konfiguracji triggera S3: {e}")

def run_analytics():
    # Konfiguracja triggera na starcie procesu analitycznego
    setup_s3_trigger()
    
    print("\n📥 Pobieranie i transformacja danych ze S3 (Analytics Layer)...")
    
    # Automatyczne upewnienie się, że bucket istnieje
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
    except Exception:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
    
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
    if "Contents" not in response:
        print("⚠️ Brak plików w buckecie S3! Uruchom najpierw producer.py i consumer.py.")
        return

    events_data = []
    for obj in response["Contents"]:
        key = obj["Key"]
        if key.endswith(".json"):
            file_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
            content = file_obj["Body"].read().decode("utf-8")
            events_data.append(pd.read_json(io.StringIO(content), typ="series"))

    if not events_data:
        print("⚠️ Nie znaleziono plików JSON.")
        return

    # Tworzenie DataFrame
    df = pd.DataFrame(events_data)
    print(f"\n✅ Załadowano {len(df)} surowych zdarzeń do DataFrame.\n")

    print("=" * 60)
    print("📊 ZAAWANSOWANY RAPORT ANALITYCZNY: E-COMMERCE CLICKSTREAM")
    print("=" * 60)

    # 1. Podstawowy rozkład akcji
    action_counts = df["action"].value_counts().reset_index()
    action_counts.columns = ["Akcja", "Liczba_Zdarzen"]
    print("\n1️⃣ Rozkład zdarzeń w systemie:")
    print(action_counts.to_string(index=False))

    # 2. Lejek konwersji (Conversion Funnel Analysis)
    print("\n2️⃣ Analiza Lejek Konwersji (Conversion Funnel):")
    actions_dict = df["action"].value_counts().to_dict()
    views = actions_dict.get("view", 0)
    carts = actions_dict.get("add_to_cart", 0)
    clicks = actions_dict.get("click", 0)
    
    if views > 0:
        cart_conversion = (carts / views) * 100
        print(f"   • Łączne wyświetlenia (Views): {views}")
        print(f"   • Dodania do koszyka (Carts):   {carts}")
        print(f"   • Współczynnik konwersji View ➔ Cart: {cart_conversion:.2f}%")
    else:
        print("   • Brak wystarczających danych o wyświetleniach do obliczenia lejka.")

    # 3. Analiza aktywności użytkowników
    if "user_id" in df.columns:
        user_activity = df["user_id"].value_counts().reset_index()
        user_activity.columns = ["Uzytkownik", "Liczba_Akcji"]
        print("\n3️⃣ Top 3 najbardziej aktywnych użytkowników (Power Users):")
        print(user_activity.head(3).to_string(index=False))

    # 4. Eksport danych do warstwy analitycznej (Parquet)
    output_dir = "analytics_output"
    os.makedirs(output_dir, exist_ok=True)
    
    action_summary_path = os.path.join(output_dir, "action_summary.parquet")
    action_counts.to_parquet(action_summary_path, index=False)
    
    if "user_id" in df.columns:
        user_activity_path = os.path.join(output_dir, "user_activity.parquet")
        user_activity.to_parquet(user_activity_path, index=False)

    print("\n" + "=" * 60)
    print(f"💾 Zapisano przetworzone tabele analityczne w formacie Parquet do folderu: '{output_dir}/'")
    print("=" * 60)

if __name__ == "__main__":
    print("🚀 Uruchomiono cykliczny proces analityczny (Micro-batch mode).")
    print("⏱️ Analityka będzie się odświeżać automatycznie co 1 minutę. (Naciśnij Ctrl+C, aby zatrzymać).\n")
    
    try:
        while True:
            print(f"\n--- [Cykl analityczny: {time.strftime('%Y-%m-%d %H:%M:%S')}] ---")
            run_analytics()
            
            print("⏳ Oczekiwanie 60 sekund do kolejnego przeliczenia...")
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n🛑 Zatrzymano proces analityczny.")