# E-commerce Clickstream Event-Driven Pipeline

A production-grade, event-driven data engineering pipeline designed to simulate, ingest, store, and process real-time e-commerce clickstream events.

## 🏗️ Architecture Flow
[ producer.py ] ➔ (Kafka Topic) ➔ [ consumer.py ] ➔ [ LocalStack S3 (Raw Storage) ]
│
(S3 Event Notification)
▼
[ query_parquet.py / DuckDB ] ◄── [ Parquet Files ] ◄── [ analytics.py (Micro-batch) ] ◄── [ SQS Queue ]
## 🛠️ Tech Stack
- **Streaming & Messaging:** Apache Kafka, Python (`confluent-kafka`)
- **Cloud Infrastructure (Local Mock):** LocalStack (AWS S3 & SQS)
- **Processing & Transformation:** Python, Pandas (Micro-batch architecture)
- **Storage & Analytics Layer:** Apache Parquet (`analytics_output/`)
- **Serving & Querying Layer:** DuckDB (`query_parquet.py`)
- **Containerization:** Docker Compose (`docker-compose.yml`)

## 📂 Project Structure
```text
realtime-data-pipeline/
│
├── analytics_output/      # Persistent analytical layer (.parquet files)
├── analytics.py           # Micro-batch analytics worker (runs every 60s & configures S3/SQS trigger)
├── consumer.py            # Kafka consumer ingesting events into LocalStack S3
├── producer.py            # E-commerce clickstream event generator
├── query_parquet.py       # Serving layer querying Parquet files via DuckDB
├── docker-compose.yml     # Local infrastructure setup (Kafka & LocalStack)
├── requirements.txt       # Python dependencies
└── README.md              # Project documentation

Key Features:
Event-Driven & Micro-batching: Automatically configures S3-to-SQS event triggers in LocalStack while utilizing a robust micro-batch loop (analytics.py) to process data safely and prevent resource overloads.

Decoupled Architecture: Clean separation between raw event ingestion (S3), transformation logic (Pandas/Parquet), and the analytical querying layer (DuckDB).

Fault Tolerance: Persistent local Parquet outputs ensure analytical reports survive container or environment restarts.

How to Run Locally
Clone the repository:
git clone https://github.com/Jaimardio/realtime-data-pipeline.git
cd realtime-data-pipeline

Start LocalStack & Kafka via Docker Compose:
docker compose up -d

Create and activate a virtual environment (recommended):
Windows (PowerShell):
python -m venv venv
venv\Scripts\Activate

macOS / Linux:
python3 -m venv venv
source venv/bin/activate

Install Python dependencies:
pip install -r requirements.txt

Run the pipeline components (in separate terminal windows):
Step 1 (Producer): python producer.py
Step 2 (Consumer): python consumer.py
Step 3 (Analytics Worker): python analytics.py (runs continuously in micro-batch mode)
Step 4 (Querying): python query_parquet.py (to inspect analytical results via DuckDB)

