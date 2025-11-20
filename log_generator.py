import json
import time
import random
import re
from datetime import datetime
from kafka import KafkaProducer

def serialize_json(obj):
    """
    Serializer function for KafkaProducer to convert Python dict to JSON bytes.
    """
    return json.dumps(obj).encode('utf-8')

def parse_windows_log_line(log_line):
    """
    Parses a single line from the Windows_2k.log file and extracts relevant fields.
    Format: YYYY-MM-DD HH:MM:SS, Severity Source Message
    """
    # Regex to capture timestamp, severity, source, and message
    match = re.match(r"(\d{{4}}-\d{{2}}-\d{{2}} \d{{2}}:\d{{2}}:\d{{2}}), (\w+)\s+(\w+)\s+(.*)", log_line)
    if match:
        timestamp_str, severity, source, message = match.groups()
        # Create a unique event_id for each log entry
        event_id = f"winlog_{datetime.now().strftime('%Y%m%d%H%M%S%f')}_{random.randint(0, 9999)}"
        return {
            "timestamp": timestamp_str,
            "event_id": event_id,
            "event_type": "WINDOWS_LOG", # General type for these logs
            "service_name": source,      # Source acts as a service name
            "severity": severity.upper(),
            "message": message.strip(),
            "details": {
                "raw_log": log_line.strip()
            }
        }
    else:
        # Fallback for unparsable lines
        event_id = f"unparsed_winlog_{datetime.now().strftime('%Y%m%d%H%M%S%f')}_{random.randint(0, 9999)}"
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "event_id": event_id,
            "event_type": "UNPARSED_LOG",
            "service_name": "unknown",
            "severity": "UNKNOWN",
            "message": log_line.strip(),
            "details": {
                "raw_log": log_line.strip()
            }
        }

def kafka_log_producer_from_file(log_file_path, kafka_bootstrap_servers='localhost:9092', topic='monitoring_logs', delay_seconds=0.1):
    """
    Reads log events from a file and sends them to a Kafka topic.
    """
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=serialize_json,
            acks='all',
            retries=5
        )
        print(f"Kafka Producer started for topic: {topic} on servers: {kafka_bootstrap_servers}")

        with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for i, line in enumerate(f):
                log_event = parse_windows_log_line(line)
                producer.send(topic, value=log_event)
                print(f"Sent log {i+1}: {log_event['severity']} - {log_event['message'][:80]}...")
                time.sleep(delay_seconds)

    except FileNotFoundError:
        print(f"Error: Log file not found at {log_file_path}")
    except Exception as e:
        print(f"Error in Kafka producer: {e}")
    finally:
        if producer:
            producer.flush()
            producer.close()
            print("Kafka Producer closed.")

if __name__ == "__main__":
    log_file = "Windows_2k.log" # Assuming the log file is in the same directory
    print(f"Starting log generator from {log_file}...")
    kafka_log_producer_from_file(log_file, delay_seconds=0.01) # Send logs faster for demo
