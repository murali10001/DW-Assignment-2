import threading
import queue
import sqlite3
import time
import random
from datetime import datetime

# ==========================================
# 1. MESSAGE BROKER (Simulates MQTT Server)
# ==========================================
# In reality, this would be a separate server running Eclipse Mosquitto or AWS IoT.
# Here, we use a thread-safe Queue to pass messages between our "devices" and "servers".
message_queue = queue.Queue()

# ==========================================
# 2. SENSOR NODE (Simulates physical IoT device)
# ==========================================
# In reality, this would be C++ code running on a physical ESP32 or Raspberry Pi.
def sensor_device(device_id, metric_name, min_val, max_val, interval):
    """Simulates a sensor constantly reading and transmitting data."""
    while True:
        value = round(random.uniform(min_val, max_val), 2)
        payload = {
            "device_id": device_id,
            "metric": metric_name,
            "value": value,
            "timestamp": datetime.now()
        }
        # Publish to the "Broker"
        message_queue.put(payload)
        print(f"📡 [Sensor: {device_id}] Transmitted {value} for {metric_name}")
        time.sleep(interval)

# ==========================================
# 3. ETL / INGESTION WORKER (Simulates Cloud Server)
# ==========================================
# In reality, this is a Python/Node.js script running on a cloud server 
# that listens to MQTT and writes to a Cloud Data Warehouse like Snowflake or BigQuery.
def data_warehouse_ingestion():
    """Listens to the message queue and saves data to the warehouse."""
    # Each thread needs its own DB connection. WAL mode allows concurrent reading/writing.
    conn = sqlite3.connect('smart_home_advanced.db', check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL;')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS iot_data (
            timestamp DATETIME,
            device_id TEXT,
            metric_type TEXT,
            metric_value REAL
        )
    ''')
    
    while True:
        # Pull data from the queue
        payload = message_queue.get()
        
        # Load data into the Warehouse
        conn.execute('''
            INSERT INTO iot_data (timestamp, device_id, metric_type, metric_value)
            VALUES (?, ?, ?, ?)
        ''', (payload['timestamp'], payload['device_id'], payload['metric'], payload['value']))
        conn.commit()
        
        message_queue.task_done()

# ==========================================
# 4. AUTOMATION RULE ENGINE (Simulates Analytics Server)
# ==========================================
# In reality, this is a separate service like Home Assistant or a cron job.
def rule_engine():
    """Periodically queries the data warehouse to trigger automations."""
    conn = sqlite3.connect('smart_home_advanced.db', check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL;')
    
    while True:
        time.sleep(5) # Run analytics every 5 seconds
        
        cursor = conn.cursor()
        cursor.execute('''
            SELECT AVG(metric_value) FROM (
                SELECT metric_value FROM iot_data 
                WHERE metric_type = 'temperature' 
                ORDER BY timestamp DESC LIMIT 3
            )
        ''')
        result = cursor.fetchone()[0]
        
        if result:
            avg_temp = round(result, 2)
            print(f"\n📊 [Rule Engine] 3-Reading Temp Average is {avg_temp}°C")
            if avg_temp > 25.0:
                print("⚙️  [ACTION] -> Sending command to AC: Turn ON (Cooling)\n")
            elif avg_temp < 20.0:
                print("⚙️  [ACTION] -> Sending command to HVAC: Turn ON (Heating)\n")

# ==========================================
# MAIN EXECUTION (Starts all threads)
# ==========================================
if __name__ == "__main__":
    print("Starting Distributed Smart Home Simulation...\n" + "-"*40)
    
    # 1. Start the Ingestion Worker (Daemon thread runs in background)
    ingestion_thread = threading.Thread(target=data_warehouse_ingestion, daemon=True)
    ingestion_thread.start()
    
    # 2. Start the Rule Engine (Daemon thread)
    engine_thread = threading.Thread(target=rule_engine, daemon=True)
    engine_thread.start()
    
    # 3. Start Sensors (Daemon threads)
    temp_sensor = threading.Thread(target=sensor_device, args=("living_room_temp", "temperature", 18.0, 30.0, 2), daemon=True)
    temp_sensor.start()
    
    # Let the simulation run for 15 seconds, then exit
    try:
        time.sleep(15)
        print("\nSimulation complete. Shutting down threads safely.")
    except KeyboardInterrupt:
        print("\nSimulation stopped by user.")