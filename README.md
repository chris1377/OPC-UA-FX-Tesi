# OPC UA PubSub Simulation with InfluxDB

*Read this in other languages: [Italiano 🇮🇹](README.it.md)*

---

This repository contains a **complete OPC UA PubSub (Publisher/Subscriber) simulation** based on **UDP (UADP)**, developed in **Python** using the **asyncua (opcua-asyncio)** library, with data persistence on **InfluxDB v2** via high-performance asynchronous writing. 

The project is designed for **industrial / research** contexts, such as:
- TSN / IIoT simulations
- OPC UA PubSub load testing
- High-frequency data acquisition
- OPC UA → Time Series Database integration

## 📡 Publisher – `Publisher.py`

Simulates an **industrial machine**.

### Main Features
- **OPC UA Server (TCP)**
  - Exposes readable measurements
  - Exposes a writable control variable (`Speed_Input`)
  - Compatible with **UaExpert** or other OPC UA clients
- **OPC UA PubSub Publisher (UDP – UADP)**
  - High-frequency cyclic publication
  - Payload with **source timestamp**
  - WriterGroup and DataSetWriter configuration

### Simulated Variables
| Variable | Description |
| :--- | :--- |
| `JointPosition` | Sine wave (joint position) |
| `Temperature` | Sawtooth waveform |
| `Vibration` | Random noise proportional to speed |
| `Voltage` | Constant value |

### Real-Time Control
The variable:
```text
Speed_Input (Double)
```
can be modified **in real-time** via OPC UA TCP (e.g., UaExpert) to change the simulation frequency.

---

## 📥 Subscriber – `Subscriber.py`

Receives **OPC UA PubSub UDP** packets, decodes the dataset, and saves the data to **InfluxDB v2**.

### Key Features
- UADP decoding compliant with OPC UA metadata
- **Asynchronous** (non-blocking) writing
- **Automatic batching**

### Batching Strategy
- **500 points** per batch
- **Flush every 1 second**

---

## 🗄️ InfluxDB

The Subscriber uses **InfluxDB v2** as a time-series database.

### Configuration (Subscriber.py)
```python
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "my-super-secret-token-auth-token"
INFLUX_ORG = "unibo"
INFLUX_BUCKET = "tesi_tsn"
```

### Measurement
All data is saved in the measurement:
```text
misurazioni_fx
```
with the following fields:
- JointPosition
- Temperature
- Vibration
- Voltage

---

## ▶️ How to Run the Project

### 1️⃣ Start InfluxDB
Ensure InfluxDB v2 is running and that the following are correctly configured:
- bucket
- token
- organization

### 2️⃣ Start the Subscriber
```bash
python Subscriber.py
```
The subscriber will start listening for UDP packets.

### 3️⃣ Start the Publisher
```bash
python Publisher.py
```
The publisher will:
- start the OPC UA TCP server
- begin UDP PubSub publication

### 4️⃣ (Optional) Control via UaExpert
Connect to:
```text
opc.tcp://<IP_PUBLISHER>:4840
```
and modify `Speed_Input` to vary the simulation in real-time.

---

## 📦 Dependencies
```bash
pip install asyncua influxdb-client
```
