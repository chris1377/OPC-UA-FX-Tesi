
# OPC UA PubSub Simulation with InfluxDB

  

Questo repository contiene una **simulazione completa OPC UA PubSub (Publisher/Subscriber)** basata su **UDP (UADP)**, sviluppata in **Python** utilizzando la libreria **asyncua (opcua-asyncio)**, con persistenza dei dati su **InfluxDB v2** tramite scrittura asincrona ad alte prestazioni.

  

Il progetto è pensato per contesti **industriali / di ricerca**, ad esempio:

- simulazioni TSN / IIoT

- test di carico PubSub OPC UA

- acquisizione dati ad alta frequenza

- integrazione OPC UA → Time Series Database

  



## 📡 Publisher – `Publisher.py`

  
Simula un **macchinario industriale**.

  

### Funzionalità principali

  

-  **OPC UA Server (TCP)**

- Espone misurazioni leggibili

- Espone una variabile di controllo scrivibile (`Speed_Input`)

- Compatibile con **UaExpert** o altri client OPC UA

  

-  **OPC UA PubSub Publisher (UDP – UADP)**

- Pubblicazione ciclica ad alta frequenza

- Payload con **timestamp di sorgente**

- Configurazione WriterGroup e DataSetWriter

  

### Variabili simulate


| `JointPosition` - Onda sinusoidale (posizione giunto) 

| `Temperature` - Andamento a dente di sega 

| `Vibration` - Rumore casuale proporzionale alla velocità 

| `Voltage` - Valore costante 

  

### Controllo in tempo reale

  

La variabile:

  

```

Speed_Input (Double)

```

  

può essere modificata **in tempo reale** via OPC UA TCP (es. UaExpert) per cambiare la frequenza della simulazione.

  

---

  

## 📥 Subscriber – `Subscriber.py`

  

Riceve i pacchetti **OPC UA PubSub UDP**, decodifica il dataset e salva i dati su **InfluxDB v2**.

  

### Caratteristiche chiave

  

-  Decodifica UADP conforme al metadata OPC UA

-  Scrittura **asincrona** (non bloccante)

-  **Batching automatico**

  

### Strategia di batching

  

-  **500 punti** per batch

-  **Flush ogni 1 secondo**

 
---

  

## 🗄️ InfluxDB

  

Il Subscriber utilizza **InfluxDB v2** come database time-series.

  

### Configurazione (Sunscriber.py)

  

```python

INFLUX_URL = "http://localhost:8086"

INFLUX_TOKEN = "my-super-secret-token-auth-token"

INFLUX_ORG = "unibo"

INFLUX_BUCKET = "tesi_tsn"

```

  

### Measurement

  

Tutti i dati vengono salvati nel measurement:

  

```

misurazioni_fx

```

  

con campi:

- JointPosition

- Temperature

- Vibration

- Voltage

  

---

  

## ▶️ Avvio del progetto

  

### 1️⃣ Avvia InfluxDB

  

Assicurati che InfluxDB v2 sia attivo e che:

- bucket

- token

- organizzazione

  

siano correttamente configurati.

  

### 2️⃣ Avvia il Subscriber

  

```bash

python  Subscriber.py

```

  

Il subscriber resta in ascolto dei pacchetti UDP.

  

### 3️⃣ Avvia il Publisher

  

```bash

python  Publisher.py

```

  

Il publisher:

- avvia il server OPC UA TCP

- inizia la pubblicazione PubSub UDP

  

### 4️⃣ (Opzionale) Controllo con UaExpert

  

Collegati a:

  

```

opc.tcp://<IP_PUBLISHER>:4840

```

  

e modifica `Speed_Input` per variare la simulazione in tempo reale.

  

---

  

## 📦 Dipendenze

  

```bash

pip  install  asyncua  influxdb-client

```

  

