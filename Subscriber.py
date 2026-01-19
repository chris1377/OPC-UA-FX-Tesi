"""
DESCRIZIONE:
    Subscriber OPC UA PubSub con scrittura su InfluxDB.
    Riceve pacchetti UDP (UADP), estrae i dati di telemetria e li salva su InfluxDB.
    
    CARATTERISTICHE:
    - Scrittura Asincrona (Non bloccante): Il loop di ricezione non aspetta il DB.
    - Batching: I dati vengono inviati al DB a blocchi (500pt) o a tempo (1s)
"""

import asyncio
import logging
import time
from datetime import datetime
from asyncua import ua, pubsub
from asyncua.pubsub.udp import UdpSettings
from dataclasses import dataclass, field

from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import ASYNCHRONOUS

# --- CONFIGURAZIONE DATABASE ---
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "my-super-secret-token-auth-token"
INFLUX_ORG = "unibo"
INFLUX_BUCKET = "tesi_tsn"

# --- CONFIGURAZIONE PUBSUB ---
# Configurazione dei parametri di rete PubSub
@dataclass
class PubSubCFG:
    PublisherID: ua.Variant = field(default_factory=lambda: ua.Variant(ua.UInt16(1)))
    WriterGroupId: ua.UInt16 = field(default_factory=lambda: ua.UInt16(1))
    DataSetWriterId: ua.UInt16 = field(default_factory=lambda: ua.UInt16(100)) 
    Url: str = "opc.udp://0.0.0.0:4840"

CFG = PubSubCFG()

# Definisce la struttura attesa del DataSet in ingresso
def create_meta_data():
    dataset = pubsub.DataSetMeta.Create("100")
    dataset.add_scalar("JointPosition", ua.VariantType.Double)
    dataset.add_scalar("Temperature", ua.VariantType.Double)
    dataset.add_scalar("Vibration", ua.VariantType.Double)
    dataset.add_scalar("Voltage", ua.VariantType.Double)
    return dataset

# Callback di gestione
class BatchingCallback:
    # Gestisce gli eventi di successo o errore della scrittura asincrona su DB
    def success(self, conf: (str, str, str), data: str):
        pass

    def error(self, conf: (str, str, str), data: str, exception: Exception):
        print(f"ERRORE SCRITTURA ASINCRONA: {exception}")

# Classe principale che gestisce la ricezione dei pacchetti OPC UA FX e il buffering verso InfluxDB.
class OnDataReceived:
    def __init__(self):
        # --- INIZIALIZZAZIONE CONNESSIONE DB ASINCRONA ---
        print(f"Connessione a InfluxDB ({INFLUX_URL}) in modalità ASYNC...")
        
        self.client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        
        # 1. DEFINIAMO LE REGOLE DEL BATCHING
        # batch_size=500: Spedisci se hai accumulato 500 punti
        # flush_interval=1000: Spedisci comunque se è passato 1 secondo (1000ms)
        # retry_interval=5000: Se fallisce, riprova dopo 5 secondi
        write_options = WriteOptions(batch_size=500, 
                                     flush_interval=1000, 
                                     jitter_interval=0, 
                                     retry_interval=5000)

        # 2. ISTANZIAMO LE CALLBACK
        callback = BatchingCallback()

        # 3. CREIAMO L'API ASINCRONA
        try:
            self.write_api = self.client.write_api(
                write_options=ASYNCHRONOUS,
                write_options_config=write_options,
                success_callback=callback.success,
                error_callback=callback.error,
                retry_callback=callback.error
            )
            print("Client Async Pronto (Batch: 500pt o 1s)")
        except Exception as e:
             print(f"Errore inizializzazione DB: {e}")

    # Callback invocata dalla libreria asyncua ogni volta che arriva un pacchetto UDP valido
    async def on_dataset_received(self, _meta: pubsub.DataSetMeta, fields: list[pubsub.DataSetValue]):
        # Helper per estrarre il valore numerico puro
        def get_val(obj):
            while hasattr(obj, 'Value'):
                obj = obj.Value
            return obj

        try:
            # ESTRIAMO IL TIMESTAMP ORIGINALE (TSN)
            # Usiamo il tempo in cui il dato è stato generato dal Publisher, non quando è arrivato.
            ts_source = None
            if hasattr(fields[0], 'Value') and hasattr(fields[0].Value, 'SourceTimestamp'): 
                 ts_source = fields[0].Value.SourceTimestamp
            
            # Nel caso dovesse mancare prendiamo il tempo attuale
            if ts_source is None:
                ts_source = datetime.now()
            
            # ESTRIAMO I DATI (Ordine posizionale basato su create_meta_data)
            pos = float(get_val(fields[0]))
            temp = float(get_val(fields[1]))
            vib = float(get_val(fields[2]))
            volt = float(get_val(fields[3]))
            
            # --- PREPARAZIONE PUNTO INFLUX ---
            p = Point("misurazioni_fx") \
                .tag("source", "Advantech_Device") \
                .field("JointPosition", pos) \
                .field("Temperature", temp) \
                .field("Vibration", vib) \
                .field("Voltage", volt) \
                .time(ts_source)
            
            # --- SCRITTURA ASINCRONA (NON BLOCCANTE) ---
            
            self.write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=p)

        except Exception as e:
            print(f"Errore elaborazione pacchetto locale: {e}")

    # Monitora lo stato della connessione PubSub
    async def on_state_change(self, meta: pubsub.DataSetMeta, state: ua.PubSubState):
        print(f"Stato connessione: {state.name}")

    # Chiusura pulita: forza l'invio dei dati rimasti nel buffer
    def close(self):
        print("Svuotamento buffer e chiusura connessione DB...")
        try:
            self.write_api.close() # Forza l'invio dei dati rimasti
            self.client.close()
            print("chiuso correttamente.")
        except Exception as e:
             print(f"Errore durante la chiusura: {e}")


if __name__ == "__main__":
    handler_ref = None
    try:
        # quando chiudo il sub termino il runner ma non il salvataggio dati per evitare di perderli e quindi salvo un riferimento in modo che possa eseguire anche quando runner è chiuso
        # Configuro manualmente event loop non usando (asyncio.run())
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Salvo riferimento oggetto che gestisce la ricezione dati (per chiuderlo nel finally)
        handler_ref = OnDataReceived()
        metadata = create_meta_data()

        
        logging.basicConfig(level=logging.WARNING)
        print(f"--- SUBSCRIBER TSN START (ID Writer: {CFG.DataSetWriterId}) ---")

        # Configurazione del ReaderGroup
        # Controllo che i dati siano del publisher 1, writerGroup 1 e dataset 100
        reader_group = pubsub.ReaderGroup.new(
            name="ReaderGroup1",
            enable=True,
            reader=[
                pubsub.DataSetReader.new(
                    CFG.PublisherID,
                    CFG.WriterGroupId,
                    CFG.DataSetWriterId, # Leggo set di dati numero 100
                    metadata,
                    name="FXReader",
                    subscribed=handler_ref,
                    enabled=True,
                )
            ],
        )

        # Configurazione della connessione UDP
        connection = pubsub.PubSubConnection.udp_uadp(
            "Subscriber Connection",
            ua.UInt16(1), # Ascolto dati del dispositvo con id 1
            UdpSettings(Url=CFG.Url),
            reader_groups=[reader_group],
        )



        ps = pubsub.PubSub.new(connections=[connection])
        connection._app = ps

        async def runner(): # loop principale
             print("In attesa di pacchetti (Scrittura Async attiva)...")
             async with ps:
                while True:
                    await asyncio.sleep(1)

        loop.run_until_complete(runner()) # avvio loop

    except KeyboardInterrupt:
        print("Stop richiesto dall'utente.")
    finally:
        # Questo blocco viene eseguito SEMPRE, anche in caso di errore.
        # È fondamentale chiudere l'handler per salvare gli ultimi dati nel buffer InfluxDB.
        if handler_ref:
            handler_ref.close() # Flush
