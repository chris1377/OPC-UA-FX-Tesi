import asyncio
import logging
import time
import csv
import sys
from asyncua import ua, pubsub
from asyncua.pubsub.udp import UdpSettings
from dataclasses import dataclass

# --- CONFIGURAZIONE ---
@dataclass
class PubSubCFG:
    # Ascolta su tutte le interfacce
    Url: str = "opc.udp://0.0.0.0:4840"

CFG = PubSubCFG()


# Es: "latenza_TSN_heavy.csv" oppure "latenza_NO_TSN_heavy.csv"
FILENAME = "dati_latenza_heavy.csv" 

# --- DEFINIZIONE DATI ---

def create_meta_data():
    dataset = pubsub.DataSetMeta.Create("Simple")
    
    # CAMPO 0: TimestampTX (Int64) 
    dataset.add_scalar("TimestampTX", ua.VariantType.Int64)
    
    # CAMPO 1: JointPosition (Double)
    dataset.add_scalar("JointPosition", ua.VariantType.Double)
    
    # CAMPO 2: Padding (String) 
    dataset.add_scalar("Padding", ua.VariantType.String)
    
    return dataset

class OnDataReceived:
    def __init__(self, filename):
        print(f"--- INIZIO REGISTRAZIONE SU FILE: {filename} ---")
        self.f = open(filename, 'w', newline='')
        self.writer = csv.writer(self.f)
        # Intestazione CSV
        self.writer.writerow(["SequenceID", "TX_Time_ns", "RX_Time_ns", "Latency_ms"])
        self.count = 0
        self.last_print = time.time()
        
    async def on_dataset_received(self, _meta: pubsub.DataSetMeta, fields: list[pubsub.DataSetValue]):
        # 1. TEMPO DI ARRIVO (RX) 
        rx_ns = time.time_ns()
        
        try:
            # 2. ESTRAZIONE TEMPO DI INVIO (TX)

            val_obj = fields[0]
            while hasattr(val_obj, 'Value'): 
                val_obj = val_obj.Value
            
            tx_ns = int(val_obj)

            # 3. CALCOLO LATENZA
            # (RX - TX) / 1.000.000 per avere millisecondi
            latency_ms = (rx_ns - tx_ns) / 1_000_000.0
            
            # 4. SALVATAGGIO SU CSV
            self.writer.writerow([self.count, tx_ns, rx_ns, latency_ms])
            self.count += 1
            

            if time.time() - self.last_print > 1.0:
                print(f"Rx {self.count} pkts | Last Lat: {latency_ms:.3f} ms")
                self.last_print = time.time()
                
        except Exception as e:

            pass

    async def on_state_change(self, meta: pubsub.DataSetMeta, state: ua.PubSubState):
        pass
    
    def close(self):
        print(f"\nChiusura file CSV. Totale pacchetti: {self.count}")
        self.f.close()

async def main():
    logging.basicConfig(level=logging.WARNING)
    print(f"--- SUBSCRIBER STRESS TEST RECORDER ---")
    print("In attesa di pacchetti pesanti... (Premi CTRL+C per fermare)")
    
    handler = OnDataReceived(FILENAME)
    metadata = create_meta_data()
    
    # Configurazione Reader
    reader = pubsub.ReaderGroup.new(
        name="ReaderGroup1",
        enable=True,
        reader=[
            pubsub.DataSetReader.new(
                ua.Variant(ua.UInt16(1)), 
                ua.UInt16(1),             
                ua.UInt32(100), # WriterID del Publisher           
                metadata,
                name="SimpleDataSetReader",
                subscribed=handler,
                enabled=True,
            )
        ],
    )
    
    connection = pubsub.PubSubConnection.udp_uadp(
        "Subscriber Connection",
        ua.UInt16(1),
        UdpSettings(Url=CFG.Url),
        reader_groups=[reader],
    )
    
    ps = pubsub.PubSub.new(connections=[connection])
    connection._app = ps

    try:
        async with ps:
            while True:
                await asyncio.sleep(1)
    finally:
        handler.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stop.")
