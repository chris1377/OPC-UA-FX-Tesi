import asyncio
import logging
import time
from asyncua import ua, pubsub
from asyncua.pubsub.udp import UdpSettings
from dataclasses import dataclass, field

# --- CONFIGURAZIONE ---
@dataclass
class PubSubCFG:
    # Ascolta su tutte le interfacce
    Url: str = "opc.udp://0.0.0.0:4840"

CFG = PubSubCFG()

# Classe per calcolare il Jitter
class JitterStats:
    def __init__(self):
        self.last_arrival = 0
    
    def update(self, now_ns):
        if self.last_arrival == 0:
            self.last_arrival = now_ns
            return None
        
        # Calcola tempo passato dall'ultimo pacchetto
        delta_ms = (now_ns - self.last_arrival) / 1_000_000.0
        self.last_arrival = now_ns
        return delta_ms

def create_meta_data():
    dataset = pubsub.DataSetMeta.Create("Simple")
    dataset.add_scalar("Timestamp", ua.VariantType.Int64)
    dataset.add_scalar("Posizione", ua.VariantType.Double)
    return dataset

class OnDataReceived:
    def __init__(self):
        self.stats = JitterStats()

    # --- FIX: Aggiunto 'async' qui ---
    async def on_dataset_received(self, _meta: pubsub.DataSetMeta, fields: list[pubsub.DataSetValue]):
        now = time.time_ns()
        
        # Calcolo Delta
        delta = self.stats.update(now)
        
        if delta is not None:
            # Estrazione valore (giusto per vederlo)
            val_obj = fields[1]
            while hasattr(val_obj, 'Value'): val_obj = val_obj.Value
            valore = val_obj
            
            # Stampa
            # Se TSN funziona bene, 'Inter-Arrivo' deve stare fisso vicino a 10.000 ms
            print(f"📦 Val: {valore:.1f} | Inter-Arrivo: {delta:.3f} ms")

    # --- FIX: Aggiunto metodo mancante per chiusura pulita ---
    async def on_state_change(self, meta: pubsub.DataSetMeta, state: ua.PubSubState):
        pass

async def main():
    logging.basicConfig(level=logging.WARNING)
    print(f"--- SUBSCRIBER JITTER ANALYZER (Atteso: 10ms) ---")
    
    handler = OnDataReceived()
    metadata = create_meta_data()
    
    reader = pubsub.ReaderGroup.new(
        name="ReaderGroup1",
        enable=True,
        reader=[
            pubsub.DataSetReader.new(
                ua.Variant(ua.UInt16(1)), # PublisherID (Deve coincidere col Pub)
                ua.UInt16(1),             # WriterGroupID
                ua.UInt32(32),            # DataSetWriterID (Deve coincidere col Pub: 32)
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

    async with ps:
        while True:
            await asyncio.sleep(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stop.")
