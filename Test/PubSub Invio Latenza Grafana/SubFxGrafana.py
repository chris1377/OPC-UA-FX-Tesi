import asyncio
import logging
import time
from asyncua import ua, pubsub
from asyncua.pubsub.udp import UdpSettings
from dataclasses import dataclass, field
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import ASYNCHRONOUS

# --- CONFIGURAZIONE ---
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "my-super-secret-token-auth-token" 
INFLUX_ORG = "unibo"
INFLUX_BUCKET = "tesi_tsn"
UDP_URL = "opc.udp://0.0.0.0:4840" 
DECIMATION_FACTOR = 10 

@dataclass
class PubSubCFG:
    PublisherID: ua.Variant = field(default_factory=lambda: ua.Variant(ua.UInt16(1)))
    WriterGroupId: ua.UInt16 = field(default_factory=lambda: ua.UInt16(1))
    DataSetWriterId: ua.UInt16 = field(default_factory=lambda: ua.UInt16(100)) 

CFG = PubSubCFG()

def create_meta_data():
    dataset = pubsub.DataSetMeta.Create("100")
    dataset.add_scalar("JointPosition", ua.VariantType.Double)
    dataset.add_scalar("Temperature", ua.VariantType.Double)
    dataset.add_scalar("Vibration", ua.VariantType.Double)
    dataset.add_scalar("Voltage", ua.VariantType.Double)
    return dataset

class BatchingCallback:
    def success(self, conf: (str, str, str), data: str): pass
    def error(self, conf: (str, str, str), data: str, exception: Exception):
        print(f"[InfluxDB] Errore: {exception}")

class OnDataReceived:
    def __init__(self):
        print(f"--- Client InfluxDB FAST ({INFLUX_URL}) ---")
        self.client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        
        # BATCHING 
        write_options = WriteOptions(batch_size=10000,   
                                     flush_interval=5000, 
                                     jitter_interval=0, 
                                     retry_interval=5000)
        
        callback = BatchingCallback()
        self.write_api = self.client.write_api(write_options=ASYNCHRONOUS, write_options_config=write_options, success_callback=callback.success, error_callback=callback.error, retry_callback=callback.error)
        
        self.count = 0
        self.last_print = time.time()

    async def on_dataset_received(self, _meta: pubsub.DataSetMeta, fields: list[pubsub.DataSetValue]):

        rx_ns = time.time_ns()
        

        self.count += 1
        if self.count % DECIMATION_FACTOR != 0:
            return 

        def get_val(obj):
            while hasattr(obj, 'Value'): obj = obj.Value
            return obj

        try:

            ts_source_dt = None
            if hasattr(fields[0], 'Value') and hasattr(fields[0].Value, 'SourceTimestamp'): 
                 ts_source_dt = fields[0].Value.SourceTimestamp
            
            if ts_source_dt:
                # Conversione datetime -> nanosecondi epoch
                tx_ns = int(ts_source_dt.timestamp() * 1_000_000_000)
            else:
                tx_ns = rx_ns


            latency_ms = (rx_ns - tx_ns) / 1_000_000.0
            if latency_ms < 0: latency_ms = 0.0

            pos = float(get_val(fields[0]))
            temp = float(get_val(fields[1]))
            vib = float(get_val(fields[2]))
            volt = float(get_val(fields[3]))
            
            p = Point("misurazioni_fx") \
                .tag("source", "Advantech_Device") \
                .field("JointPosition", pos) \
                .field("Temperature", temp) \
                .field("Vibration", vib) \
                .field("Voltage", volt) \
                .field("Latency_ms", latency_ms) \
                .time(tx_ns) 
            
            self.write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=p)

            if time.time() - self.last_print > 2.0:
                print(f"Pkts: {self.count} | Latency Instant: {latency_ms:.3f} ms")
                self.last_print = time.time()

        except Exception as e:
            print(f"Errore pkt: {e}")
            pass

    def close(self):
        self.write_api.close()
        self.client.close()

if __name__ == "__main__":
    handler_ref = None
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        handler_ref = OnDataReceived()
        metadata = create_meta_data()

        logging.basicConfig(level=logging.WARNING)
        print(f"--- SUBSCRIBER (Decimation: 1/{DECIMATION_FACTOR}) ---")

        reader_group = pubsub.ReaderGroup.new(
            name="ReaderGroup1", enable=True,
            reader=[pubsub.DataSetReader.new(CFG.PublisherID, CFG.WriterGroupId, CFG.DataSetWriterId, metadata, name="FXReader", subscribed=handler_ref, enabled=True)]
        )

        connection = pubsub.PubSubConnection.udp_uadp("Subscriber Connection", ua.UInt16(1), UdpSettings(Url=UDP_URL), reader_groups=[reader_group])

        ps = pubsub.PubSub.new(connections=[connection])
        connection._app = ps

        async def runner():
             print("In attesa di pacchetti...")
             async with ps:
                while True: await asyncio.sleep(1)

        loop.run_until_complete(runner())

    except KeyboardInterrupt:
        print("\nStop.")
    finally:
        if handler_ref: handler_ref.close()
