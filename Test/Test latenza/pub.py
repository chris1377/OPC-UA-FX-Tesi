import asyncio
import logging
from datetime import datetime
import time
import socket

from asyncua import pubsub, ua
from asyncua.pubsub.udp import UdpSettings
from asyncua.ua import Variant

# --- CONFIGURAZIONE ---
URL = "opc.udp://192.168.99.2:4840"
PDSNAME = "Simple"
PRIORITY = 6  # La priorità TSN

def create_published_dataset() -> pubsub.PublishedDataSet:
    dataset = pubsub.DataSetMeta.Create("Simple")
    dataset.add_scalar("Timestamp", ua.VariantType.Int64)
    dataset.add_scalar("Posizione", ua.VariantType.Double)
    return pubsub.PublishedDataSet.Create(PDSNAME, dataset)

def init_data_source(source: pubsub.PubSubDataSource):
    source.datasources["Simple"] = {}
    now = datetime.utcnow()
    source.datasources["Simple"]["Timestamp"] = ua.DataValue(ua.Variant(0, ua.VariantType.Int64), SourceTimestamp=now)
    source.datasources["Simple"]["Posizione"] = ua.DataValue(ua.Variant(0.0, ua.VariantType.Double), SourceTimestamp=now)

async def main():
    logging.basicConfig(level=logging.WARNING)
    print(f"--- AVVIO PUB EX (Standalone) su {URL} ---")

    # ==============================================================================
    # 🔥 HACK TSN V2: Patch sull'Istanza del Loop (PIÙ ROBUSTO) 🔥
    # ==============================================================================
    # Prendiamo il loop che sta girando ORA
    loop = asyncio.get_running_loop()
    
    # Salviamo il metodo originale (già legato a questo loop)
    original_create_datagram_endpoint = loop.create_datagram_endpoint

    # Definiamo il wrapper che inietta la priorità
    async def tsn_create_datagram_endpoint(*args, **kwargs):
        print(f"⚡ Intercettata creazione socket UDP! Applico Priorità {PRIORITY}...")
        # Chiamiamo il metodo originale per creare il socket
        transport, protocol = await original_create_datagram_endpoint(*args, **kwargs)
        
        # Estraiamo il socket "vero" dall'oggetto Transport di asyncio
        sock = transport.get_extra_info('socket')
        if sock:
            try:
                # 1. Priorità Socket (Linux Queue Discipline mapping)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_PRIORITY, PRIORITY)
                # 2. IP Type of Service (Opzionale, ma aiuta in alcuni switch)
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_TOS, 0xB8)
                print(f"✅ SUCCESSO: Socket configurato con SO_PRIORITY={PRIORITY}")
            except Exception as e:
                print(f"❌ ERRORE: Impossibile settare priorità: {e}")
        return transport, protocol

    # Sostituiamo il metodo del loop con il nostro wrapper
    loop.create_datagram_endpoint = tsn_create_datagram_endpoint
    # ==============================================================================

    # Configurazione OPC UA standard
    pds = create_published_dataset()
    source = pds.get_source()
    init_data_source(source)

    con = pubsub.PubSubConnection.udp_uadp(
        "Publisher Connection1 UDP UADP",
        ua.UInt16(1),
        UdpSettings(Url=URL),
        writer_groups=[
            pubsub.WriterGroup.new_uadp(
                name="WriterGroup1",
                writer_group_id=ua.UInt32(1),
                publishing_interval=1, 
                writer=[
                    pubsub.DataSetWriter.new_uadp(
                        name="Writer1",
                        dataset_writer_id=ua.UInt32(32),
                        dataset_name=PDSNAME,
                        datavalue=True,
                    )
                ],
            )
        ],
    )

    ps = pubsub.PubSub.new(connections=[con], datasets=[pds])
    con._app = ps # Fix per bug libreria

    print("Attivazione stack PubSub...")

    # Quando entriamo qui, asyncua chiamerà 'create_datagram_endpoint'
    # e scatterà il nostro HACK.
    async with ps:
        valore = 0
        while True:
            ts_now = time.time_ns()
            valore += 0.1
            if valore > 100: valore = 0

            source.datasources[PDSNAME]["Timestamp"] = ua.DataValue(
                ua.Variant(int(ts_now), ua.VariantType.Int64),
                SourceTimestamp=datetime.utcnow()
            )
            source.datasources[PDSNAME]["Posizione"] = ua.DataValue(
                ua.Variant(float(valore), ua.VariantType.Double),
                SourceTimestamp=datetime.utcnow()
            )

            print(f"Inviato: {ts_now} | Val: {valore:.1f}")
            await asyncio.sleep(0.001)

if __name__ == "__main__":
    try:
        # Nota: serve sudo per SO_PRIORITY
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stop.")