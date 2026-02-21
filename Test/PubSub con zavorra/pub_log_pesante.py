"""
DESCRIZIONE:
    Simulatore OPC UA PubSub (Publisher) - STRESS TEST MODE.
    
    Caratteristiche:
    1. PAYLOAD PESANTE: Aggiunge una stringa di padding (Zavorra) per ingrossare il pacchetto.
    2. ALTA FREQUENZA: Ciclo a 1ms (1000 Hz).
    3. TIMESTAMP TX: Mantenuto per calcolare la latenza precisa.
"""

import asyncio
import logging
import time
import math
import random
import socket
from datetime import datetime, timezone

from asyncua import pubsub, ua, Server
from asyncua.pubsub.udp import UdpSettings

# --- CONFIGURAZIONE ---
URL_DEST = "opc.udp://192.168.99.2:4840" 
PRIORITY = 6
WRITER_ID = 100
DS_NAME = str(WRITER_ID)

# Aggiungo 1024 caratteri (1KB) di dati inutili per appesantire il pacchetto
PADDING_SIZE = 1024 
PADDING_DATA = "X" * PADDING_SIZE 

async def main():
    logging.basicConfig(level=logging.WARNING)
    print(f"--- AVVIO SIMULATORE STRESS TEST (TSN p{PRIORITY}) ---")
    print(f"--- Payload Extra: {PADDING_SIZE} bytes | Frequenza: 1ms ---")

    # ==========================================================================
    # 1. SERVER (TCP)
    # ==========================================================================
    server = Server()
    await server.init()
    server.set_endpoint("opc.tcp://0.0.0.0:4840") 

    idx = await server.register_namespace("http://opcfoundation.org/UA/FX/AC/")
    objects = server.nodes.objects
    
    # Variabili TCP (Standard)
    device_folder = await objects.add_folder(idx, "DeviceSet")
    my_device = await device_folder.add_object(idx, "MotionDevice_1")
    proc_data_folder = await my_device.add_object(idx, "ProcessData")
    v_pos = await proc_data_folder.add_variable(idx, "JointPosition", 0.0, ua.VariantType.Double)

    # ==========================================================================
    # 2. PUBSUB (UDP)
    # ==========================================================================
    ds_meta = pubsub.DataSetMeta.Create(DS_NAME)
    
    # CAMPO 0: TimestampTX (Int64) - SEMPRE IL PRIMO!
    ds_meta.add_scalar("TimestampTX", ua.VariantType.Int64)
    
    # Dati Utili
    ds_meta.add_scalar("JointPosition", ua.VariantType.Double)
    

    ds_meta.add_scalar("Padding", ua.VariantType.String)
    
    pds = pubsub.PublishedDataSet.Create(DS_NAME, ds_meta)
    
    source = pds.get_source()
    source.datasources[DS_NAME] = {}
    now = datetime.now(timezone.utc)
    
    # Inizializzazione
    source.datasources[DS_NAME]["TimestampTX"] = ua.DataValue(ua.Variant(0, ua.VariantType.Int64), SourceTimestamp=now)
    source.datasources[DS_NAME]["JointPosition"] = ua.DataValue(ua.Variant(0.0, ua.VariantType.Double), SourceTimestamp=now)
    source.datasources[DS_NAME]["Padding"] = ua.DataValue(ua.Variant("", ua.VariantType.String), SourceTimestamp=now)

    # Configurazione Writer
    my_writer = pubsub.DataSetWriter.new_uadp(
        name="Writer_1", dataset_writer_id=WRITER_ID, dataset_name=DS_NAME, datavalue=True 
    )
    my_writer.DataSetName = DS_NAME

    # Configurazione WriterGroup - Interval 1ms
    my_writer_group = pubsub.WriterGroup.new_uadp(
        name="WG_Main", writer_group_id=ua.UInt32(1), publishing_interval=1, writer=[my_writer] 
    )

    con = pubsub.PubSubConnection.udp_uadp(
        "TSN_Connection", ua.UInt16(1) , UdpSettings(Url=URL_DEST), writer_groups=[my_writer_group] 
    )

    ps = pubsub.PubSub.new(connections=[con], datasets=[pds])
    con._app = ps 

    print("--- INIZIO ---")

    async with server:
        async with ps:
            start_time = time.time()
            while True:
                now_ts = datetime.now(timezone.utc)

                # --- PREPARAZIONE DATI ---
                
                # 1. Timestamp TX (Nanosecondi)
                tx_ns = time.time_ns()
                ds_data = source.datasources[DS_NAME]
                ds_data["TimestampTX"] = ua.DataValue(ua.Variant(tx_ns, ua.VariantType.Int64), SourceTimestamp=now_ts)
                
                # 2. Dati finti
                val_pos = math.sin(time.time()) * 100
                ds_data["JointPosition"] = ua.DataValue(ua.Variant(val_pos, ua.VariantType.Double), SourceTimestamp=now_ts)
                
                # 3. ZAVORRA
                ds_data["Padding"] = ua.DataValue(ua.Variant(PADDING_DATA, ua.VariantType.String), SourceTimestamp=now_ts)

                # Loop veloce (1ms)
                await asyncio.sleep(0.001)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stop.")
