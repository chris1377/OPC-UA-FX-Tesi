"""
DESCRIZIONE:
    Simulatore OPC UA PubSub (Publisher).
    Questo script svolge due funzioni principali:
    1. OPC UA SERVER (TCP): Espone le variabili e permette a un client (es. UaExpert)
       di modificare la velocità di simulazione ("Speed_Input").
    2. OPC UA PUBLISHER (UDP): Invia i dati di telemetria (Posizione, Temperatura, Vibrazioni)
       via UDP usando il protocollo UADP.
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

URL_DEST = "opc.udp://192.168.99.2:4840" # IP del subscriber nella VLAN
PRIORITY = 6

WRITER_ID = 100 # Deve combaciare con CFG.DataSetWriterId
DS_NAME = str(WRITER_ID) 

async def main():
    """
    Funzione principale che inizializza il Server e il Publisher,
    e gestisce il loop di simulazione.
    """
    logging.basicConfig(level=logging.WARNING)
    print(f"--- AVVIO SIMULATORE FX SPEED CONTROL (TSN p{PRIORITY}) ---")

    # ==========================================================================
    # 1. SERVER 
    # ==========================================================================
    # Il server permette di monitorare lo stato e ricevere comandi esterni.
    server = Server()
    await server.init()
    server.set_endpoint("opc.tcp://0.0.0.0:4840") 

    # Creazione dello spazio dei nomi e della struttura oggetti
    idx = await server.register_namespace("http://opcfoundation.org/UA/FX/AC/")
    objects = server.nodes.objects
    
    device_folder = await objects.add_folder(idx, "DeviceSet")
    my_device = await device_folder.add_object(idx, "MotionDevice_1")
    proc_data_folder = await my_device.add_object(idx, "ProcessData")

    # --- DEFINIZIONE VARIABILI DATI 
    v_pos = await proc_data_folder.add_variable(idx, "JointPosition", 0.0, ua.VariantType.Double)
    v_temp = await proc_data_folder.add_variable(idx, "Temperature", 25.0, ua.VariantType.Double)
    v_vib = await proc_data_folder.add_variable(idx, "Vibration", 0.0, ua.VariantType.Double)
    v_volt = await proc_data_folder.add_variable(idx, "Voltage", 24.0, ua.VariantType.Double)

    # --- VARIABILE DI CONTROLLO (SCRIVIBILE da UaExpert) ---
    v_speed = await proc_data_folder.add_variable(idx, "Speed_Input", 1.0, ua.VariantType.Double)
    await v_speed.set_writable()  # Permette a UaExpert di scrivere

    # ==========================================================================
    # 2. PUBSUB (UDP)
    # ==========================================================================
    
    # Definizione della struttura dei dati (MetaData) che verrà spedita
    ds_meta = pubsub.DataSetMeta.Create(DS_NAME)
    ds_meta.add_scalar("JointPosition", ua.VariantType.Double)
    ds_meta.add_scalar("Temperature", ua.VariantType.Double)
    ds_meta.add_scalar("Vibration", ua.VariantType.Double)
    ds_meta.add_scalar("Voltage", ua.VariantType.Double)
    
    # Creazione del DataSet pubblicato
    pds = pubsub.PublishedDataSet.Create(DS_NAME, ds_meta)
    
    # Inizializzazione della sorgente dati interna al PubSub
    source = pds.get_source()
    source.datasources[DS_NAME] = {}
    now = datetime.now(timezone.utc)
    for name in ["JointPosition", "Temperature", "Vibration", "Voltage"]:
        source.datasources[DS_NAME][name] = ua.DataValue(ua.Variant(0.0, ua.VariantType.Double), SourceTimestamp=now)

    # Configurazione del Writer
    my_writer = pubsub.DataSetWriter.new_uadp(
        name="Writer_1", dataset_writer_id=WRITER_ID, dataset_name=DS_NAME, datavalue=True # datavalue=True include timestamp e status nel payload
    )
    my_writer.DataSetName = DS_NAME

    # Configurazione del Gruppo (Frequenza di pubblicazione)
    my_writer_group = pubsub.WriterGroup.new_uadp(
        name="WG_Main", writer_group_id=ua.UInt32(1), publishing_interval=1, writer=[my_writer] # Id writer group ua.UInt32(1)
    )

    # Connessione UDP
    con = pubsub.PubSubConnection.udp_uadp(
        "TSN_Connection", ua.UInt16(1) , UdpSettings(Url=URL_DEST), writer_groups=[my_writer_group] # ua.UInt16(1) id del dispositivo
    )

    # Attivazione PubSub
    ps = pubsub.PubSub.new(connections=[con], datasets=[pds])
    con._app = ps 

    print("--- INIZIO SIMULAZIONE ---")
    

    # ==========================================================================
    # 3. IL LOOP
    # ==========================================================================
    async with server:
        async with ps:
            start_time = time.time()
            
            # Variabili per la fisica
            current_phase = 0.0
            last_time = start_time

            while True:
                now_t = time.time()
                dt = now_t - last_time
                last_time = now_t
                now_ts = datetime.now(timezone.utc)

                # --- A. LEGGIAMO IL COMANDO DA UAEXPERT ---
                # Leggiamo la velocità desiderata impostata dall'utente via TCP
                speed_val = await v_speed.read_value()
                
                # Protezione: evita velocità nulle o negative
                if speed_val < 0.1: speed_val = 0.1

                # --- B. CALCOLO FISICA ---
                # Aggiorniamo la fase basandoci sulla velocità attuale
                # nuova_fase = vecchia_fase + (velocità * delta_tempo)
                current_phase += speed_val * dt
                
                # Calcoliamo il seno usando la fase accumulata
                val_pos = 100.0 * math.sin(current_phase)

                # Temperatura (Dente di sega)
                val_temp = 25.0 + (now_t % 60) * 0.5

                # Vibrazione (Rumore casuale amplificato dalla velocità)
                val_vib = random.uniform(0, 0.5) * speed_val

                # Voltaggio (Costante)
                val_volt = 24.0

                # --- C. INVIO DATI (PubSub UDP) ---
                ds_data = source.datasources[DS_NAME]
                ds_data["JointPosition"] = ua.DataValue(ua.Variant(val_pos, ua.VariantType.Double), SourceTimestamp=now_ts)
                ds_data["Temperature"] = ua.DataValue(ua.Variant(val_temp, ua.VariantType.Double), SourceTimestamp=now_ts)
                ds_data["Vibration"] = ua.DataValue(ua.Variant(val_vib, ua.VariantType.Double), SourceTimestamp=now_ts)
                ds_data["Voltage"] = ua.DataValue(ua.Variant(val_volt, ua.VariantType.Double), SourceTimestamp=now_ts)

                # --- D. AGGIORNAMENTO DATI TCP ---
                # Aggiorniamo i nodi del server per permettere la visualizzazione su UaExpert
                await v_pos.write_value(val_pos)
                await v_temp.write_value(val_temp)
                await v_vib.write_value(val_vib)
                # Non aggiorno v_speed

                # Log
                if int(now_t * 10) % 20 == 0:
                    print(f"Speed: {speed_val:.1f} | Pos: {val_pos:.2f}")

                await asyncio.sleep(0.001) # 1ms ciclo

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stop.")
        
