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

async def main():
    logging.basicConfig(level=logging.WARNING)
    print(f"--- AVVIO PUBLISHER  (TSN p{PRIORITY}) ---")


    # ==========================================================================
    # 1. SERVER OPC UA 
    # ==========================================================================
    server = Server()
    await server.init()
    server.set_endpoint("opc.tcp://0.0.0.0:4840") 

    idx = await server.register_namespace("http://opcfoundation.org/UA/FX/AC/")
    objects = server.nodes.objects
    device_folder = await objects.add_folder(idx, "DeviceSet")
    my_device = await device_folder.add_object(idx, "MotionDevice_1")
    proc_data_folder = await my_device.add_object(idx, "ProcessData")
    v_pos = await proc_data_folder.add_variable(idx, "JointPosition", 0.0, ua.VariantType.Double)
    v_temp = await proc_data_folder.add_variable(idx, "Temperature", 25.0, ua.VariantType.Double)
    v_vib = await proc_data_folder.add_variable(idx, "Vibration", 0.0, ua.VariantType.Double)
    v_volt = await proc_data_folder.add_variable(idx, "Voltage", 24.0, ua.VariantType.Double)

    v_speed = await proc_data_folder.add_variable(idx, "Speed_Input", 1.0, ua.VariantType.Double)
    await v_speed.set_writable() 

    # ==========================================================================
    # 2. PUBSUB
    # ==========================================================================
    ds_meta = pubsub.DataSetMeta.Create(DS_NAME)
    ds_meta.add_scalar("JointPosition", ua.VariantType.Double)
    ds_meta.add_scalar("Temperature", ua.VariantType.Double)
    ds_meta.add_scalar("Vibration", ua.VariantType.Double)
    ds_meta.add_scalar("Voltage", ua.VariantType.Double)
    
    pds = pubsub.PublishedDataSet.Create(DS_NAME, ds_meta)
    source = pds.get_source()
    source.datasources[DS_NAME] = {}

    now = datetime.now(timezone.utc)
    for name in ["JointPosition", "Temperature", "Vibration", "Voltage"]:
        source.datasources[DS_NAME][name] = ua.DataValue(ua.Variant(0.0, ua.VariantType.Double), SourceTimestamp=now)

    my_writer = pubsub.DataSetWriter.new_uadp(
        name="Writer_1", dataset_writer_id=WRITER_ID, dataset_name=DS_NAME, datavalue=True
    )
    
    my_writer_group = pubsub.WriterGroup.new_uadp(
        name="WG_Main", writer_group_id=ua.UInt32(1), publishing_interval=1.0, writer=[my_writer]
    )

    con = pubsub.PubSubConnection.udp_uadp(
        "TSN_Connection", ua.UInt16(1), UdpSettings(Url=URL_DEST), writer_groups=[my_writer_group]
    )

    ps = pubsub.PubSub.new(connections=[con], datasets=[pds])
    con._app = ps 

    print("--- LOOP REAL-TIME AVVIATO ---")
    print(" Modifica 'Speed_Input' da UaExpert per controllare la velocità.")

    # ==========================================================================
    # 3. LOOP 
    # ==========================================================================
    async with server:
        async with ps:
            current_phase = 0.0
            last_time = time.perf_counter()
            
            #
            TARGET_INTERVAL = 0.001  # 1ms
            UA_EXPERT_RATE = 100     # Aggiorna server TCP ogni 100 cicli (100ms)
            cycle_counter = 0
            
           
            speed_val = 1.0

            while True:
                now_t = time.perf_counter()
                
              
                dt = now_t - last_time
                if dt <= 0: dt = 0.000001
                last_time = now_t
                
                if cycle_counter % UA_EXPERT_RATE == 0:
                     try:
                        val = await v_speed.read_value()
                        if val > 0: speed_val = val
                     except: pass


                current_phase += speed_val * dt
                val_pos = 100.0 * math.sin(current_phase)
                val_temp = 25.0 + (now_t % 60) * 0.1
                val_vib = random.uniform(0, 0.5) * speed_val
                val_volt = 24.0


                now_ts = datetime.now(timezone.utc)
                ds_data = source.datasources[DS_NAME]
                ds_data["JointPosition"] = ua.DataValue(ua.Variant(val_pos, ua.VariantType.Double), SourceTimestamp=now_ts)
                ds_data["Temperature"] = ua.DataValue(ua.Variant(val_temp, ua.VariantType.Double), SourceTimestamp=now_ts)
                ds_data["Vibration"] = ua.DataValue(ua.Variant(val_vib, ua.VariantType.Double), SourceTimestamp=now_ts)
                ds_data["Voltage"] = ua.DataValue(ua.Variant(val_volt, ua.VariantType.Double), SourceTimestamp=now_ts)

                if cycle_counter % UA_EXPERT_RATE == 0:
                    # Aggiorna UaExpert 
                    await v_pos.write_value(val_pos)
                    await v_temp.write_value(val_temp)
                    await v_vib.write_value(val_vib)
                    
              

                cycle_counter += 1

              
                elapsed = time.perf_counter() - now_t
                sleep_time = TARGET_INTERVAL - elapsed
                
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                else:
                    await asyncio.sleep(0)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stop.")
