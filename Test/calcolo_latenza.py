import struct
import pandas as pd
import os

# --- CONFIGURAZIONE ---
LOADS = [0, 200, 500, 900]     # I livelli di traffico (Mbps)
MODES = ['tsn', 'notsn']       # Le due modalità
OUTPUT_PREFIX = "dati_"        # Prefisso per i file CSV di uscita

def read_pcap(filename):
    """Legge PCAP e restituisce lista (timestamp_ns, payload_signature)"""
    packets = []
    try:
        with open(filename, 'rb') as f:
            # Header
            gh = f.read(24)
            if len(gh) < 24: return []
            magic = struct.unpack('I', gh[:4])[0]
            
            time_scale = 1000 if magic in [0xa1b2c3d4, 0xd4c3b2a1] else 1
            endian = '<'

            while True:
                ph = f.read(16)
                if len(ph) < 16: break
                ts_sec, ts_frac, incl_len, orig_len = struct.unpack(endian + 'IIII', ph)
                ts_ns = (ts_sec * 1_000_000_000) + (ts_frac * time_scale)
                
                data = f.read(incl_len)
                
                # Parsing Header (Eth+IP+UDP)
                offset = 14
                if len(data) > 14 and data[12:14] == b'\x81\x00': offset = 18 # VLAN
                if offset + 20 > len(data): continue
                
                if data[offset] >> 4 != 4: continue 
                ihl = (data[offset] & 0x0F) * 4
                protocol = data[offset + 9]
                offset += ihl
                
                if protocol == 17: # UDP
                    offset += 8
                    if offset < len(data):
                        # Usa i primi 32 byte del payload come firma univoca
                        payload = data[offset:]
                        sig = payload[:min(32, len(payload))]
                        packets.append((ts_ns, sig))
    except Exception as e:
        print(f"Errore su {filename}: {e}")
    return packets

def process_scenario(load, mode):
    tx_file = f"tx_{mode}_{load}.pcap"
    rx_file = f"rx_{mode}_{load}.pcap"
    out_csv = f"{OUTPUT_PREFIX}{mode}_{load}.csv"
    
    print(f"--- Elaborazione: {mode.upper()} @ {load} Mbps ---")
    if not os.path.exists(tx_file) or not os.path.exists(rx_file):
        print(f" File mancanti ({tx_file} o {rx_file}). Salto.")
        return

    # 1. Carica TX
    tx_pkts = read_pcap(tx_file)
    tx_dict = {sig: ts for ts, sig in tx_pkts}
    
    # 2. Carica RX e calcola latenza
    rx_pkts = read_pcap(rx_file)
    results = []
    
    for rx_ts, sig in rx_pkts:
        if sig in tx_dict:
            tx_ts = tx_dict[sig]
            latency = (rx_ts - tx_ts) / 1e6 # Converti in ms
            if latency > 0:
                results.append({'TX_Time_ns': tx_ts, 'Latency_ms': latency})
    
    # 3. Salva
    pd.DataFrame(results).to_csv(out_csv, index=False)
    print(f"Salvato: {out_csv} ({len(results)} pcchetti)")


print("INIZIO ESTRAZIONE DATI...")
for mode in MODES:
    for load in LOADS:
        process_scenario(load, mode)
print("\nEstrazione Completata.")