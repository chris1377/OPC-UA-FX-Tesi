import socket
import time
import statistics

# Metti qui l'IP del Lato A
TARGET_IP = "192.168.99.2" 
PORT = 5005
COUNT = 1000

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
# Opzionale: timeout
sock.settimeout(1.0)

latenze = []

print(f"🚀 Inizio test Ping-Pong UDP verso {TARGET_IP}...")

for i in range(COUNT):
    payload = b'x' * 64 # 64 byte di dati a caso
    
    t1 = time.time_ns()
    sock.sendto(payload, (TARGET_IP, PORT))
    
    try:
        data, addr = sock.recvfrom(1024)
        t2 = time.time_ns()
        
        # RTT / 2 = Latenza unidirezionale stimata
        rtt_ns = t2 - t1
        latenza_ms = (rtt_ns / 2) / 1_000_000
        latenze.append(latenza_ms)
        
        if i % 100 == 0:
            print(f"Pacchetto {i}: {latenza_ms:.3f} ms")
            
    except socket.timeout:
        print("Perso!")

avg = statistics.mean(latenze)
min_lat = min(latenze)
max_lat = max(latenze)

print(f"\n--- RISULTATI ({COUNT} pacchetti) ---")
print(f"✅ Latenza Media: {avg:.4f} ms")
print(f"⚡ Migliore: {min_lat:.4f} ms")
print(f"🐢 Peggiore: {max_lat:.4f} ms")
