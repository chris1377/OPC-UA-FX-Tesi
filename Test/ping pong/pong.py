import socket

# Ascolta su tutte le interfacce, porta 5005
IP = "0.0.0.0"
PORT = 5005

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((IP, PORT))

print(f"🏓 MIRROR UDP pronto su {PORT}. Rimbalzo i pacchetti...")

while True:
    data, addr = sock.recvfrom(1024)
    # Appena riceve, rimanda indietro IMMEDIATAMENTE
    sock.sendto(data, addr)
