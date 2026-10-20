import SocketTCP

address = ('localhost', 5000)

# CLIENT
client_socketTCP = SocketTCP.SocketTCP()
print("[Client] Conectando a", address)
client_socketTCP.connect(address)
print("[Client] Conectado! Enviando tests...")

# test 1
message = "Mensje de len=16".encode()
client_socketTCP.send(message)

# test 2
message = "Mensaje de largo 19".encode()
client_socketTCP.send(message)

# test 3
message = "Mensaje de largo 19".encode()
client_socketTCP.send(message)

print("[Client] Todos los mensajes fueron enviados!")

# test 4
client_socketTCP.close()
print("Test 4: Passed")

