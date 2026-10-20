import threading
import time
import random
import SocketTCP


MODO_DEBUG = True
PROBABILIDAD_PERDIDA = 0.2  #probabilidad de perder paquetes

#printea solo si el debug mode esta activo
def printf(texto):
    if MODO_DEBUG:
        print(texto)

#clase para simular caidas de paquetes
class SocketLossy:
    def __init__(self, socket, afectado):
        self.socket = socket
        self.afectado = afectado

    def sendto(self, data, addr):
        if random.random() < PROBABILIDAD_PERDIDA:
            printf(f"[DEBUG PÉRDIDA] [{self.afectado}] -> Paquete descartado intencionalmente.")
            return len(data) 
        return self.socket.sendto(data, addr)

    def __getattr__(self, name):
        return getattr(self.socket, name)


def test_prueba(puerto, n_bytes):
    address = ('localhost', puerto)

    print(f" Pérdida simulada: {int(PROBABILIDAD_PERDIDA * 100)}% | Modo Debug: {MODO_DEBUG}")

    #creando mensaje
    mensaje_original = b"A" * n_bytes + b"B" * n_bytes
    resultado_servidor = []

    #servidor
    def servidor():
        serv_socket = SocketTCP.SocketTCP()
        serv_socket.socket = SocketLossy(serv_socket.socket, "SERVIDOR")
        serv_socket.bind(address)
        
        printf("[DEBUG] [SERVIDOR] Esperando conexión de handshake...")
        conn, addr = serv_socket.accept()
        printf("[DEBUG] [SERVIDOR] Handshake exitoso con el cliente.")

        # realizar 2 llamadas a recv 
        parte1 = conn.recv(n_bytes)
        printf(f"[DEBUG] [SERVIDOR] recv(1) recibió {len(parte1)} bytes.")
        
        parte2 = conn.recv(n_bytes)
        printf(f"[DEBUG] [SERVIDOR] recv(2) recibió {len(parte2)} bytes.")
        printf("[DEBUG] [SERVIDOR] Esperando cierre de conexión...")

        conn.recv_close()
        printf("[DEBUG] [SERVIDOR] Conexión cerrada con éxito.")
        
        resultado_servidor.append(parte1 + parte2)

    
    def cliente():
        time.sleep(0.3)
        cli_socket = SocketTCP.SocketTCP()
        cli_socket.socket = SocketLossy(cli_socket.socket, "CLIENTE")
        
        printf("[DEBUG] [CLIENTE] Conectando al servidor...")
        cli_socket.connect(address)
        printf("[DEBUG] [CLIENTE] Conectado. Enviando archivo...")

        cli_socket.send(mensaje_original)
        printf("[DEBUG] [CLIENTE] Archivo enviado completo. Cerrando conexión...")

        cli_socket.close()
        printf("[DEBUG] [CLIENTE] Conexión cerrada con éxito.")

    # ejecutar 
    t1 = threading.Thread(target=servidor)
    t2 = threading.Thread(target=cliente)
    t1.start(); t2.start()
    t1.join(); t2.join()

    # verificar 
    recibido = resultado_servidor[0] if resultado_servidor else b""
    if recibido == mensaje_original:
        print("\n-> RESULTADO: EXITOSO, llego el mensaje correctamente a pesar de las perdidas.")
    else:
        print("\n-> RESULTADO: FALLIDO.")


#prueba paso 7
def test_paso_7(puerto):
    address = ('localhost', puerto)
    print(f" Pérdida simulada: {int(PROBABILIDAD_PERDIDA * 100)}% | Modo Debug: {MODO_DEBUG}")

    msg1 = "Mensje de len=16".encode()
    msg2 = "Mensaje de largo 19".encode()
    msg3 = "Mensaje de largo 19".encode()
    resultado_servidor = []

    def servidor():
        serv_socket = SocketTCP.SocketTCP()
        serv_socket.socket = SocketLossy(serv_socket.socket, "SERVIDOR")
        serv_socket.bind(address)
        
        printf("[DEBUG] [SERVIDOR] Esperando conexión de handshake...")
        conn, addr = serv_socket.accept()
        printf("[DEBUG] [SERVIDOR] Handshake exitoso con el cliente.")

        #t1: buff_size = 16
        r1 = conn.recv(16)
        printf(f"[DEBUG] [SERVIDOR] Test 1 recibido: {r1}")

        #t2: buff_size = 19
        r2 = conn.recv(19)
        printf(f"[DEBUG] [SERVIDOR] Test 2 recibido: {r2}")

        #t3: buff_size = 14 (en 2 llamadas)
        r3_p1 = conn.recv(14)
        r3_p2 = conn.recv(14)
        r3 = r3_p1 + r3_p2
        printf(f"[DEBUG] [SERVIDOR] Test 3 recibido: {r3}")

        printf("[DEBUG] [SERVIDOR] Esperando cierre de conexión...")
        conn.recv_close()
        printf("[DEBUG] [SERVIDOR] Conexión cerrada con éxito.")

        resultado_servidor.extend([r1, r2, r3])

    def cliente():
        time.sleep(0.3)
        cli_socket = SocketTCP.SocketTCP()
        cli_socket.socket = SocketLossy(cli_socket.socket, "CLIENTE")
        
        printf("[DEBUG] [CLIENTE] Conectando al servidor...")
        cli_socket.connect(address)
        printf("[DEBUG] [CLIENTE] Conectado. Enviando mensajes...")

        cli_socket.send(msg1)
        cli_socket.send(msg2)
        cli_socket.send(msg3)
        printf("[DEBUG] [CLIENTE] Mensajes enviados. Cerrando conexión...")

        cli_socket.close()
        printf("[DEBUG] [CLIENTE] Conexión cerrada con éxito.")

    t1 = threading.Thread(target=servidor)
    t2 = threading.Thread(target=cliente)
    t1.start(); t2.start()
    t1.join(); t2.join()

    # verificar que los 3 mensajes hayan llegado bien
    if resultado_servidor == [msg1, msg2, msg3]:
        print("\n-> RESULTADO: EXITOSO, llego el mensaje correctamente a pesar de las perdidas.")
    else:
        print("\n-> RESULTADO: FALLIDO.")


if __name__ == "__main__":
    print("Iniciando: Prueba de buff_size < message_length")
    test_prueba(puerto=8500, n_bytes=25)

    print("\nIniciando: Prueba Paso 7")
    test_paso_7(puerto=8600)

