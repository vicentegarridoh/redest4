import threading
import time
import random
import SocketTCP

MODO_DEBUG = True
PROBABILIDAD_PERDIDA = 0.2  #probabilidad de perder paquetes

def printf(texto):
    if MODO_DEBUG:
        print(texto)

#clase para simular caidas de paquetes
class SocketLossy:
    def __init__(self, socket, afectado, tasa_perdida=PROBABILIDAD_PERDIDA, seed=None):
        self.socket = socket
        self.afectado = afectado
        self.tasa_perdida = tasa_perdida
        self.rng = random.Random(seed)

    def sendto(self, data, addr):
        flags = data[0] if len(data) > 0 else 0
        is_syn = bool(flags & SocketTCP.SocketTCP.SYN)
        is_fin = bool(flags & SocketTCP.SocketTCP.FIN)

        if not is_syn and not is_fin and self.rng.random() < self.tasa_perdida:
            printf(f"[DEBUG PÉRDIDA] [{self.afectado}] -> Paquete descartado intencionalmente.")
            return len(data)
        return self.socket.sendto(data, addr)

    def __getattr__(self, name):
        return getattr(self.socket, name)


def test_sin_perdidas(n_bytes=50):
    puerto = random.randint(30000, 40000)
    address = ('localhost', puerto)
    print("PRUEBA 1: Integridad con Control de Congestión (SIN PÉRDIDAS)")

    #creando mensaje
    mensaje_original = b"A" * n_bytes + b"B" * n_bytes
    resultado_servidor = []

    #servidor
    def servidor():
        serv_socket = SocketTCP.SocketTCP()
        serv_socket.bind(address)

        printf("[DEBUG] [SERVIDOR] Esperando conexión...")
        conn, addr = serv_socket.accept()
        printf("[DEBUG] [SERVIDOR] Conectado. Recibiendo datos con Go-Back-N...")

        recibido = bytearray()
        while len(recibido) < len(mensaje_original):
            chunk = conn.recv(16, mode="go_back_n")
            if not chunk:
                break
            recibido.extend(chunk)

        printf(f"[DEBUG] [SERVIDOR] Recibidos {len(recibido)} bytes en total.")
        resultado_servidor.append(bytes(recibido))
        try:
            conn.recv_close()
        except Exception:
            pass
        finally:
            serv_socket.socket.close()

    def cliente():
        time.sleep(0.3)
        cli_socket = SocketTCP.SocketTCP()
        cli_socket.set_congestion_control(True)
        cli_socket.set_debug(MODO_DEBUG)

        printf("[DEBUG] [CLIENTE] Conectando al servidor...")
        cli_socket.connect(address)
        printf("[DEBUG] [CLIENTE] Enviando mensaje con Go-Back-N y Control de Congestión...")

        cli_socket.send(mensaje_original, mode="go_back_n")
        printf("[DEBUG] [CLIENTE] Mensaje enviado completo. Cerrando conexión...")

        try:
            cli_socket.close()
        except Exception:
            pass

    t1 = threading.Thread(target=servidor, daemon=True)
    t2 = threading.Thread(target=cliente, daemon=True)
    t1.start()
    t2.start()
    t1.join(15)
    t2.join(15)

    recibido = resultado_servidor[0] if resultado_servidor else b""
    if recibido == mensaje_original:
        print(f"\n-> RESULTADO: EXITOSO, llegaron los {len(recibido)} bytes de manera íntegra.\n")
    else:
        print(f"\n-> RESULTADO: FALLIDO. Esperados {len(mensaje_original)}, recibidos {len(recibido)}.\n")


def test_con_perdidas(n_bytes=50, tasa_perdida=0.20):
    puerto = random.randint(40001, 50000)
    address = ('localhost', puerto)
    print(f"PRUEBA 2: Integridad con Control de Congestión (CON PÉRDIDA: {int(tasa_perdida * 100)}%)")

    mensaje_original = b"A" * n_bytes + b"B" * n_bytes
    resultado_servidor = []

    def servidor():
        serv_socket = SocketTCP.SocketTCP()
        serv_socket.bind(address)

        printf("[DEBUG] [SERVIDOR] Esperando conexión...")
        conn, addr = serv_socket.accept()
        conn.socket = SocketLossy(conn.socket, "SERVIDOR_CONN", tasa_perdida=tasa_perdida, seed=77)
        conn.socket.settimeout(0.5)
        printf("[DEBUG] [SERVIDOR] Conectado. Recibiendo datos con Go-Back-N...")

        recibido = bytearray()
        while len(recibido) < len(mensaje_original):
            chunk = conn.recv(16, mode="go_back_n")
            if not chunk:
                break
            recibido.extend(chunk)

        printf(f"[DEBUG] [SERVIDOR] Recibidos {len(recibido)} bytes en total a pesar de las pérdidas.")
        resultado_servidor.append(bytes(recibido))
        try:
            conn.recv_close()
        except Exception:
            pass
        finally:
            serv_socket.socket.close()

    def cliente():
        time.sleep(0.3)
        cli_socket = SocketTCP.SocketTCP()
        cli_socket.set_congestion_control(True)
        cli_socket.set_debug(MODO_DEBUG)
        cli_socket.socket = SocketLossy(cli_socket.socket, "CLIENTE", tasa_perdida=tasa_perdida, seed=88)
        cli_socket.socket.settimeout(0.5)

        printf("[DEBUG] [CLIENTE] Conectando al servidor...")
        cli_socket.connect(address)
        printf("[DEBUG] [CLIENTE] Enviando mensaje con Go-Back-N y Control de Congestión...")

        cli_socket.send(mensaje_original, mode="go_back_n")
        printf("[DEBUG] [CLIENTE] Mensaje enviado completo. Cerrando conexión...")

        try:
            cli_socket.close()
        except Exception:
            pass

    t1 = threading.Thread(target=servidor, daemon=True)
    t2 = threading.Thread(target=cliente, daemon=True)
    t1.start()
    t2.start()
    t1.join(25)
    t2.join(25)

    recibido = resultado_servidor[0] if resultado_servidor else b""
    if recibido == mensaje_original:
        print(f"\n-> RESULTADO: EXITOSO, se recuperaron todas las pérdidas y los {len(recibido)} bytes llegaron íntegros.\n")
    else:
        print(f"\n-> RESULTADO: FALLIDO. Esperados {len(mensaje_original)}, recibidos {len(recibido)}.\n")


if __name__ == "__main__":
    test_sin_perdidas(n_bytes=50)
    test_con_perdidas(n_bytes=50, tasa_perdida=0.20)
