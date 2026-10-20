import threading
import time
import random
import SocketTCP

TIMEOUT = 0.5                # Timeout de 0.5 segundos según la guía
PROBABILIDAD_PERDIDA = 0.20  # 20% de probabilidad de pérdida


class LossySocket:
    def __init__(self, real_sock, drop_prob=0.20, seed=42):
        self.real_sock = real_sock
        self.drop_prob = drop_prob
        self.rng = random.Random(seed)
        self.sent_count = 0
        self.drop_count = 0

    def sendto(self, data, address):
        self.sent_count += 1
        flags = data[0] if len(data) > 0 else 0
        is_syn = bool(flags & SocketTCP.SocketTCP.SYN)
        is_fin = bool(flags & SocketTCP.SocketTCP.FIN)

        # Solo dropear paquetes de datos (no flags de control de conexion SYN/FIN)
        if not is_syn and not is_fin and self.rng.random() < self.drop_prob:
            self.drop_count += 1
            return len(data)
        return self.real_sock.sendto(data, address)

    def recvfrom(self, bufsize):
        return self.real_sock.recvfrom(bufsize)

    def settimeout(self, t):
        self.real_sock.settimeout(t)

    def bind(self, addr):
        self.real_sock.bind(addr)

    def close(self):
        self.real_sock.close()


def ejecutar_envio(mensaje, con_cc=True, tasa_perdida=0.20, seed=None):
    server = SocketTCP.SocketTCP()
    port = random.randint(35000, 55000)
    server.bind(('localhost', port))

    recibido = bytearray()
    sent_segments = [0]
    elapsed = [0.0]
    client_done = threading.Event()

    def run_server():
        conn = None
        try:
            conn, addr = server.accept()
            conn.socket = LossySocket(conn.socket, drop_prob=tasa_perdida, seed=(seed + 1 if seed is not None else None))
            conn.socket.settimeout(0.1)
            while len(recibido) < len(mensaje):
                chunk = conn.recv(16, mode="go_back_n")
                if not chunk:
                    break
                recibido.extend(chunk)

            # Mantener al servidor respondiendo ACKs por si el último ACK de datos se perdió
            while not client_done.is_set():
                try:
                    msg, addr_cli = conn.socket.recvfrom(1024)
                    ack = conn.create_segment({'ack': True, 'seq': conn.SEQ})
                    conn.socket.sendto(ack, addr_cli)
                except Exception:
                    pass
        finally:
            if conn is not None:
                try:
                    conn.socket.close()
                except Exception:
                    pass
            try:
                server.socket.close()
            except Exception:
                pass

    def run_client():
        client = None
        try:
            time.sleep(0.05)
            client = SocketTCP.SocketTCP()
            client.set_congestion_control(con_cc)
            client.connect(('localhost', port))
            client.socket = LossySocket(client.socket, drop_prob=tasa_perdida, seed=seed)
            client.socket.settimeout(TIMEOUT)

            t0 = time.time()
            client.send(mensaje, mode="go_back_n")
            t1 = time.time()

            elapsed[0] = t1 - t0
            sent_segments[0] = client.number_of_sent_segments
        finally:
            client_done.set()
            if client is not None:
                try:
                    client.socket.close()
                except Exception:
                    pass

    t_srv = threading.Thread(target=run_server, daemon=True)
    t_cli = threading.Thread(target=run_client, daemon=True)
    t_srv.start()
    t_cli.start()
    t_cli.join(30)
    t_srv.join(2)

    assert bytes(recibido) == mensaje, f"Error: esperados {len(mensaje)} bytes, recibidos {len(recibido)}"
    return elapsed[0], sent_segments[0]


def correr_experimento_100b():
    mensaje = b"0123456789" * 10  # 100 bytes
    repeticiones = 5
    print("\n" + "=" * 70)
    print(f"EXPERIMENTO 1: Archivo de 100 Bytes (Pérdida={int(PROBABILIDAD_PERDIDA*100)}%, Timeout={TIMEOUT}s)")
    print("=" * 70)

    tiempos_con_cc, segs_con_cc = [], []
    tiempos_sin_cc, segs_sin_cc = [], []

    print("\n--- Ejecutando 5 repeticiones CON Control de Congestión ---")
    for i in range(1, repeticiones + 1):
        t, segs = ejecutar_envio(mensaje, con_cc=True, seed=100 + i * 9)
        tiempos_con_cc.append(t)
        segs_con_cc.append(segs)
        print(f"  Repetición {i}: Tiempo = {t:.3f} s | Segmentos enviados = {segs}")

    print("\n--- Ejecutando 5 repeticiones SIN Control de Congestión (Ventana fija 8) ---")
    for i in range(1, repeticiones + 1):
        t, segs = ejecutar_envio(mensaje, con_cc=False, seed=100 + i * 9)
        tiempos_sin_cc.append(t)
        segs_sin_cc.append(segs)
        print(f"  Repetición {i}: Tiempo = {t:.3f} s | Segmentos enviados = {segs}")

    # Tabla resumen
    print("\n" + "-" * 70)
    print(f"{'Repetición':<12} | {'CON CC (s)':<12} | {'CON CC (Segs)':<14} | {'SIN CC (s)':<12} | {'SIN CC (Segs)':<14}")
    print("-" * 70)
    for i in range(repeticiones):
        print(f"{i+1:<12} | {tiempos_con_cc[i]:<12.3f} | {segs_con_cc[i]:<14} | {tiempos_sin_cc[i]:<12.3f} | {segs_sin_cc[i]:<14}")
    print("-" * 70)

    prom_t_con = sum(tiempos_con_cc) / repeticiones
    prom_s_con = sum(segs_con_cc) / repeticiones
    prom_t_sin = sum(tiempos_sin_cc) / repeticiones
    prom_s_sin = sum(segs_sin_cc) / repeticiones
    print(f"{'PROMEDIO':<12} | {prom_t_con:<12.3f} | {prom_s_con:<14.1f} | {prom_t_sin:<12.3f} | {prom_s_sin:<14.1f}")
    print("-" * 70)


def correr_experimento_1000b():
    mensaje = b"0123456789" * 100  # 1000 bytes
    print("\n" + "=" * 70)
    print(f"EXPERIMENTO 2: Archivo de 1000 Bytes (>1000 B) para comparar tiempos")
    print("=" * 70)

    print("Midiendo transmisión CON Control de Congestión...")
    t_con, segs_con = ejecutar_envio(mensaje, con_cc=True, seed=200)
    print(f"  -> CON CC: Tiempo = {t_con:.3f} s | Segmentos enviados = {segs_con}")

    print("Midiendo transmisión SIN Control de Congestión...")
    t_sin, segs_sin = ejecutar_envio(mensaje, con_cc=False, seed=200)
    print(f"  -> SIN CC: Tiempo = {t_sin:.3f} s | Segmentos enviados = {segs_sin}")

    print("-" * 70)
    if t_sin > 0:
        diferencia = t_con - t_sin
        porcentaje = ((t_con / t_sin) - 1) * 100
        print(f"Diferencia: SIN CC fue {diferencia:.3f} s más rápido ({porcentaje:.1f}% más rápido)")
    print("-" * 70)


if __name__ == "__main__":
    t_inicio_total = time.time()
    correr_experimento_100b()
    correr_experimento_1000b()
    print(f"\nTiempo total de ejecución del script: {time.time() - t_inicio_total:.2f} s")
