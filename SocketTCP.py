import socket
import random
import time
from slidingWindowCC import SlidingWindowCC
from socketUDP import SocketUDP
from CongestionControl import CongestionControl


#TCP DE LA T3
class SocketTCP():
   
    SYN = 0x01   # 00000001
    ACK = 0x02   # 00000010
    FIN = 0x04   # 00000100

    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.address = ('localhost', 5000)
        self.SEQ = 0
        self.socket.settimeout(2.0)
        self.bytes_por_recibir = 0
        self.buffer_interno = b''
        self.segmentos_enviados = 0
        self.congestion_controler = CongestionControl(MSS=8)
        self.congestion_control_on = False
        self.debug = False

    def set_congestion_control(self, value: bool):
        self.congestion_control_on = bool(value)
        if self.congestion_control_on:
            self.congestion_controler = CongestionControl(MSS=8)

    def set_congestion_control_on(self, value: bool):
        self.set_congestion_control(value)

    def set_debug(self, value: bool):
        self.debug = bool(value)

    def printf(self,texto):
        if self.debug:
            print(texto)

    @property
    def number_of_sent_segments(self):
        return self.segmentos_enviados

    @number_of_sent_segments.setter
    def number_of_sent_segments(self, value):
        self.segmentos_enviados = value


    @staticmethod
    def parse_segment(segment):
        ##errores de que el header llego mal
        if not isinstance(segment, (bytes, bytearray)):
            raise TypeError("segment debe ser bytes")

        if len(segment) < 2:
            raise ValueError("Segmento demasiado corto")
        ##primer byte de flags, segundo byte de seq, y el resto es data
        flags = segment[0] 
        ack = False
        syn = False
        fin = False
        if (flags & SocketTCP.ACK) != 0:
            ack = True
        if (flags & SocketTCP.SYN) != 0:
            syn = True
        if (flags & SocketTCP.FIN) != 0:
            fin = True
        return {
            'seq': segment[1],
            'ack': ack,
            'syn': syn,
            'fin': fin,
            'data': bytes(segment[2:])
        }

    

    @staticmethod
    def create_segment(segment):
        seq = int(segment.get('seq', 0))

        data = segment.get('data', b'')
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("data debe ser bytes")
        flags = 0
        if segment.get('syn', False):
            flags |= SocketTCP.SYN
        if segment.get('ack', False):
            flags |= SocketTCP.ACK
        if segment.get('fin', False):
            flags |= SocketTCP.FIN

        return bytes([flags, seq]) + bytes(data)




    def bind(self,address) : 
        self.socket.bind(address)
        self.address = address
        return address
        

    
    def connect(self, address): 
        #ahora el handshake client
        self.SEQ = random.randint(0, 100)
        respuesta = self.create_segment({ 'seq': self.SEQ, 'syn': True })
        while True:
            try:
                self.socket.sendto(respuesta, address) #salto accept
                response_bytes, address_nueva = self.socket.recvfrom(1024) ##Esperamos respuesta
                response = self.parse_segment(response_bytes)
                if response['syn'] == True and response['ack'] == True:
                    break
            except (socket.timeout, TimeoutError, ConnectionResetError):
                continue
        self.SEQ = self.SEQ + 1
        respuesta = self.create_segment({'ack': True, 'seq': self.SEQ})
        self.socket.sendto(respuesta, address_nueva) #salto a accept
        self.address = address_nueva

        return self.address
    

    def accept(self): 
        ##handshake server
        self.socket.settimeout(None)
        message, address = self.socket.recvfrom(1024)
        #ahora nos deberia llegar un SYN del cliente conect
        mensaje_desempaquetado = self.parse_segment(message)
        if mensaje_desempaquetado['syn'] == True:
            #preparamos un nuevo socket, para que conecte en la llamada
            new_socket = SocketTCP()
            nuevo_puerto = random.randint(5001, 8000)
            new_socket.bind(('localhost', nuevo_puerto))
            new_socket.SEQ = random.randint(0, 100)
            respuesta = self.create_segment({'syn': True, 'ack': True, 'seq': new_socket.SEQ})
            
            seq_cliente = mensaje_desempaquetado['seq']
            seq_esperado = (seq_cliente + 1) % 256

            new_socket.socket.settimeout(2)
            while True:
                try:
                    new_socket.socket.sendto(respuesta, address)
                    message, address = new_socket.socket.recvfrom(1024)
                    mensaje_desempaquetado = self.parse_segment(message)
                    if mensaje_desempaquetado['syn'] == False and mensaje_desempaquetado['seq'] == seq_esperado:
                        break
                except (socket.timeout, TimeoutError, ConnectionResetError):
                    pass  
            
            return new_socket, address


    def send(self, message, mode="stop_and_wait"):
        if mode == "stop_and_wait":
            self.send_using_stop_and_wait(message)
        elif mode == "go_back_n":
            self.send_using_go_back_n(message)

    def recv(self, buff_size, mode="stop_and_wait"):
        if mode == "stop_and_wait":
            return self.recv_using_stop_and_wait(buff_size)
        elif mode == "go_back_n":
            return self.recv_using_go_back_n(buff_size)

    def close(self):
        contador_timeouts = 3
        respuesta = self.create_segment({'seq': self.SEQ, 'fin': True})
        self.socket.sendto(respuesta, self.address) #llega el mensaje

        while contador_timeouts > 0: 
            try:
                message, address = self.socket.recvfrom(1024)
                mensaje_desempaquetado = self.parse_segment(message)
                if mensaje_desempaquetado['ack'] == True and mensaje_desempaquetado['fin'] == True:
                    respuesta = self.create_segment({'ack': True, 'seq': (self.SEQ + 1) % 256})
                    for _ in range(3):
                        self.socket.sendto(respuesta, self.address)
                        time.sleep(2)
                    break
            except (socket.timeout, TimeoutError, ConnectionResetError):
                contador_timeouts -= 1
                self.socket.sendto(respuesta, self.address)
        self.socket.close()

    def recv_close(self):
        # host b: espera el primer FIN con reintentos por timeout
        contador_timeouts = 3
        while contador_timeouts > 0:
            try:
                message, address = self.socket.recvfrom(1024)
                mensaje_desempaquetado = self.parse_segment(message)
                if mensaje_desempaquetado['fin'] == True:
                    respuesta = self.create_segment({'ack': True, 'fin': True, 'seq': (self.SEQ + 1) % 256})
                    self.socket.sendto(respuesta, address)
                    
                    # Ahora espera el último ACK hasta 3 timeouts
                    contador_ack = 3
                    while contador_ack > 0:
                        try:
                            message, address = self.socket.recvfrom(1024)
                            mensaje_ack = self.parse_segment(message)
                            if mensaje_ack['ack'] == True:
                                break
                        except (socket.timeout, TimeoutError, ConnectionResetError):
                            contador_ack -= 1
                            if contador_ack > 0:
                                self.socket.sendto(respuesta, address)
                    break
                else:
                    ack_duplicado = self.create_segment({'ack': True, 'seq': self.SEQ})
                    self.socket.sendto(ack_duplicado, address)
            except (socket.timeout, TimeoutError, ConnectionResetError):
                contador_timeouts -= 1
        
        self.socket.close()

#################################################
#T4


    def send_using_stop_and_wait(self,message):
        #emisor
        largo = len(message)
        seq_esperado = (self.SEQ + len(str(largo).encode())) % 256
        llego_length = False
        while not llego_length:
            try:
                respuesta = self.create_segment({'seq': self.SEQ, 'data': str(largo).encode()})
                self.socket.sendto(respuesta, self.address)
                
                response_bytes, address = self.socket.recvfrom(2 + 16)
                response = self.parse_segment(response_bytes)
                if response['ack'] and not response['syn'] and response['seq'] == seq_esperado:
                    llego_length = True
                    self.SEQ = seq_esperado
            except (socket.timeout, TimeoutError, ConnectionResetError):
                pass  

        #ahora se manda el mensaje en trozos de 16 bytes
        trozo = 16
        for i in range(0, len(message), trozo):
            trozo_actual = message[i:i+trozo]
            seq_esperado = (self.SEQ + len(trozo_actual)) % 256
            llego_trozo = False
            while not llego_trozo:
                try:
                    respuesta = self.create_segment({'seq': self.SEQ, 'data': trozo_actual})
                    self.socket.sendto(respuesta, self.address)
                    
                    response_bytes, address = self.socket.recvfrom(2 + 16)
                    response = self.parse_segment(response_bytes)
                    if response['ack'] and not response['syn'] and response['seq'] == seq_esperado:
                        llego_trozo = True
                        self.SEQ = seq_esperado
                except (socket.timeout, TimeoutError, ConnectionResetError):
                    pass


    def recv_using_stop_and_wait(self, buff_size):
        #receptor
        #recibimos el largo del mensaje primero
        if self.bytes_por_recibir == 0 and len(self.buffer_interno) == 0:
            while True:
                try:
                    message, address = self.socket.recvfrom(2 + 16)
                    mensaje_desempaquetado = self.parse_segment(message)
                    if mensaje_desempaquetado['data']:
                        self.bytes_por_recibir = int(mensaje_desempaquetado['data'].decode())
                        self.SEQ = (mensaje_desempaquetado['seq'] + len(mensaje_desempaquetado['data'])) % 256
                        respuesta = self.create_segment({'ack': True, 'seq': self.SEQ})
                        self.socket.sendto(respuesta, address)
                        break
                except (socket.timeout, TimeoutError, ConnectionResetError):
                    pass

        #recibimos los datos en trozos
        while len(self.buffer_interno) < buff_size and self.bytes_por_recibir > 0:
            try:
                message, address = self.socket.recvfrom(2 + 16)
                mensaje_desempaquetado = self.parse_segment(message)
                
                if mensaje_desempaquetado['seq'] == self.SEQ:
                    #correcto seq
                    self.buffer_interno += mensaje_desempaquetado['data']
                    self.bytes_por_recibir -= len(mensaje_desempaquetado['data'])
                    
                    self.SEQ = (self.SEQ + len(mensaje_desempaquetado['data'])) % 256
                    respuesta = self.create_segment({'ack': True, 'seq': self.SEQ})
                    self.socket.sendto(respuesta, address)
                else:
                    #paquete perdido, reenviar el que corresponde
                    respuesta = self.create_segment({'ack': True, 'seq': self.SEQ})
                    self.socket.sendto(respuesta, address)
            except (socket.timeout, TimeoutError, ConnectionResetError):
                pass

        #recortar los bytes a retornar
        retorno_final = min(buff_size, len(self.buffer_interno))
        datos_a_retornar = self.buffer_interno[:retorno_final]
        self.buffer_interno = self.buffer_interno[retorno_final:]
        return datos_a_retornar

    def send_using_go_back_n(self, message):
        # emisor
        # envío del largo del mensaje 
        largo = len(message)
        seq_esperado = (self.SEQ + len(str(largo).encode())) % 256
        llego_length = False
        while not llego_length:
            try:
                respuesta = self.create_segment({'seq': self.SEQ, 'data': str(largo).encode()})
                self.socket.sendto(respuesta, self.address)
                self.segmentos_enviados += 1
                
                response_bytes, address = self.socket.recvfrom(1024)
                response = self.parse_segment(response_bytes)
                if response['ack'] and not response['syn'] and response['seq'] == seq_esperado:
                    llego_length = True
                    self.SEQ = seq_esperado
            except (socket.timeout, TimeoutError, ConnectionResetError):
                pass

        if len(message) == 0:
            return

        # manejar tamano del trozo, 8 con congestion, 16 si no
        #actualizacion, ahora no se debe poner 
        if self.congestion_control_on:
            trozo = self.congestion_controler.MSS
        else:
            trozo = self.congestion_controler.MSS

        data_list = []
        for i in range(0, len(message), trozo):
            trozo_actual = message[i:i+trozo]
            data_list.append(trozo_actual)


        tamano_ventana = 8
        if self.congestion_control_on:
            tamano_ventana = max(1, self.congestion_controler.get_MSS_in_cwnd())
            

        self.printf(f"[DEBUG CC INICIO] cwnd: {self.congestion_controler.get_cwnd()} B, "
                    f"window_size: {tamano_ventana}, "
                    f"estado: {self.congestion_controler.current_state}")

        data_window = SlidingWindowCC(tamano_ventana, data_list, self.SEQ)

        # Enviar los paquetes de la ventana inicial
        for i in range(data_window.window_size):
            data = data_window.get_data(i)
            seq = data_window.get_sequence_number(i)
            if data is not None and seq is not None:
                paquete = self.create_segment({'seq': seq % 256, 'data': data})
                self.socket.sendto(paquete, self.address)
                self.segmentos_enviados += 1

        #bucle de recepcion 
        while data_window.get_data(0) is not None:
            try:
                response_bytes, address = self.socket.recvfrom(1024) #salto recv
                respuesta = self.parse_segment(response_bytes)
                if respuesta['ack'] and not respuesta['syn']:
                    seq_recibido = respuesta['seq']

                    # buscamos el paquete que llego y movemos ventana o descartamos
                    posicion_confirmada = None
                    for i in range(data_window.window_size):
                        data_i = data_window.get_data(i)
                        seq_i = data_window.get_sequence_number(i)
                        if data_i is not None and seq_i is not None:
                            esperado = (seq_i + len(data_i)) % 256
                            if seq_recibido == esperado:
                                posicion_confirmada = i
                                break

                               
                    # caso borde: si la ventana se achicó por timeout, puede llegar un ACK mayor a la ventana
                    if posicion_confirmada is None and self.congestion_control_on and data_window.get_data(0) is not None:

                        #vemos la ultima casilla de la ventana
                        ultimo_idx = -1
                        for i in range(data_window.window_size):
                            if data_window.get_data(i) is not None and data_window.get_sequence_number(i) is not None:
                                ultimo_idx = i

                        if ultimo_idx >= 0:
                            ultimo_seq = data_window.get_sequence_number(ultimo_idx)
                            ultimo_data = data_window.get_data(ultimo_idx)
                            ultimo_ack = (ultimo_seq + len(ultimo_data)) % 256
                            #vemos si esta adelante
                            if 0 < (seq_recibido - ultimo_ack) % 256 < 128:
                                while data_window.get_data(0) is not None:
                                    #movemos como si hubiera llegado
                                    data_window.move_window(data_window.window_size)
                                    for i in range(data_window.window_size):
                                        data_i = data_window.get_data(i)
                                        seq_i = data_window.get_sequence_number(i)
                                        if data_i is not None and seq_i is not None:
                                            if seq_recibido == (seq_i + len(data_i)) % 256:
                                                posicion_confirmada = i
                                                break
                                    if posicion_confirmada is not None or data_window.get_data(0) is None:
                                        break


                                    #nos detenemos en caso de pasarnos
                                    maximo_idx = data_window.window_size - 1 
                                    while maximo_idx >= 0 and data_window.get_data(maximo_idx) is None:
                                        maximo_idx -= 1
                                    if maximo_idx >= 0:
                                        maximo_seq = data_window.get_sequence_number(maximo_idx)
                                        maximo_data = data_window.get_data(maximo_idx)
                                        if (seq_recibido - (maximo_seq + len(maximo_data)) % 256) % 256 >= 128:
                                            break

               

                    # si coincide, avanzamos la ventana
                    if posicion_confirmada is not None:
                        if self.congestion_control_on:
                            self.congestion_controler.event_ack_received()

                        pasos = posicion_confirmada + 1
                        data_window.move_window(pasos)
                        self.SEQ = seq_recibido

                        # Transmitir los nuevos paquetes que ingresaron por el avance de ventana
                        inicio_nuevos = max(0, data_window.window_size - pasos)
                        for i in range(inicio_nuevos, data_window.window_size):
                            nueva_data = data_window.get_data(i)
                            nueva_seq = data_window.get_sequence_number(i)
                            if nueva_data is not None and nueva_seq is not None:
                                paquete = self.create_segment({'seq': nueva_seq % 256, 'data': nueva_data})
                                self.socket.sendto(paquete, self.address)
                                self.segmentos_enviados += 1

                        # actualizar tamano de ventana 
                        if self.congestion_control_on:
                            nuevo_tamano = max(1, self.congestion_controler.get_MSS_in_cwnd())
                            if nuevo_tamano != data_window.window_size:
                                tamano_anterior = data_window.window_size
                                data_window.update_window_size(nuevo_tamano)
                                # nuevo tamaño de ventana, entonces se deben mandar mas acks
                                if nuevo_tamano > tamano_anterior:
                                    for i in range(tamano_anterior, nuevo_tamano):
                                        nueva_data = data_window.get_data(i)
                                        nueva_seq = data_window.get_sequence_number(i)
                                        if nueva_data is not None and nueva_seq is not None:
                                            paquete = self.create_segment({'seq': nueva_seq % 256, 'data': nueva_data})
                                            self.socket.sendto(paquete, self.address)
                                            self.segmentos_enviados += 1

                            self.printf(f"[DEBUG CC ACK] cwnd: {self.congestion_controler.get_cwnd()} B, "
                                        f"window_size: {data_window.window_size}, "
                                        f"ssthresh: {self.congestion_controler.get_ssthresh()}, "
                                        f"estado: {self.congestion_controler.current_state}")


            except (socket.timeout, TimeoutError, ConnectionResetError):
                if self.congestion_control_on:
                    self.congestion_controler.event_timeout()
                    nuevo_tamano = max(1, self.congestion_controler.get_MSS_in_cwnd())
                    data_window.update_window_size(nuevo_tamano)
                    self.printf(f"[DEBUG CC TIMEOUT] cwnd: {self.congestion_controler.get_cwnd()}, "
                                f"window_size: {data_window.window_size}, "
                                f"ssthresh: {self.congestion_controler.get_ssthresh()}, "
                                f"estado: {self.congestion_controler.current_state}")

                # no llego un ack, asi q transmitimos todos los paquetes.
                for i in range(data_window.window_size):
                    data = data_window.get_data(i)
                    seq = data_window.get_sequence_number(i)
                    if data is not None and seq is not None:
                        paquete = self.create_segment({'seq': seq % 256, 'data': data})
                        self.socket.sendto(paquete, self.address)
                        self.segmentos_enviados += 1

    def recv_using_go_back_n(self, buff_size):
        # receptor: recibimos el largo del mensaje primero si aún no se recibe
        if self.bytes_por_recibir == 0 and len(self.buffer_interno) == 0:
            while True:
                try:
                    message, address = self.socket.recvfrom(1024) #salto a send
                    mensaje_desempaquetado = self.parse_segment(message)
                    if mensaje_desempaquetado['data']:
                        self.bytes_por_recibir = int(mensaje_desempaquetado['data'].decode())
                        self.SEQ = (mensaje_desempaquetado['seq'] + len(mensaje_desempaquetado['data'])) % 256
                        respuesta = self.create_segment({'ack': True, 'seq': self.SEQ})
                        self.socket.sendto(respuesta, address)
                        break
                except (socket.timeout, TimeoutError, ConnectionResetError):
                    pass

        # recibimos los datos en trozos
        while len(self.buffer_interno) < buff_size and self.bytes_por_recibir > 0:
            try:
                message, address = self.socket.recvfrom(1024) #salto a send
                mensaje_desempaquetado = self.parse_segment(message)
                
                if mensaje_desempaquetado['seq'] == self.SEQ:
                    #paquete con seq exacto.
                    self.buffer_interno += mensaje_desempaquetado['data']
                    self.bytes_por_recibir -= len(mensaje_desempaquetado['data'])
                    
                    self.SEQ = (self.SEQ + len(mensaje_desempaquetado['data'])) % 256
                    respuesta = self.create_segment({'ack': True, 'seq': self.SEQ})
                    self.socket.sendto(respuesta, address)
                else:
                    #paquete erroneo
                    respuesta = self.create_segment({'ack': True, 'seq': self.SEQ})
                    self.socket.sendto(respuesta, address)
            except (socket.timeout, TimeoutError, ConnectionResetError):
                pass

        # recortar los bytes a retornar
        retorno_final = min(buff_size, len(self.buffer_interno))
        datos_a_retornar = self.buffer_interno[:retorno_final]
        self.buffer_interno = self.buffer_interno[retorno_final:]
        return datos_a_retornar