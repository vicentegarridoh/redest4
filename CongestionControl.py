


#CongestionControl(MSS: int)
class CongestionControl():

    

    def __init__(self, MSS: int):
        self.MSS = MSS
        self.current_state = "slow start"
        self.cwnd = self.MSS  
        self.ssthresh = None
        

    #get_cwnd()-> bytes
    def get_cwnd(self): 
        return self.cwnd
        

    #get_MSS_in_cwnd()-> int
    def get_MSS_in_cwnd(self):
        return int(self.cwnd // self.MSS)

    #event_ack_received()
    #asocia los cambios de la recepcion de ACK
    def event_ack_received(self):
        if self.current_state == "slow start":
            self.cwnd += self.MSS
            if self.ssthresh is not None and self.cwnd >= self.ssthresh:
                self.current_state = "congestion avoidance"
        elif self.current_state == "congestion avoidance":
            self.cwnd += (1/self.get_MSS_in_cwnd())*self.MSS


    def event_timeout(self):
        self.ssthresh = int(self.get_cwnd()//2)
        self.cwnd = self.MSS
        self.current_state = "slow start"

    def is_state_slow_start(self):
        if self.current_state == "slow start":
            return True 
        return False
        

    def is_state_congestion_avoidance(self):
        if self.current_state == "congestion avoidance":
            return True 
        return False

    def get_ssthresh(self):
        return self.ssthresh
    

