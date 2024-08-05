import sys
from collections import namedtuple
import threading
import queue
import multiprocessing as mp
import time
import yaml
import socket

class LEDProxier:
    SlotType = namedtuple("SlotType", ["server", "address", "slave", "length"])
    ServerType = namedtuple("ServerType", ["host", "port"])
    def __init__(self, config):
        if isinstance(config, str):
            with open(config, "r") as f:
                config = yaml.safe_load(f)
        self.config = config
        self.tailing_byte = self.config["tailing_byte"].to_bytes(1, "big") # type: bytes
        
        self.servers = { it["name"]: LEDProxier.ServerType(it["host"], it["port"])
                         for it in self.config["servers"] }
        self.slots = { it["key"]: LEDProxier.SlotType(it["server"], it["address"], it["slave"], it["length"]) for it in self.config["slots"] }
        self.header_bin = bytes.fromhex("00 01 00 00 00 9B 01 10 00 00 00 4A 94")
        self.data = list(bytes.fromhex("31 35 20 20 20 20 00 02 D5 FD D4 DA BC EC B3 B5 00 02 20 20 20 20 B3 B5 C1 BE D5 FD D4 DA BC EC B2 E2 A3 AC C7 EB D2 C0 B4 CE B4 F2 BF AA B3 B5 B5 C6 20 20 20 20 20 20 20 20 00 02 D3 D0 00 02 D3 D0 00 02 D3 D0 00 02 D3 D0 00 02 D7 F3 C1 C1 20 20 00 02 D3 D2 C1 C1 20 20 00 02 32 30 20 20 20 20 00 02 D7 F3 B2 BB C1 C1 00 01 D3 D2 B2 BB C1 C1 00 01 B2 BB C9 C1 CB B8 00 01 D7 F3 C1 C1 20 20 00 02 D3 D2 B2 BB C1 C1 00 01 C1 C1 C6 F0 20 20 00 02"))

    def connect(self, s, server):
        # type: (socket.socket, LEDProxier.ServerType) -> bool
        try:
            s.connect((server.host, server.port))
            return True
        except socket.error as e:
            print(f"Received Exception when connecting ({e})", file=sys.stderr)
            return False

    def write_str(self, slot, msg, color, encoding="utf-8"):
        # type: (str, str, int, str) -> bool
        if slot not in self.slots:
            print(f"Slot {slot} not found.", file=sys.stderr)
            return False
        s = self.slots[slot]
        v = self.registers_from_str(msg, encoding, tailling=self.tailing_byte)
        if len(v) > s.length - 1:
            v = v[:s.length - 1]
        elif len(v) < s.length - 1:
            v.extend([int.from_bytes(self.tailing_byte * 2, byteorder="big")] * (s.length - 1 - len(v)))
        v.append(color)
        return self.write_registers(slot, v)

    def write_str_without_color(self, slot, msg, encoding="utf-8"):
        # type: (str, str, str) -> bool
        if slot not in self.slots:
            print(f"Slot {slot} not found.", file=sys.stderr)
            return False
        s = self.slots[slot]
        v = self.registers_from_str(msg, encoding, tailling=self.tailing_byte)
        if len(v) > s.length - 1:
            v = v[:s.length - 1]
        elif len(v) < s.length - 1:
            v.extend([int.from_bytes(self.tailing_byte * 2, byteorder="big")] * (s.length - 1 - len(v)))
        return self.write_registers(slot, v)

    def write_color(self, slot, color):
        # type: (str, int, str) -> bool
        return self.write_registers(slot, color, -1)

    def write_bytes(self, slot, msg, offset=0):
        # type: (str, bytes, int) -> bool
        """
        - offset: in WORD
        """
        return self.write_registers(slot, self.registers_from_bytes(msg, tailling=self.tailing_byte), offset)

    def write_registers(self, slot, values, offset=0):
        # type: (str, list[int] | int, int) -> bool
        if slot not in self.slots:
            print(f"Slot {slot} not found.", file=sys.stderr)
            return False
        s = self.slots[slot]
        server_info = self.servers[s.server]
        v = values.copy()
        if offset < 0:
            offset = s.length + offset
        full_length = s.length - offset
        if len(v) > full_length:
            v = v[:full_length]
        self.write_registers_raw(server_info, s.address + offset, v, s.slave)

    def write_registers_raw(self, server_info, address, values, slave):
        # type: (LEDProxier.ServerType, int, list[int] | int, int) -> bool
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if not self.connect(s, server_info):
            print("Connection failed.", file=sys.stderr)
            return False

        try:
            for i, n in enumerate(values):
                self.data[(address + i) * 2 : (address + i) * 2 + 2] = n.to_bytes(2, byteorder="big")
            rr = s.send(self.header_bin)
            rr = s.send(bytes(self.data))
        except Exception as e:
            print(f"Received Exception when writing registers ({e})", file=sys.stderr)
            return False

        s.close()

        return True
    
    @classmethod
    def registers_from_bytes(cls, msg, tailling=b'\x00'):
        # type: (bytes, bytes) -> list[int]
        payload = []
        payload_bin = msg
        if (len(payload_bin) % 2 == 1):
            payload_bin = payload_bin + tailling[:1]
        for i in range(len(payload_bin)//2):
            payload.append(int.from_bytes(bytes(payload_bin[i*2:i*2+2]), byteorder="big"))
        return payload
    
    @classmethod
    def registers_to_bytes(cls, regs):
        # type: (list[int]) -> bytes
        payload = []
        for it in regs:
            payload.extend(list(it.to_bytes(2, byteorder="big")))
        return bytes(payload)
    
    @classmethod
    def registers_from_str(cls, msg, encoding="utf-8", tailling=b'\x00'):
        # type: (str, str, bytes) -> list[int]
        return cls.registers_from_bytes(msg.encode(encoding=encoding), tailling=tailling)
    
    @classmethod
    def registers_to_str(cls, regs, encoding="utf-8"):
        # type: (list[int], str) -> str
        return cls.registers_to_bytes(regs).decode(encoding)

class ModbusDispatcher(threading.Thread):
    def __init__(self, proxier, capacity=50, q=None):
        # type: (LEDProxier | str | dict, int, None | mp.Queue) -> None
        """
        # Args
        - proxier: an instance of LEDProxier or a config file or an dict containing the config
        - capacity: capacity of the queue. ignored if q is not None.
        - q: multiprocessing.Queue[dict[str]] with { "slot": slot, "msg": msg, "color": color } inside, where slot: int, msg: str, color: int. if None, mp.Queue will be created automatically.
        """
        super(ModbusDispatcher, self).__init__()

        self.capacity = capacity

        if q is not None:
            self.queue = q # type: mp.Queue[dict[str]]
        else:
            self.queue = mp.Queue(maxsize=self.capacity) # type: mp.Queue[dict[str]]

        if isinstance(proxier, LEDProxier):
            self.proxier = proxier
        else:
            self.proxier = LEDProxier(proxier)

    def push(self, slot, msg, color, block=True, timeout=None):
        # type: (str, str, int, bool, float | None) -> bool
        """
        # Args
        - slot: slot name
        - msg: message string
        """
        if slot not in self.proxier.slots:
            print(f"Slot {slot} not found.", file=sys.stderr)
            return False
        try:
            self.queue.put(dict(slot=slot, msg=msg, color=color), block=block, timeout=timeout)
            return True
        except Exception as e:
            print(f"Received Exception while enqueing ({e})", file=sys.stderr)
            return False

    def process_one(self, block=True, timeout=None):
        # type: (bool, float | None) -> bool
        try:
            msg = self.queue.get(block, timeout)
            return self.proxier.write_str(msg["slot"], msg["msg"], msg["color"], encoding="gb2312")
        except Exception as e:
            print(f"Received Exception while processing ({e})", file=sys.stderr)
            return False

    def run(self):
        self.running = True
        while self.running:
            if not self.running: break
            self.process_one()

    def stop(self):
        self.running = False


def dispatch_modbus(q):
    dispatcher = ModbusDispatcher("modbus-dispatcher.yaml", q=q)
    dispatcher.run()


# === For test ===

def assert_data(data, i):
    datas = [
        bytes.fromhex("""
                    00 01 00 00 00 9B
                    01 10 00 00 00 4A 94
                    31 35 20 20 20 20 00 02
                    D5 FD D4 DA BC EC B3 B5 00 02
                    c7 eb b0 b4 cc e1 ca be b2 d9 d7 f7 20 20 20 20
                    20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20
                    20 20 20 20 20 20 20 20 00 01
                    D3 D0 00 02 D3 D0 00 02 D3 D0 00 02 D3 D0 00 02
                    D7 F3 C1 C1 20 20 00 02 D3 D2 C1 C1 20 20 00 02
                    32 30 20 20 20 20 00 02 D7 F3 B2 BB C1 C1 00 01
                    D3 D2 B2 BB C1 C1 00 01 B2 BB C9 C1 CB B8 00 01
                    D7 F3 C1 C1 20 20 00 02 D3 D2 B2 BB C1 C1 00 01
                    C1 C1 C6 F0 20 20 00 02
                    """),
        bytes.fromhex("""
                    00 01 00 00 00 9B
                    01 10 00 00 00 4A 94
                    36 32 35 20 20 20 00 01
                    D5 FD D4 DA BC EC B3 B5 00 02
                    c7 eb b0 b4 cc e1 ca be b2 d9 d7 f7 20 20 20 20
                    20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20
                    20 20 20 20 20 20 20 20 00 01
                    D3 D0 00 02 D3 D0 00 02 D3 D0 00 02 D3 D0 00 02
                    D7 F3 C1 C1 20 20 00 02 D3 D2 C1 C1 20 20 00 02
                    32 30 20 20 20 20 00 02 D7 F3 B2 BB C1 C1 00 01
                    D3 D2 B2 BB C1 C1 00 01 B2 BB C9 C1 CB B8 00 01
                    D7 F3 C1 C1 20 20 00 02 D3 D2 B2 BB C1 C1 00 01
                    C1 C1 C6 F0 20 20 00 02
                    """),
        bytes.fromhex("""
                    00 01 00 00 00 9B
                    01 10 00 00 00 4A 94
                    36 32 35 20 20 20 00 01
                    bc ec b3 b5 d6 d0 20 20 00 01
                    c7 eb b0 b4 cc e1 ca be b2 d9 d7 f7 20 20 20 20
                    20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20
                    20 20 20 20 20 20 20 20 00 01
                    D3 D0 00 02 D3 D0 00 02 D3 D0 00 02 D3 D0 00 02
                    D7 F3 C1 C1 20 20 00 02 D3 D2 C1 C1 20 20 00 02
                    32 30 20 20 20 20 00 02 D7 F3 B2 BB C1 C1 00 01
                    D3 D2 B2 BB C1 C1 00 01 B2 BB C9 C1 CB B8 00 01
                    D7 F3 C1 C1 20 20 00 02 D3 D2 B2 BB C1 C1 00 01
                    C1 C1 C6 F0 20 20 00 02
                    """),
        bytes.fromhex("""
                    00 01 00 00 00 9B
                    01 10 00 00 00 4A 94
                    36 32 35 20 20 20 00 01
                    bc ec b3 b5 d6 d0 20 20 00 01
                    c7 eb b0 b4 cc e1 ca be b2 d9 d7 f7 20 20 20 20
                    20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20
                    20 20 20 20 20 20 20 20 00 01
                    41 42 00 01 D3 D0 00 02 D3 D0 00 02 D3 D0 00 02
                    D7 F3 C1 C1 20 20 00 02 D3 D2 C1 C1 20 20 00 02
                    32 30 20 20 20 20 00 02 D7 F3 B2 BB C1 C1 00 01
                    D3 D2 B2 BB C1 C1 00 01 B2 BB C9 C1 CB B8 00 01
                    D7 F3 C1 C1 20 20 00 02 D3 D2 B2 BB C1 C1 00 01
                    C1 C1 C6 F0 20 20 00 02
                    """),
        bytes.fromhex("""
                    00 01 00 00 00 9B
                    01 10 00 00 00 4A 94
                    36 32 35 20 20 20 00 01
                    bc ec b3 b5 d6 d0 20 20 00 01
                    c7 eb b0 b4 cc e1 ca be b2 d9 d7 f7 20 20 20 20
                    20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20
                    20 20 20 20 20 20 20 20 00 01
                    41 42 00 01 D3 D0 00 02 D3 D0 00 02 D3 D0 00 02
                    D7 F3 C1 C1 20 20 00 02 D3 D2 C1 C1 20 20 00 02
                    32 30 20 20 20 20 00 02 D7 F3 B2 BB C1 C1 00 01
                    D3 D2 B2 BB C1 C1 00 01 B2 BB C9 C1 CB B8 00 01
                    D7 F3 C1 C1 20 20 00 02 30 31 32 33 20 20 00 02
                    C1 C1 C6 F0 20 20 00 02
                    """),
    ]
    assert bytes(data) == datas[i]

def test_server_handler(s, a, i):
    # type: (socket.socket, socket._RetAddress, int) -> None
    all_data = []
    while True:
        data = s.recv(4096)
        if len(data) == 0: break; # disconnect
        all_data.extend(list(data))
        print(f"len(all_data)={len(all_data)}")
        j = 0
        while j < len(data):
            print(data[j:min(j+16, len(data))].hex(" "))
            j = min(j+16, len(data))
        while len(all_data) >= 161:
            assert_data(all_data[:161], i)
            all_data = all_data[161:]


def test_server():
    svr = socket.socket()
    svr.bind(("0.0.0.0", 5003))
    svr.listen(0)
    i = 0
    while True:
        s, a = svr.accept()
        print(f"Got a connection[{i}].")
        test_server_handler(s, a, i)
        i += 1

def main():
    server = mp.Process(target=test_server)
    server.start()

    q = mp.Queue(50)
    subproc = mp.Process(target=dispatch_modbus, args=(q,))
    subproc.start()

    q.put(dict(slot=3, msg="请按提示操作", color=1))
    time.sleep(1)
    q.put(dict(slot=1, msg="625", color=1))
    time.sleep(1)
    q.put(dict(slot=2, msg="检车中", color=1))
    time.sleep(1)
    q.put(dict(slot=4, msg="AB", color=1))
    time.sleep(1)
    q.put(dict(slot=15, msg="0123", color=2))
    time.sleep(1)

    subproc.kill()
    server.kill()
    # subproc.join()
    # server.join()
    pass

if __name__ == "__main__":
    main()

# --- For test ---
