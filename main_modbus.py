import sys
from collections import namedtuple
import threading
import queue
import multiprocessing as mp
import time
import yaml
import pymodbus
from pymodbus.client import ModbusTcpClient

class ModbusProxier:
    SlotType = namedtuple("SlotType", ["server", "address", "slave", "length"])
    def __init__(self, config):
        if isinstance(config, str):
            with open(config, "r") as f:
                config = yaml.safe_load(f)
        self.config = config
        self.tailing_byte = self.config["tailing_byte"].to_bytes(1, "big") # type: bytes

        self.clients = { it["name"]: ModbusTcpClient(it["host"],
                                                     port=it.get("port", 502),
                                                     framer=it.get("framer", pymodbus.Framer.SOCKET))
                         for it in self.config["servers"] }
        self.slots = { it["key"]: ModbusProxier.SlotType(it["server"], it["address"], it["slave"], it["length"]) for it in self.config["slots"] }

    def __del__(self):
        for it in self.clients.values():
            if it.connected:
                it.close()
    
    def connect(self, client):
        if not client.connect():
            print(f"Failed to connect to {client.comm_params.host}:{client.comm_params.port}.", file=sys.stderr)
            return False
        return True


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
        client = self.clients[s.server]
        v = values.copy()
        if offset < 0:
            offset = s.length + offset
        full_length = s.length - offset
        if len(v) > full_length:
            v = v[:full_length]
        self.write_registers_raw(client, s.address + offset, v, s.slave)

    def write_registers_raw(self, client, address, values, slave):
        # type: (ModbusTcpClient, int, list[int] | int, int) -> bool
        if not client.connected:
            if not self.connect(client): return False

        try:
            rr = client.write_registers(address, values, slave=slave)
        except pymodbus.ModbusException as e:
            print(f"Received ModbusException({e})", file=sys.stderr)

        if rr.isError():
            print(f"Received Modbus library error({rr})", file=sys.stderr)
            return False

        return True
    
    def read_holding_registers_raw(self, client, address, count, slave):
        # type: (ModbusTcpClient, int, int, int) -> list[int]
        if not client.connected:
            if not self.connect(client): return None
        try:
            rr = client.read_holding_registers(address, count, slave)
        except pymodbus.ModbusException as e:
            print(f"Received ModbusException({e})", file=sys.stderr)

        if rr.isError():
            print(f"Received Modbus library error({rr})", file=sys.stderr)
            return None

        return rr.registers

    def read_holding_registers(self, slot, count=None, offset=0):
        # type: (str, int, int) -> list[int]
        if slot not in self.slots:
            print(f"Slot {slot} not found.", file=sys.stderr)
            return None
        s = self.slots[slot]
        client = self.clients[s.server]
        if offset < 0:
            offset = s.length + offset
        if count is None or count < 0 or count > s.length:
            count = s.length
        if count > s.length - offset:
            count = s.length - offset
        return self.read_holding_registers_raw(client, s.address + offset, count, s.slave)

    def read_str(self, slot, count=None, encoding="utf-8"):
        # type: (str, int | None, str) -> str
        if slot not in self.slots:
            print(f"Slot {slot} not found.", file=sys.stderr)
            return None
        s = self.slots[slot]
        if count is None or count < 0 or count > s.length - 1:
            count = s.length - 1
        ret = self.read_holding_registers(slot, count)
        if ret is not None:
            ret = self.registers_to_str(ret, encoding)
        return ret

    def read_color(self, slot):
        # type: (str) -> int
        ret = self.read_holding_registers(slot, 1, -1)
        ret = ret[0] if ret is not None else 0
        return ret
    
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
        # type: (ModbusProxier | str | dict, int, None | mp.Queue) -> None
        """
        # Args
        - proxier: an instance of ModbusProxier or a config file or an dict containing the config
        - capacity: capacity of the queue. ignored if q is not None.
        - q: multiprocessing.Queue[dict[str]] with { "slot": slot, "msg": msg, "color": color } inside, where slot: int, msg: str, color: int. if None, mp.Queue will be created automatically.
        """
        super(ModbusDispatcher, self).__init__()

        self.capacity = capacity

        if q is not None:
            self.queue = q # type: mp.Queue[dict[str]]
        else:
            self.queue = mp.Queue(maxsize=self.capacity) # type: mp.Queue[dict[str]]

        if isinstance(proxier, ModbusProxier):
            self.proxier = proxier
        else:
            self.proxier = ModbusProxier(proxier)

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
        except:
            return False

    def process_one(self, block=True, timeout=None):
        # type: (bool, float | None) -> bool
        try:
            msg = self.queue.get(block, timeout)
        except:
            return False
        return self.proxier.write_str(msg["slot"], msg["msg"], msg["color"], encoding="gb2312")

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
                    31 35 20 20 20 20 00 02
                    D5 FD D4 DA BC EC B3 B5 00 02
                    c3 bb d3 d0 bc ec b3 b5 cf ee c4 bf 20 20 20 20
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
                    36 32 35 20 20 20 00 01
                    D5 FD D4 DA BC EC B3 B5 00 02
                    c3 bb d3 d0 bc ec b3 b5 cf ee c4 bf 20 20 20 20
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
                    36 32 35 20 20 20 00 01
                    ce de cf ee c4 bf 20 20 00 01
                    c3 bb d3 d0 bc ec b3 b5 cf ee c4 bf 20 20 20 20
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
                    36 32 35 20 20 20 00 01
                    ce de cf ee c4 bf 20 20 00 01
                    c3 bb d3 d0 bc ec b3 b5 cf ee c4 bf 20 20 20 20
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
                    36 32 35 20 20 20 00 01
                    ce de cf ee c4 bf 20 20 00 01
                    c3 bb d3 d0 bc ec b3 b5 cf ee c4 bf 20 20 20 20
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

def main():
    q = mp.Queue(50)
    subproc = mp.Process(target=dispatch_modbus, args=(q,))
    subproc.start()
    proxier = ModbusProxier("modbus-dispatcher.yaml")
    init_data = proxier.registers_from_bytes(bytes.fromhex("31 35 20 20 20 20 00 02 D5 FD D4 DA BC EC B3 B5 00 02 20 20 20 20 B3 B5 C1 BE D5 FD D4 DA BC EC B2 E2 A3 AC C7 EB D2 C0 B4 CE B4 F2 BF AA B3 B5 B5 C6 20 20 20 20 20 20 20 20 00 02 D3 D0 00 02 D3 D0 00 02 D3 D0 00 02 D3 D0 00 02 D7 F3 C1 C1 20 20 00 02 D3 D2 C1 C1 20 20 00 02 32 30 20 20 20 20 00 02 D7 F3 B2 BB C1 C1 00 01 D3 D2 B2 BB C1 C1 00 01 B2 BB C9 C1 CB B8 00 01 D7 F3 C1 C1 20 20 00 02 D3 D2 B2 BB C1 C1 00 01 C1 C1 C6 F0 20 20 00 02"), tailling=b'\x20')
    proxier.write_registers_raw(proxier.clients[proxier.slots[3].server], 0, init_data,1)
    
    q.put(dict(slot=3, msg="没有检车项目", color=1))
    time.sleep(1)
    assert_data(proxier.registers_to_bytes(proxier.read_holding_registers_raw(proxier.clients[proxier.slots[3].server], 0, 74, 1)), 0)
    q.put(dict(slot=1, msg="625", color=1))
    time.sleep(1)
    assert_data(proxier.registers_to_bytes(proxier.read_holding_registers_raw(proxier.clients[proxier.slots[1].server], 0, 74, 1)), 1)
    q.put(dict(slot=2, msg="无项目", color=1))
    time.sleep(1)
    assert_data(proxier.registers_to_bytes(proxier.read_holding_registers_raw(proxier.clients[proxier.slots[2].server], 0, 74, 1)), 2)
    q.put(dict(slot=4, msg="AB", color=1))
    time.sleep(1)
    assert_data(proxier.registers_to_bytes(proxier.read_holding_registers_raw(proxier.clients[proxier.slots[4].server], 0, 74, 1)), 3)
    q.put(dict(slot=15, msg="0123", color=2))
    time.sleep(1)
    assert_data(proxier.registers_to_bytes(proxier.read_holding_registers_raw(proxier.clients[proxier.slots[15].server], 0, 74, 1)), 4)


    print(proxier.read_holding_registers(3))
    assert proxier.read_str(3, encoding="gb2312").strip() == "没有检车项目"
    assert proxier.read_color(3)==1

    print("All test passed.")

    subproc.kill()
    pass

if __name__ == "__main__":
    main()

# --- For test ---
