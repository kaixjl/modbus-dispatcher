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
        self.tailing_byte = self.config["tailing_byte"] # type: int

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
        v = self.registers_from_str(msg, encoding)
        if len(v) > s.length - 1:
            v = v[:s.length - 1]
        elif len(v) < s.length - 1:
            v.extend([int.from_bytes(bytes([self.tailing_byte] * 2), byteorder="big")] * (s.length - 1 - len(v)))
        v.append(color)
        print(v)
        return self.write_registers(slot, v)

    def write_str_without_color(self, slot, msg, encoding="utf-8"):
        # type: (str, str, str) -> bool
        if slot not in self.slots:
            print(f"Slot {slot} not found.", file=sys.stderr)
            return False
        s = self.slots[slot]
        v = self.registers_from_str(msg, encoding)
        if len(v) > s.length - 1:
            v = v[:s.length - 1]
        elif len(v) < s.length - 1:
            v.extend([self.tailing_byte] * 2 * (s.length - 1 - len(v)))
        return self.write_registers(slot, v)

    def write_color(self, slot, color):
        # type: (str, int, str) -> bool
        return self.write_registers(slot, color, -1)

    def write_bytes(self, slot, msg, offset=0):
        # type: (str, bytes, int) -> bool
        """
        - offset: in WORD
        """
        return self.write_registers(slot, self.registers_from_bytes(msg), offset)

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
    def registers_from_bytes(cls, msg):
        # type: (bytes) -> list[int]
        payload = []
        payload_bin = msg
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
    def registers_from_str(cls, msg, encoding="utf-8"):
        # type: (str, str) -> list[int]
        return cls.registers_from_bytes(msg.encode(encoding=encoding))
    
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
            self.queue = mp.Queue(maxlen=self.capacity) # type: mp.Queue[dict[str]]

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

def main():
    q = mp.Queue(50)
    subproc = mp.Process(target=dispatch_modbus, args=(q,))
    subproc.start()

    q.put(dict(slot=3, msg="苹果派", color=1))

    time.sleep(1)
    proxier = ModbusProxier("modbus-dispatcher.yaml")
    print(proxier.read_holding_registers(3))
    print(proxier.read_str(3, encoding="gb2312"))
    print(proxier.read_color(3))

    subproc.kill()
    pass

if __name__ == "__main__":
    main()

# --- For test ---
