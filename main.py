import sys
from collections import namedtuple, deque
import threading
import time
import yaml
import pymodbus
from pymodbus.client import ModbusTcpClient

class ModbusProxier:
    SlotType = namedtuple("SlotType", ["server", "address", "slave"])
    def __init__(self, config):
        if isinstance(config, str):
            with open(config, "r") as f:
                config = yaml.safe_load(f)
        self.config = config

        self.clients = { it["name"]: ModbusTcpClient(it["host"],
                                                     port=it.get("port", 502),
                                                     framer=it.get("framer", pymodbus.Framer.SOCKET))
                         for it in self.config["servers"] }
        self.slots = { it["key"]: ModbusProxier.SlotType(it["server"], it["address"], it["slave"]) for it in self.config["slots"] }

    def __del__(self):
        for it in self.clients.values():
            if it.connected:
                it.close()

    def write_str(self, slot, msg):
        # type: (str, str) -> bool
        if slot not in self.slots:
            print(f"Slot {slot} not found.", file=sys.stderr)
            return False
        client = self.clients[self.slots[slot].server]
        if not client.connected:
            if not client.connect():
                print(f"Failed to connect to {client.comm_params.host}:{client.comm_params.port}.", file=sys.stderr)
                return False
        try:
            rr = client.write_registers(self.slots[slot].address, ModbusTcpClient.convert_to_registers(msg, ModbusTcpClient.DATATYPE.STRING), slave=self.slots[slot].slave)
        except pymodbus.ModbusException as e:
            print(f"Received ModbusException({e})", file=sys.stderr)

        if rr.isError():
            print(f"Received Modbus library error({rr})", file=sys.stderr)
            return False

        return True

    def write_registers(self, slot, values):
        # type: (str, list[int] | int) -> bool
        if slot not in self.slots:
            print(f"Slot {slot} not found.", file=sys.stderr)
            return False
        client = self.clients[self.slots[slot].server]
        if not client.connected:
            if not client.connect():
                print(f"Failed to connect to {client.comm_params.host}:{client.comm_params.port}.", file=sys.stderr)
                return False
        try:
            rr = client.write_registers(self.slots[slot].address, values, slave=self.slots[slot].slave)
        except pymodbus.ModbusException as e:
            print(f"Received ModbusException({e})", file=sys.stderr)

        if rr.isError():
            print(f"Received Modbus library error({rr})", file=sys.stderr)
            return False

        return True

    def read_holding_registers(self, slot, count):
        # type: (str, int) -> list[int]
        if slot not in self.slots:
            print(f"Slot {slot} not found.", file=sys.stderr)
            return False
        client = self.clients[self.slots[slot].server]
        if not client.connected:
            if not client.connect():
                print(f"Failed to connect to {client.comm_params.host}:{client.comm_params.port}.", file=sys.stderr)
                return None
        try:
            rr = client.read_holding_registers(self.slots[slot].address, count, self.slots[slot].slave)
        except pymodbus.ModbusException as e:
            print(f"Received ModbusException({e})", file=sys.stderr)

        if rr.isError():
            print(f"Received Modbus library error({rr})", file=sys.stderr)
            return None

        return rr.registers

    def read_holding_registers_str(self, slot, count):
        # type: (str, int) -> str
        if slot not in self.slots:
            print(f"Slot {slot} not found.", file=sys.stderr)
            return False
        client = self.clients[self.slots[slot].server]
        if not client.connected:
            if not client.connect():
                print(f"Failed to connect to {client.comm_params.host}:{client.comm_params.port}.", file=sys.stderr)
                return None
        try:
            rr = client.read_holding_registers(self.slots[slot].address, count, self.slots[slot].slave)
        except pymodbus.ModbusException as e:
            print(f"Received ModbusException({e})", file=sys.stderr)

        if rr.isError():
            print(f"Received Modbus library error({rr})", file=sys.stderr)
            return None

        return ModbusTcpClient.convert_from_registers(rr.registers, ModbusTcpClient.DATATYPE.STRING)

class ModbusDispatcher(threading.Thread):
    MsgType = namedtuple("MsgType", ["slot", "msg"])
    def __init__(self, proxier, capacity=50):
        # type: (ModbusProxier | str | dict, int) -> None
        """
        # Args
        - proxier: an instance of ModbusProxier or a config file or an dict containing the config
        - capacity: capacity of the queue
        """
        super(ModbusDispatcher, self).__init__()
        self.capacity = capacity
        self.deque = deque(maxlen=self.capacity) # type: deque[ModbusDispatcher.MsgType]
        if isinstance(proxier, ModbusProxier):
            self.proxier = proxier
        else:
            self.proxier = ModbusProxier(proxier)

        # === multi-thread ===
        self.lock = threading.Lock()
        self.running = False
        # --- multi-thread ---

    def push(self, slot, msg):
        # type: (str, str) -> bool
        """
        # Args
        - slot: slot name
        - msg: message string
        """
        if slot not in self.proxier.slots:
            print(f"Slot {slot} not found.", file=sys.stderr)
            return False
        self.deque.append(ModbusDispatcher.MsgType(slot=slot, msg=msg))
        return True

    def process_one(self):
        # type: () -> bool
        if len(self.deque) == 0: return False
        msg = self.deque.popleft()
        self.proxier.write_str(msg.slot, msg.msg)

    # === multi-thread ===
    def push_safe(self, slot, msg):
        self.lock.acquire(True, -1)
        self.push(slot, msg)
        self.lock.release()

    def process_one_safe(self):
        # type: () -> bool
        if len(self.deque) == 0: return False
        if self.lock.acquire(True, 1):
            self.process_one()
            self.lock.release()
            return True
        return False

    def run(self):
        self.running = True
        while self.running:
            if len(self.deque) == 0:
                time.sleep(0)
                continue
            if self.lock.acquire(False, -1):
                if not self.running: break
                self.process_one()
                self.lock.release()

    def stop(self):
        self.running = False
    # --- multi-thread ---


def main():
    dispatcher = ModbusDispatcher("modbus-dispatcher.yaml")
    proxier = dispatcher.proxier
    dispatcher.start()

    regs = proxier.read_holding_registers(2, 2)
    print(f"regs: {regs}")
    assert len(regs) == 2 and regs[0] == 17 and regs[1] == 17 # In `server_async.py`, values in 2,3 are both 17 by default.


    dispatcher.push(2, "苹果")
    time.sleep(1)
    dispatcher.stop()
    dispatcher.join()

    txt = "苹果"
    regs = ModbusTcpClient.convert_to_registers(txt, ModbusTcpClient.DATATYPE.STRING)
    print(f"regs: {regs}")
    val = proxier.read_holding_registers_str(2, len(regs))
    print(f"Val: {val}")
    assert val == txt


    proxier.write_registers(2, [17, 17])
    regs = proxier.read_holding_registers(2, 2)
    print(f"regs: {regs}")
    assert len(regs) == 2 and regs[0] == 17 and regs[1] == 17

    pass

if __name__ == "__main__":
    main()
