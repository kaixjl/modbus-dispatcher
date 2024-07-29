import sys
from collections import namedtuple, deque
import yaml
import pymodbus
from pymodbus.client import ModbusTcpClient

def main():
    client = ModbusTcpClient("localhost", port=5050, framer=pymodbus.Framer.SOCKET)
    if not client.connect():
        print("Failed to connect to the server.")
        exit(1)



    rr = client.read_coils(3, 2, 1)
    if rr.isError():
        print("Error occured while reading coils.")
        exit(1)
    print(f"0x03[0]: {rr.bits[0]}, 0x03[1]: {rr.bits[1]}.")

    rr = client.read_holding_registers(3, 2, 1)
    if rr.isError():
        print("Error occured while reading holding registers.")
        exit(1)
    print(f"0x03: {rr.registers[0]}, 0x04: {rr.registers[1]}.")


    rr = client.write_registers(3, [1, 2], 1)
    if rr.isError():
        print("Error occured while writing registers.")
        exit(1)

    rr = client.read_coils(3, 2, 1)
    if rr.isError():
        print("Error occured while reading coils.")
        exit(1)
    print(f"0x03[0]: {rr.bits[0]}, 0x03[1]: {rr.bits[1]}.")

    rr = client.read_holding_registers(3, 2, 1)
    if rr.isError():
        print("Error occured while reading holding registers.")
        exit(1)
    print(f"0x03: {rr.registers[0]}, 0x04: {rr.registers[1]}.")


    regs = client.convert_to_registers("苹果", client.DATATYPE.STRING)
    l = len(regs)
    print(f"convert_to_registers('苹果', STRING): {regs}")
    print(f"UTF8('苹果'): {'苹果'.encode()}")
    rr = client.write_registers(3, regs, 1)
    if rr.isError():
        print("Error occured while writing registers.")
        exit(1)

    rr = client.read_holding_registers(3, l, 1)
    if rr.isError():
        print("Error occured while reading holding registers.")
        exit(1)
    print(rr.registers)
    val = client.convert_from_registers(rr.registers, client.DATATYPE.STRING)
    print(f"Val: {val}")


    client.close()
    pass

if __name__ == "__main__":
    main()
