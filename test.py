import sys
from collections import namedtuple, deque
import yaml
import pymodbus
from pymodbus.client import ModbusTcpClient

def main():
    # IP: 192.168.27.123，192.168.27.124，端口：5003。
    client = ModbusTcpClient("localhost", port=5050, framer=pymodbus.Framer.SOCKET)
    if not client.connect():
        print("Failed to connect to the server.")
        exit(1)


    payload = []
    payload_hex = "31 35 20 20 20 20 00 02 D5 FD D4 DA BC EC B3 B5 00 02 20 20 20 20 B3 B5 C1 BE D5 FD D4 DA BC EC B2 E2 A3 AC C7 EB D2 C0 B4 CE B4 F2 BF AA B3 B5 B5 C6 20 20 20 20 20 20 20 20 00 02 D3 D0 00 02 D3 D0 00 02 D3 D0 00 02 D3 D0 00 02 D7 F3 C1 C1 20 20 00 02 D3 D2 C1 C1 20 20 00 02 32 30 20 20 20 20 00 02 D7 F3 B2 BB C1 C1 00 01 D3 D2 B2 BB C1 C1 00 01 B2 BB C9 C1 CB B8 00 01 D7 F3 C1 C1 20 20 00 02 D3 D2 B2 BB C1 C1 00 01 C1 C1 C6 F0 20 20 00 02"
    payload_bin = bytes.fromhex(payload_hex)
    for i in range(len(payload_bin)//2):
        payload.append(int.from_bytes(bytes(payload_bin[i*2:i*2+2]), byteorder="big"))
    rr = client.write_registers(0, payload, 1)
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



    client.close()
    pass

if __name__ == "__main__":
    main()
