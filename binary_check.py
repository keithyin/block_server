import struct, json

with open("output.bin", "rb") as f:
    meta_len = struct.unpack("<I", f.read(4))[0]
    meta = json.loads(f.read(meta_len))
    print(meta)