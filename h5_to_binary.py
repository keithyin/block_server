import h5py
import json
import struct
import numpy as np

input_h5 = "/data1/raw-signal-data/20251124_240601Y0014_Run0003_02_pk0022.hdf5"
output_bin = "/data1/raw-signal-data/20251124_240601Y0014_Run0003_02_pk0022.bin"

with h5py.File(input_h5, "r") as h5:

    # 读取 raw_voltage 下所有属性
    attrs = h5["stimulus_signal/raw_voltage"].attrs
    meta = {k: (v.tolist() if hasattr(v, "tolist") else v) for k, v in attrs.items()}

    # 加入 data shape 信息（未来解析必要）
    data_pos = h5["run_data_pos/data_pos"][:]
    data_neg = h5["run_data_neg/data_neg"][:]
    meta["data_pos_shape"] = data_pos.shape
    meta["data_neg_shape"] = data_neg.shape
    meta["posDataStart"] = 12
    meta["negDataStart"] = int(12 + np.prod(data_pos.shape))
    meta["posChannelPoints"] = int(data_pos.shape[1])
    meta["negChannelPoints"] = int(data_neg.shape[1])


### --- 写入二进制文件 --- ###
with open(output_bin, "wb") as f:

    # 首先预留 meta_start_pos(8) + meta_len(4) 空位
    f.write(b"\x00" * 12)

    # 写入 data_pos 和 data_neg
    f.write(data_pos.tobytes())
    f.write(data_neg.tobytes())

    # 记录 meta JSON 写入位置
    meta_start_pos = f.tell()
    meta_json = json.dumps(meta).encode("utf-8")
    meta_len = len(meta_json)

    # 写 meta JSON
    f.write(meta_json)

    # 回到开头补写 meta_start_pos & meta_len
    f.seek(0)
    f.write(struct.pack("<Q", meta_start_pos))  # u64 LE
    f.write(struct.pack("<I", meta_len))  # u32 LE

print("✔ Binary written:", output_bin)
print(f"meta_start_pos={meta_start_pos},  meta_len={meta_len}")
