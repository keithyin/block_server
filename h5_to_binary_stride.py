import h5py
import json
import struct
import numpy as np
from tqdm import tqdm


def write_strided_data(f, data: np.ndarray, consec_points):
    num_channel, point_in_channel = data.shape
    assert point_in_channel % consec_points == 0
    for block in range(0, point_in_channel // consec_points):
        data_start = block * consec_points
        data_end = data_start + consec_points
        for channel in tqdm(range(0, num_channel), desc=f"writing  block:{block}"):
            f.write(data[channel][data_start:data_end].tobytes())


def main(input_fname, output_fname):
    with h5py.File(input_fname, "r") as h5:

        # 读取 raw_voltage 下所有属性
        attrs = h5["stimulus_signal/raw_voltage"].attrs
        meta = {k: (v.tolist() if hasattr(v, "tolist") else v)
                for k, v in attrs.items()}

        # 加入 data shape 信息（未来解析必要）
        data_pos = h5["run_data_pos/data_pos"][:]
        data_neg = h5["run_data_neg/data_neg"][:]
        meta["data_pos_shape"] = data_pos.shape
        meta["data_neg_shape"] = data_neg.shape
        meta["posDataStart"] = 12
        meta["negDataStart"] = int(12 + np.prod(data_pos.shape))
        meta["posChannelPoints"] = int(data_pos.shape[1])
        meta["negChannelPoints"] = int(data_neg.shape[1])
        meta["numChannels"] = int(data_pos.shape[0])
        meta["posConsecutivePoints"] = 15000
        meta["negConsecutivePoints"] = 1875

    ### --- 写入二进制文件 --- ###
    with open(output_fname, "wb") as f:

        # 首先预留 meta_start_pos(8) + meta_len(4) 空位
        f.write(b"\x00" * 12)

        # 写入 data_pos 和 data_neg

        write_strided_data(
            f, data_pos, consec_points=meta["posConsecutivePoints"])
        write_strided_data(
            f, data_neg, consec_points=meta["negConsecutivePoints"])

        # f.write(data_pos.tobytes())
        # f.write(data_neg.tobytes())

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

        pass


if __name__ == "__main__":
    input_h5 = "/data1/raw-signal-data/20250829_250302Y0003_Run0011_00_pk0001.hdf5"
    output_bin = "/data1/raw-signal-data/20250829_250302Y0003_Run0011_00_pk0001.bin-2"
    main(input_h5, output_bin)
