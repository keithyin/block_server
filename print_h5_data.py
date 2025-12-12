import h5py

input_h5 = "/data1/raw-signal-data/20250829_250302Y0003_Run0011_00_pk0001.hdf5"
with h5py.File(input_h5, "r") as h5:
    # 读取 dataset
    data_pos = h5["run_data_pos/data_pos"][0:10, :]
    # data_neg = h5["run_data_neg/data_neg"][:]
    print(data_pos)