import socket
import json
import h5py
import numpy as np


def read_exact(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:  # 对方断开或EOF
            raise ConnectionError(
                "connection closed before enough data was received")
        buf += chunk
    return buf


def tcp_client():

    # 创建 TCP socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # 服务器地址和端口
    server_host = "127.0.0.1"
    # server_host = '127.0.0.1'  # 本地回环地址
    server_port = 30002

    try:
        # 连接服务器
        print(f"正在连接服务器 {server_host}:{server_port}...")
        client_socket.connect((server_host, server_port))
        print("连接成功!")

        # client send file request to block_server
        req_data = {
            "FP": "/data1/raw-signal-data/20251211_240601Y0088_Run0011_07_pk0001.data"
        }
        req_bytes = json.dumps(req_data).encode("utf-8")
        bytes_len = len(req_bytes)
        client_socket.sendall(bytes_len.to_bytes(4, byteorder="little"))
        client_socket.sendall(req_bytes)

        # get meta info from server
        meta_len_bytes = read_exact(client_socket, 4)
        meta_len = int.from_bytes(meta_len_bytes, byteorder="little")
        print(f"meta_len:{meta_len}")
        meta_info = read_exact(client_socket, meta_len)
        
        print("meta_str:'{}'".format(meta_info.decode("utf-8")))
        meta_info = json.loads(meta_info.decode("utf-8"))

        # client send data request to block_server
        data_req = {
            "CS": 5,  # channel start。请求的 channel 起始
            "CE": meta_info["numChannels"],  # channel end。请求的 channel 结束。
            "B": 257,  # batch size, 文件服务一次性 返回多少 channel 的数据
            # positive data start. 对应 posDataStart
            "PDS": meta_info["posDataStart"],
            # negative data start. 对应 negDataStart
            "NDS": meta_info["negDataStart"],
            "PDCL": meta_info[
                "posChannelPoints"
            ],  # 单channel的正向电流点数，对应 posChannelPoints
            "NDCL": meta_info[
                "negChannelPoints"
            ],  # 单channel的负向电流点数，对应 posChannelPoints
            "UN": True,  # use negative data. 如果为 True, 则返回 负向电流数据，否则不返回
            "TC": meta_info["numChannels"],
            "PCP": meta_info["posConsecutivePoints"],
            "NCP": meta_info["negConsecutivePoints"]
        }
        data_req_bytes = json.dumps(data_req).encode("utf-8")
        client_socket.sendall(
            len(data_req_bytes).to_bytes(4, byteorder="little"))
        client_socket.sendall(data_req_bytes)

        # reciever the channel raw signal data from block_server
        channel_cursor = 0
        print("start receving data")
        while True:
            meta_len_bytes = read_exact(client_socket, 4)
            meta_len = int.from_bytes(meta_len_bytes, byteorder="little")
            print(f"data_meta_len:{meta_len}")
            meta_info_bytes = read_exact(client_socket, meta_len)
            print(f"data_meta_info_bytes:{meta_info_bytes}")
            meta_info = json.loads(meta_info_bytes.decode("utf-8"))
            if meta_info["NC"] == 0:
                print("read done")
                break
            positive_data_length = meta_info["PDL"]
            negative_data_lenth = meta_info["NDL"]  # 仅对于 UN is True 时生效
            num_channels = meta_info["NC"]
            positive_data = read_exact(client_socket, positive_data_length)
            
            # print(positive_data[:100])

            positive_data = np.array(list(positive_data), dtype=np.uint8).reshape(
                [num_channels, -1]
            )
            # print(positive_data[])
            break

            negative_data = read_exact(client_socket, negative_data_lenth)
            # negative_data = np.array(list(negative_data), dtype=np.uint8).reshape(
            #     [num_channels, -1]
            # )

            print("check ok")

            channel_cursor += meta_info["NC"]
            # do something

    except ConnectionRefusedError:
        print("连接被拒绝，请检查服务器是否启动")
    except Exception as e:
        print(f"发生错误: {e}")
    finally:
        # 关闭连接
        client_socket.close()
        print("连接已关闭")


if __name__ == "__main__":
    tcp_client()
