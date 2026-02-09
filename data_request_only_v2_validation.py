import socket
import json
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


def tcp_yield_data(server_host, filepath):

    # 创建 TCP socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # 服务器地址和端口
    # server_host = "192.168.3.55"
    # server_host = '127.0.0.1'  # 本地回环地址
    server_port = 30002

    try:
        # 连接服务器
        print(f"正在连接服务器 {server_host}:{server_port}...")
        client_socket.connect((server_host, server_port))
        print("连接成功!")

        # client send file request to block_server
        req_data = {
            "FP": filepath
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
        meta_info = json.loads(meta_info.decode("utf-8"))

        # client send data request to block_server
        c_start = 1
        data_req = {
            "CS": c_start,  # channel start。请求的 channel 起始
            "CE": meta_info["numChannels"],  # channel end。请求的 channel 结束。
            "B": 1,  # batch size, 文件服务一次性 返回多少 channel 的数据
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
            "UN": False,  # use negative data. 如果为 True, 则返回 负向电流数据，否则不返回
            "TC": meta_info["numChannels"],
            "PCP": meta_info["posConsecutivePoints"],
            "NCP": meta_info["negConsecutivePoints"]
        }
        data_req_bytes = json.dumps(data_req).encode("utf-8")
        client_socket.sendall(
            len(data_req_bytes).to_bytes(4, byteorder="little"))
        client_socket.sendall(data_req_bytes)

        # reciever the channel raw signal data from block_server
        channel_cursor = c_start
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
            positive_data = read_exact(client_socket, positive_data_length)
            yield positive_data
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
    ip1 = "192.168.3.55"
    ip2 = "127.0.0.1"

    fpath = "/data1/raw-signal-data/20250829_250302Y0003_Run0011_00_pk0001.bin-2"

    iter1 = tcp_yield_data(ip1, fpath)
    iter2 = tcp_yield_data(ip2, fpath)

    while True:
        data1 = next(iter1)
        data2 = next(iter2)

        if data1 != data2:
            raise ValueError("not equal")
