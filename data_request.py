import socket
import json


def read_exact(sock: socket.socket, n: int) -> bytes:
    buf = b''
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
    server_host = '192.168.9.10'
    # server_host = '127.0.0.1'  # 本地回环地址
    server_port = 30002

    try:
        # 连接服务器
        print(f"正在连接服务器 {server_host}:{server_port}...")
        client_socket.connect((server_host, server_port))
        print("连接成功!")

        # client send file request to block_server
        req_data = {
            "FP": "/data1/20250724_250302Y0004_Run0001_adapter.bam"
        }
        req_bytes = json.dumps(req_data).encode("utf-8")
        bytes_len = len(req_bytes)
        client_socket.sendall(bytes_len.to_bytes(4, byteorder="big"))
        client_socket.sendall(req_bytes)

        # get meta info from server
        meta_len_bytes = read_exact(client_socket, 4)
        meta_len = int.from_bytes(meta_len_bytes, byteorder="little")
        meta_info = read_exact(client_socket, meta_len)

        # client send data request to block_server
        data_req = {
            "CS": 0,      # channel start。请求的 channel 起始
            "CE": 1000,   # channel end。请求的 channel 结束。
            "B": 128,  # batch size, 文件服务一次性 返回多少 channel 的数据
            "PDS": 2048,  # positive data start. 对应 posDataStart
            "NDS": 204800,  # negative data start. 对应 negDataStart
            "PDCL": 300000,  # 单channel的正向电流点数，对应 posChannelPoints
            "NDCL": 37500,   # 单channel的负向电流点数，对应 posChannelPoints
            "UN": True, # use negative data. 如果为 True, 则返回 负向电流数据，否则不返回
        }
        data_req_bytes = json.dumps(data_req).encode("utf-8")
        client_socket.sendall(data_req_bytes)
        

        # reciever the channel raw signal data from block_server
        while True:
            meta_len_bytes = read_exact(client_socket, 4)
            meta_len = int.from_bytes(meta_len_bytes, byteorder="little")
            meta_info_bytes = read_exact(client_socket, meta_len)
            meta_info = json.loads(meta_info_bytes.decode("utf-8"))
            if meta_info["NC"] == 0:
                break
            positive_data_length = meta_info["PDL"]
            negative_data_lenth = meta_info["NDL"] # 仅对于 UN is True 时生效
            positive_data = read_exact(client_socket, positive_data_length)
            negative_data = read_exact(client_socket, negative_data_lenth)
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
