import socket
import json
import time
import multiprocessing
from multiprocessing import Process
import sys


def read_exact(sock: socket.socket, n: int) -> bytes:
    """精确读取 n 字节数据"""
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("connection closed before enough data was received")
        buf += chunk
    return buf


def client_worker(server_host, server_port, file_path, process_id, result_queue):
    """
    单个客户端进程的工作函数
    """
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    total_bytes_received = 0
    start_time = time.time()

    try:
        # 连接服务器
        print(f"[Process {process_id}] Connecting to {server_host}:{server_port}...")
        client_socket.connect((server_host, server_port))
        print(f"[Process {process_id}] Connected.")

        # 发送文件路径请求，获取元数据
        req_data = {"FP": file_path}
        req_bytes = json.dumps(req_data).encode("utf-8")
        client_socket.sendall(len(req_bytes).to_bytes(4, byteorder="little"))
        client_socket.sendall(req_bytes)

        # 接收元数据
        meta_len_bytes = read_exact(client_socket, 4)
        meta_len = int.from_bytes(meta_len_bytes, byteorder="little")
        meta_info = json.loads(read_exact(client_socket, meta_len).decode("utf-8"))

        # 构建数据请求（与原逻辑一致）
        data_req = {
            "CS": 1,
            "CE": meta_info["numChannels"],
            "B": 1,
            "PDS": meta_info["posDataStart"],
            "NDS": meta_info["negDataStart"],
            "PDCL": meta_info["posChannelPoints"],
            "NDCL": meta_info["negChannelPoints"],
            "UN": True,
            "TC": meta_info["numChannels"],
            "PCP": meta_info["posConsecutivePoints"],
            "NCP": meta_info["negConsecutivePoints"],
            "EBP": 1,
        }
        data_req_bytes = json.dumps(data_req).encode("utf-8")
        client_socket.sendall(len(data_req_bytes).to_bytes(4, byteorder="little"))
        client_socket.sendall(data_req_bytes)

        # 循环接收数据块
        channel_cursor = 1
        while True:
            meta_len_bytes = read_exact(client_socket, 4)
            meta_len = int.from_bytes(meta_len_bytes, byteorder="little")
            meta_info_block = json.loads(read_exact(client_socket, meta_len).decode("utf-8"))

            if meta_info_block["NC"] == 0:
                print(f"[Process {process_id}] Data reception completed.")
                break

            positive_len = meta_info_block["PDL"]
            negative_len = meta_info_block["NDL"]
            num_channels = meta_info_block["NC"]

            # 接收正负数据并累加字节数
            positive_data = read_exact(client_socket, positive_len)
            negative_data = read_exact(client_socket, negative_len)
            total_bytes_received += positive_len + negative_len

            # 发送确认（可选，根据服务器要求）
            client_socket.send(b'0')

            channel_cursor += num_channels

    except Exception as e:
        print(f"[Process {process_id}] Error: {e}")
    finally:
        client_socket.close()
        end_time = time.time()
        elapsed = end_time - start_time
        rate_mbps = (total_bytes_received * 8) / (elapsed * 1_000_000) if elapsed > 0 else 0
        result_queue.put({
            "pid": process_id,
            "bytes": total_bytes_received,
            "time": elapsed,
            "rate_mbps": rate_mbps
        })
        print(f"[Process {process_id}] Finished: {total_bytes_received/1024/1024:.2f} MB in {elapsed:.2f}s, "
              f"rate = {rate_mbps:.2f} Mbps")


def main():
    # 配置参数
    SERVER_HOST = "192.168.3.55"
    SERVER_PORT = 30002
    FILE_PATH = "/data1/raw-signal-data/20250829_250302Y0003_Run0011_00_pk0001.bin-2"
    NUM_PROCESSES = 10  # 可根据 CPU 核心数和网络条件调整

    result_queue = multiprocessing.Queue()
    processes = []

    print(f"Starting {NUM_PROCESSES} client processes for bandwidth test...")
    for i in range(NUM_PROCESSES):
        p = Process(target=client_worker, args=(SERVER_HOST, SERVER_PORT, FILE_PATH, i, result_queue))
        p.start()
        processes.append(p)

    # 等待所有进程结束
    for p in processes:
        p.join()

    # 汇总统计
    total_bytes = 0
    total_time = 0.0
    print("\n========== Bandwidth Test Summary ==========")
    while not result_queue.empty():
        stat = result_queue.get()
        print(f"Process {stat['pid']}: {stat['bytes']/1024/1024:.2f} MB, "
              f"{stat['time']:.2f}s, {stat['rate_mbps']:.2f} Mbps")
        total_bytes += stat["bytes"]
        # 注意：总耗时不能简单相加，需要根据实际并发时间取最大值或单独计算
        total_time = max(total_time, stat["time"])  # 近似总测试时长

    avg_rate_mbps = (total_bytes * 8) / (total_time * 1_000_000) if total_time > 0 else 0
    print(f"Total data received: {total_bytes/1024/1024:.2f} MB")
    print(f"Total test duration: {total_time:.2f} s")
    print(f"Aggregate bandwidth: {avg_rate_mbps:.2f} Mbps")
    print("============================================")


if __name__ == "__main__":
    # 解决 Windows 下多进程可能的问题（跨平台兼容）
    multiprocessing.freeze_support()
    main()