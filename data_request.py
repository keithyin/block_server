import socket
import json

def tcp_client():
    # 创建 TCP socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # 服务器地址和端口
    server_host = '192.168.9.10'  # 本地回环地址
    # server_host = '127.0.0.1'  # 本地回环地址
    server_port = 30002
    
    try:
        # 连接服务器
        print(f"正在连接服务器 {server_host}:{server_port}...")
        client_socket.connect((server_host, server_port))
        print("连接成功!")
        
        req_data = {
            "command": "read_file",
            "fpath": "/data1/20250724_250302Y0004_Run0001_adapter.bam"
        }
        req_bytes = json.dumps(req_data).encode("utf-8")
        bytes_len = len(req_bytes)
        
        
        client_socket.send(bytes_len.to_bytes(4, byteorder="big"))
        client_socket.send(req_bytes)
        tot_recv = 0
        tt = 0
        while True:
            
            response = client_socket.recv(1024)
            client_socket.send((1).to_bytes(1, byteorder="big") )
            tot_recv += len(response)
            tt += 1
            if tt % 100 == 0:
                print(f"receivered: {tot_recv}")
            if len(response) < 1024:
                break
        print(tot_recv)
            
            
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