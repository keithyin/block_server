use std::{
    io,
    sync::{Arc, Mutex, OnceLock, atomic::AtomicBool},
};

use block_server::net::{ControlInfo, ControlResponse, extract_control_info};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

static READ_FLAG: AtomicBool = AtomicBool::new(true);
static SERVED_FILES: OnceLock<Arc<Mutex<Vec<String>>>> = OnceLock::new();

async fn control_msg_listener() -> anyhow::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:30001").await?;

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            control_msg_processor(socket).await;
        });
    }
    Ok(())
}

async fn control_msg_processor(socket: TcpStream) -> anyhow::Result<()> {
    let mut socket = socket;

    let control_info = extract_control_info::<ControlInfo>(&mut socket).await?;

    match control_info.command.as_str() {
        "stop" => {
            READ_FLAG.store(false, std::sync::atomic::Ordering::Relaxed);
        }
        "resume" => {
            READ_FLAG.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        "data_ready" => {}
        "served_files" => {}
        _ => {
            let resp_msg =
                ControlResponse::new("error".to_string(), Some("Unknown command".to_string()));
            let resp_bytes = serde_json::to_vec(&resp_msg).unwrap();
            socket
                .write_all(&(resp_bytes.len() as u32).to_be_bytes())
                .await?;
            socket.write_all(&resp_bytes).await?;
            return Ok(());
        }
    }
    let resp_msg = ControlResponse::new("ok".to_string(), None);
    let resp_bytes = serde_json::to_vec(&resp_msg).unwrap();
    socket
        .write_all(&(resp_bytes.len() as u32).to_be_bytes())
        .await?;
    socket.write_all(&resp_bytes).await?;
    Ok(())
}

async fn data_msg_listener() -> anyhow::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:30002").await?;
    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                tokio::spawn(async move {
                    let _ = data_msg_processor(socket).await;
                });
            }
            Err(e) => {
                eprintln!("Failed to accept connection: {}", e);
            }
        }
    }
}

async fn data_msg_processor(mut socket: TcpStream) -> anyhow::Result<()> {
    let control_msg = extract_control_info::<ControlInfo>(&mut socket).await?;

    let mut f = tokio::fs::File::open(control_msg.fpath.as_ref().unwrap()).await?;

    let mut buf: Vec<u8> = Vec::with_capacity(1024);
    loop {
        buf.clear();
        let size = f.read_buf(&mut buf).await?;
        if size == 0 {
            break;
        }
        socket.write_all(&buf[..size]).await?;

        // TODO receive the ack from compute server
    }

    Ok(())
}
fn main() -> io::Result<()> {
    SERVED_FILES.set(Arc::new(Mutex::new(Vec::new()))).unwrap();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()?;

    rt.block_on(async {
        let _ = tokio::join!(control_msg_listener(), data_msg_listener());
    });
    Ok(())
}
