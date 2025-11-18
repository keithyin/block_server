use std::{
    io,
    sync::{Arc, Mutex, OnceLock, atomic::AtomicBool},
};

use anyhow::Context;
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
            let _ = control_msg_processor(socket).await;
        });
    }
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
        "file_ready" => {
            let fpath = control_info.fpath.unwrap();
            let mut served_files = SERVED_FILES.get().unwrap().lock().unwrap();
            served_files.push(fpath);
        }
        "serving_files" => {}
        _ => {
            tracing::warn!("Unknown command received: {:?}", control_info);
        }
    }

    match control_info.command.as_str() {
        "stop" | "resume" | "file_ready" => {
            let resp_msg = ControlResponse::new("ok".to_string(), None);
            let resp_bytes = serde_json::to_vec(&resp_msg).unwrap();
            socket
                .write_all(&(resp_bytes.len() as u32).to_be_bytes())
                .await?;
            socket.write_all(&resp_bytes).await?;
        }

        "serving_files" => {
            let resp_msg = {
                let served_files = SERVED_FILES.get().unwrap().lock().unwrap();
                block_server::net::ServedFilesResp::new("ok".to_string(), served_files.clone())
            };
            let resp_bytes = serde_json::to_vec(&resp_msg).unwrap();
            socket
                .write_all(&(resp_bytes.len() as u32).to_be_bytes())
                .await?;
            socket.write_all(&resp_bytes).await?;
        }
        _ => {
            // tracing::warn!("Unknown command received: {:?}", control_info);
        }
    }

    Ok(())
}

async fn data_msg_listener() -> anyhow::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:30002").await?;
    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                tokio::spawn(async move {
                    match data_msg_processor(socket).await {
                        Ok(_) => {},
                        Err(err) => {
                            tracing::error!("data msg processor error: {:?}", err);
                        }
                    }
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
        let size = f.read_buf(&mut buf).await.context("read file error")?;
        if size == 0 {
            tracing::info!("file send done");
            break;
        }
        socket.write_all(&buf[..size]).await.context("write socket error")?;

        let mut resp_bytes = [0_u8; 1];
        socket.read_exact(&mut resp_bytes).await.context("wait response error")?;

        // TODO receive the ack from compute server
    }

    Ok(())
}
fn main() -> io::Result<()> {
    tracing_subscriber::fmt::init();

    SERVED_FILES.set(Arc::new(Mutex::new(Vec::new()))).unwrap();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_io()
        .build()?;

    rt.block_on(async {
        let _ = tokio::join!(control_msg_listener(), data_msg_listener());
    });
    Ok(())
}
