use std::{ cell::OnceCell, collections::HashMap, io, sync::{Arc, Mutex, OnceLock, atomic::AtomicBool}
};

use block_server::net::{FileReadyInfo, InstrumentControlInfo};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream}, 
};

pub static READ_FLAG: AtomicBool = AtomicBool::new(true);
pub static ID2FPATH: OnceLock<Arc<Mutex<HashMap<String, String>>>> = OnceLock::new();

async fn notify_compute_server(file_ready_info: &FileReadyInfo) -> io::Result<()> {
    let src = serde_json::to_vec(file_ready_info).unwrap();

    Ok(())
}

async fn block_request_listener() -> io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:30001").await?;

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            block_request_processor(socket).await;
        });
    }
}

async fn block_request_processor(socket: TcpStream) {
    // open the file, and prepare to send data back
    loop {
        // wait the request from compute server

        // send the result

        // if the job is done, break
    }
}

async fn instrument_message_listener() -> io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:30001").await?;

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move {

            // instrument_message_processor(socket).await;
        });
    }
}

async fn instrument_message_processor(
    mut socket: TcpStream,
    compute_server_ip: &str,
) -> io::Result<()> {
    let mut len_buf = [0u8; 4];
    let _ = socket.read_exact(&mut len_buf).await?;
    let msg_len = u32::from_be_bytes(len_buf) as usize;
    let mut msg_buf = vec![0u8; msg_len];
    socket.read_exact(&mut msg_buf).await?;

    let msg: InstrumentControlInfo = serde_json::from_slice(&msg_buf).unwrap();
    match msg.command.as_str() {
        "stop" => {
            // stop the file transfer server
            READ_FLAG.store(false, std::sync::atomic::Ordering::Relaxed);
            tracing::info!("block server stop");
        }
        "resume" => {
            // resume the file transfer server
            READ_FLAG.store(true, std::sync::atomic::Ordering::Relaxed);
            tracing::info!("block server resume");
        }
        "data" => {
            let fpath = msg.fpath.unwrap();
            ID2FPATH.get().unwrap().lock().unwrap().insert(fpath.clone(), fpath.clone());
            notify_compute_server(&FileReadyInfo {
                id: fpath,
                channel_range: "0-1024".to_string(),
            })
            .await?;

            // process the data file
        }
        _ => {}
    }

    Ok(())
}


fn main() {


    ID2FPATH.get_or_init(|| {
        Arc::new(Mutex::new(HashMap::new()))
    });

    
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        println!("hello");
    })
}
