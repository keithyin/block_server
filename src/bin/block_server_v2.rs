use std::{
    io,
    sync::{Arc, Mutex, OnceLock, atomic::AtomicBool},
};

use anyhow::Context;
use block_server::net::{
    ControlInfo, ControlResponse, extract_meta_info,
    protocol::{ClientDataReq, ClientFpReq},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use affinity;
use clap::{self, Parser};
use num_cpus;

static READ_FLAG: AtomicBool = AtomicBool::new(true);
static SERVED_FILES: OnceLock<Arc<Mutex<Vec<String>>>> = OnceLock::new();

#[derive(Debug, Parser, Clone)]
#[command(version, about, long_about=None)]
struct Cli {
    #[arg(
        long = "cpus",
        help = "cpus. 0-3,5-6  . 0-3 means 0,1,2,3 (4cpus). default: use all cpus"
    )]
    cpus: Option<String>,

    #[arg(long = "db", help = "path to the database file. ???.db")]
    db_path: Option<String>,
}

impl Cli {
    fn cpus(&self) -> Vec<usize> {
        match self.cpus.as_ref() {
            Some(cpu_ids) => cpu_ids
                .trim()
                .split(",")
                .flat_map(|range| {
                    let (left, right) = range
                        .split_once("-")
                        .expect(&format!("unsupported format {}", range));
                    let lower = left.parse::<usize>().unwrap();
                    let upper = right.parse::<usize>().unwrap();
                    (lower..=upper).into_iter()
                })
                .collect(),
            None => (0..(num_cpus::get())).into_iter().collect(),
        }
    }
}

async fn data_msg_listener() -> anyhow::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:30002").await?;
    tracing::info!("data msg processor listening on 0.0.0.0:30002");
    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                tokio::spawn(async move {
                    match data_msg_processor(socket).await {
                        Ok(_) => {}
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
    let file_req_msg = extract_meta_info::<ClientFpReq>(&mut socket).await?;

    let mut f = tokio::fs::File::open(&file_req_msg.filepath).await?;
    let mut file_meta_len_bytes = [0_u8; 4];
    f.read_exact(&mut file_meta_len_bytes).await?;
    let file_meta_len = u32::from_le_bytes(file_meta_len_bytes);
    let mut file_meta_bytes = Vec::with_capacity(file_meta_len as usize);
    f.read_exact(&mut file_meta_bytes).await?;

    socket.write_all(&file_meta_len_bytes).await?;
    socket.write_all(&file_meta_bytes).await?;

    let data_req_msg = extract_meta_info::<ClientDataReq>(&mut socket).await?;
    let mut pos_data_start = data_req_msg.get_pos_data_start();
    let mut neg_data_start = data_req_msg.get_neg_data_start();
    let mut channel_start = data_req_msg.channel_start;
    let buf_size = data_req_msg.batch_size
        * (data_req_msg.positive_data_per_channel_length
            + if data_req_msg.use_negative {
                data_req_msg.negative_data_per_channel_length
            } else {
                0
            });
    
    
    let mut buf: Vec<u8> = Vec::with_capacity(1024 * 1024 * 2);
    loop {
        buf.clear();
        let size = f.read_buf(&mut buf).await.context("read file error")?;
        if size == 0 {
            tracing::info!("file send done");
            break;
        }
        socket
            .write_all(&buf[..size])
            .await
            .context("write socket error")?;

        // receiver ack from compute server
        let mut resp_bytes = [0_u8; 1];
        socket
            .read_exact(&mut resp_bytes)
            .await
            .context("wait response error")?;
    }

    Ok(())
}

fn main() -> io::Result<()> {
    tracing_subscriber::fmt::fmt().with_ansi(false).init();

    let cli = Cli::parse();
    let used_cpus = cli.cpus();
    assert!(used_cpus.len() >= 3, "at least 3 threads");
    tracing::info!("num_cpus: {}", used_cpus.len());
    tracing::info!("cpu_ids: {:?}", used_cpus);

    SERVED_FILES.set(Arc::new(Mutex::new(Vec::new()))).unwrap();

    let non_blocking_threads = 1;
    // file io binding cpu, not ready yet
    tracing::info!(
        "non-blocking threads :{}, blocking threads:{}",
        non_blocking_threads,
        used_cpus.len() - non_blocking_threads
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(non_blocking_threads)
        .enable_io()
        .max_blocking_threads(used_cpus.len() - non_blocking_threads)
        .on_thread_start(move || {
            affinity::set_thread_affinity(used_cpus.clone()).unwrap();
        })
        .enable_time()
        .build()?;

    rt.block_on(async move {
        data_msg_listener().await;
    });
    Ok(())
}
