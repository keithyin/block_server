use std::{
    io,
    sync::{Arc, Mutex, OnceLock, atomic::AtomicU8},
};

use block_server::net::{
    extract_meta_info,
    protocol::{ClientDataReq, ClientFpReq, DataMetaResp},
};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use affinity;
use clap::{self, Parser};
use num_cpus;

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

async fn data_msg_listener(retry_times: Arc<AtomicU8>) -> anyhow::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:30002").await?;
    tracing::info!("data msg processor listening on 0.0.0.0:30002");
    retry_times.store(0, std::sync::atomic::Ordering::SeqCst);
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
    let channel_end = data_req_msg.channel_end;
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

    let mut buf: Vec<u8> = Vec::with_capacity(buf_size);

    loop {
        // read a batch data
        let cur_batch_size = (channel_end - channel_start).min(data_req_msg.batch_size);
        let cur_positive_size = cur_batch_size * data_req_msg.positive_data_per_channel_length;
        let cur_negative_size = if data_req_msg.use_negative {
            cur_batch_size * data_req_msg.negative_data_per_channel_length
        } else {
            0
        };

        f.seek(io::SeekFrom::Start(pos_data_start as u64)).await?;
        f.read_exact(&mut buf[..cur_positive_size]).await?;
        if data_req_msg.use_negative {
            f.seek(io::SeekFrom::Start(neg_data_start as u64)).await?;
            f.read_exact(&mut buf[cur_positive_size..(cur_positive_size + cur_negative_size)])
                .await?;
        }

        let resp_meta = DataMetaResp {
            channel_start: channel_start,
            num_channels: cur_batch_size,
            positive_data_length: cur_positive_size,
            negative_data_length: cur_negative_size,
        };
        let resp_json = serde_json::to_string(&resp_meta)?;
        let resp_json_bytes = resp_json.as_bytes().to_vec();
        socket
            .write_all(&resp_json_bytes.len().to_le_bytes())
            .await?;
        socket.write_all(&resp_json_bytes).await?;
        socket
            .write_all(&buf[..(cur_positive_size + cur_negative_size)])
            .await?;

        channel_start += cur_batch_size;
        pos_data_start += cur_positive_size;
        neg_data_start += cur_negative_size;

        if channel_start >= channel_end {
            break;
        }
    }

    let resp_meta = DataMetaResp {
        channel_start: channel_start,
        num_channels: 0,
        positive_data_length: 0,
        negative_data_length: 0,
    };
    let resp_json = serde_json::to_string(&resp_meta)?;
    let resp_json_bytes = resp_json.as_bytes().to_vec();
    socket
        .write_all(&resp_json_bytes.len().to_le_bytes())
        .await?;
    socket.write_all(&resp_json_bytes).await?;

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
        let retry_times = Arc::new(AtomicU8::new(0));
        loop {
            match data_msg_listener(retry_times.clone()).await {
                Ok(_) => {}
                Err(err) => {
                    tracing::error!("try: {:?}, err: {}", retry_times, err);
                    retry_times.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            }
            if retry_times.load(std::sync::atomic::Ordering::SeqCst) > 20 {
                tracing::error!("retry error. break now");
                break;
            }
        }
    });
    Ok(())
}
