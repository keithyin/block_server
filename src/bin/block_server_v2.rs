use std::{
    io,
    sync::{Arc, Mutex, OnceLock, atomic::AtomicU8},
};

use anyhow::Context;
use block_server::net::{
    extract_meta_info,
    protocol::{ClientDataReq, ClientFpReq, DataMetaResp},
};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter},
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
            Ok((socket, socker_addr)) => {
                tokio::spawn(async move {
                    match data_msg_processor(socket).await {
                        Ok(_) => {}
                        Err(err) => {
                            tracing::error!(
                                "socker_addr: {:?}, data msg processor error: {:?}.",
                                socker_addr,
                                err
                            );
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

    tracing::info!("FileReq:{:?}", file_req_msg);

    let mut f = tokio::fs::File::open(&file_req_msg.filepath)
        .await
        .context(format!("Failed to open file {}", &file_req_msg.filepath))?;

    let mut file_meta_start_pos_bytes = [0_u8; 8];
    f.read_exact(&mut file_meta_start_pos_bytes)
        .await
        .context(format!(
            "read meta start pos bytes error, {}",
            &file_req_msg.filepath
        ))?;

    let file_meta_start_pos = u64::from_le_bytes(file_meta_start_pos_bytes);
    tracing::info!("meta_start_pos:{file_meta_start_pos:?}");
    let mut file_meta_len_bytes = [0_u8; 4];
    f.read_exact(&mut file_meta_len_bytes).await?;
    let file_meta_len = u32::from_le_bytes(file_meta_len_bytes);

    f.seek(io::SeekFrom::Start(file_meta_start_pos)).await?;
    let mut file_meta_bytes = vec![0_u8; file_meta_len as usize];
    f.read_exact(&mut file_meta_bytes[..file_meta_len as usize])
        .await
        .context("read meta error")?;

    socket
        .write_all(&file_meta_len_bytes)
        .await
        .context("write file meta len error")?;
    tracing::info!("file_meta_bytes:{}", file_meta_bytes.len());

    tracing::info!("file_meta:{:?}", String::from_utf8(file_meta_bytes.clone()));
    socket
        .write_all(&file_meta_bytes)
        .await
        .context("write file meta bytes error")?;
    tracing::info!("write file meta bytes DONE");

    let data_req_msg = extract_meta_info::<ClientDataReq>(&mut socket).await?;
    tracing::info!("DataReq: {:?}", data_req_msg);

    let channel_end = data_req_msg.channel_end;
    let pos_data_start = data_req_msg.get_pos_data_start();
    let neg_data_start = data_req_msg.get_neg_data_start();
    let mut channel_start = data_req_msg.channel_start;
    let buf_size = data_req_msg.batch_size
        * (data_req_msg.positive_data_per_channel_length
            + if data_req_msg.use_negative {
                data_req_msg.negative_data_per_channel_length
            } else {
                0
            });

    let mut buf: Vec<u8> = vec![0_u8; buf_size];

    let mut socket = BufWriter::new(socket);

    loop {
        // read a batch data
        let cur_batch_size = (channel_end - channel_start).min(data_req_msg.batch_size);
        let cur_positive_size = cur_batch_size * data_req_msg.positive_data_per_channel_length;

        let cur_pos_data_start: usize =
            pos_data_start + channel_start * data_req_msg.positive_consencutive_points();

        // tracing::info!("cur_pos_data_start:{cur_pos_data_start}. {pos_data_start}, {channel_start}");

        let cur_negative_size = if data_req_msg.use_negative {
            cur_batch_size * data_req_msg.negative_data_per_channel_length
        } else {
            0
        };

        let cur_neg_data_start =
            neg_data_start + channel_start * data_req_msg.negative_consencutive_points();

        let blocks_per_channel = data_req_msg.positive_data_per_channel_length
            / data_req_msg.positive_consencutive_points();

        read_data(
            &mut f,
            &mut buf[..cur_positive_size],
            cur_pos_data_start,
            cur_batch_size,
            data_req_msg.positive_consencutive_points(),
            blocks_per_channel,
            data_req_msg.tot_channels,
        )
        .await?;

        if data_req_msg.use_negative {
            let blocks_per_channel = data_req_msg.negative_data_per_channel_length
                / data_req_msg.negative_consencutive_points();

            read_data(
                &mut f,
                &mut buf[cur_positive_size..(cur_positive_size + cur_negative_size)],
                cur_neg_data_start,
                cur_batch_size,
                data_req_msg.negative_consencutive_points(),
                blocks_per_channel,
                data_req_msg.tot_channels,
            )
            .await?;
        }

        let resp_meta = DataMetaResp {
            channel_start: channel_start,
            num_channels: cur_batch_size,
            positive_data_length: cur_positive_size,
            negative_data_length: cur_negative_size,
        };
        let resp_json = serde_json::to_string(&resp_meta)?;
        // tracing::info!("resp_json:{resp_json}");
        // tracing::info!("resp_json_bytes:{:?}", resp_json.as_bytes());

        socket
            .write_all(&((resp_json.as_bytes().len() as u32).to_le_bytes()))
            .await?;

        socket.write_all(resp_json.as_bytes()).await?;

        socket
            .write_all(&buf[..(cur_positive_size + cur_negative_size)])
            .await?;

        channel_start += cur_batch_size;

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
        .write_all(&(resp_json_bytes.len() as u32).to_le_bytes())
        .await?;
    socket.write_all(&resp_json_bytes).await?;
    socket.flush().await?; // this is important?

    tracing::info!("FileReq:{:?}. Send Done.", file_req_msg);

    Ok(())
}

async fn read_data(
    f: &mut tokio::fs::File,
    buf: &mut [u8],
    mut data_start: usize,
    num_channels: usize,
    channel_consecutive_points: usize,
    blocks_per_channel: usize,
    tot_channels: usize,
) -> anyhow::Result<()> {
    let stride = tot_channels * channel_consecutive_points;

    let points_per_channel = blocks_per_channel * channel_consecutive_points;
    for block_idx in 0..blocks_per_channel {
        let block_shift = block_idx * channel_consecutive_points;
        f.seek(io::SeekFrom::Start(data_start as u64)).await?;

        for channel_idx in 0..num_channels {
            let data_shift = channel_idx * points_per_channel + block_shift;
            // tracing::info!(
            //     "writing to {}-{}",
            //     data_shift,
            //     data_shift + channel_consecutive_points
            // );
            f.read_exact(&mut buf[data_shift..(data_shift + channel_consecutive_points)])
                .await?;
        }
        data_start += stride;
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
