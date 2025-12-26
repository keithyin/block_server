use std::{
    io,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8},
    },
    time::{Duration, Instant, UNIX_EPOCH},
};

use anyhow::Context;
use block_server::{
    mqtt_last_will,
    net::{
        extract_meta_info,
        protocol::{ClientDataReq, ClientFpReq, DataMetaResp},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter},
    net::{TcpListener, TcpStream},
};

use affinity;
use clap::{self, Parser};
use num_cpus;
use tracing::info_span;

#[derive(Debug, Parser, Clone)]
#[command(version, about, long_about=None)]
struct Cli {
    #[arg(long = "logDir")]
    pub log_dir: Option<String>,

    #[arg(
        long = "cpus",
        help = "cpus. 0-3,5-6  . 0-3 means 0,1,2,3 (4cpus). default: use all cpus"
    )]
    pub cpus: Option<String>,

    #[arg(long="mqttAddr", default_value_t=String::from_str("127.0.0.1").unwrap())]
    pub mqtt_addr: String,

    #[arg(long = "mqttPort", default_value_t = 1883)]
    pub mqtt_port: u16,

    #[arg(long="mqttUsername", default_value_t=String::from_str("software").unwrap())]
    pub mqtt_username: String,
    #[arg(long="mqttPwd", default_value_t=String::from_str("123456").unwrap())]
    pub mqtt_password: String,

    #[arg(long="mqttSoftwareName", default_value_t=String::from_str("block_server").unwrap())]
    pub mqtt_software_name: String,

    #[arg(long="mqttClientId", default_value_t=String::from_str("cid06").unwrap())]
    pub mqtt_client_id: String,
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

    fn get_log_dir(&self) -> Option<&str> {
        match self.log_dir.as_ref() {
            Some(dir) => {
                if !std::path::Path::new(dir).exists() {
                    std::fs::create_dir_all(dir).expect("Failed to create log directory");
                }
                Some(dir)
            }
            None => None,
        }
    }
}

async fn data_msg_listener(
    retry_times: Arc<AtomicU8>,
    app_status: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:30002").await?;
    tracing::info!("data msg processor listening on 0.0.0.0:30002");
    retry_times.store(0, std::sync::atomic::Ordering::SeqCst);
    app_status.store(true, std::sync::atomic::Ordering::SeqCst);

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
                tracing::error!("Failed to accept connection: {}", e);
            }
        }
    }
}

async fn data_msg_processor(mut socket: TcpStream) -> anyhow::Result<()> {
    let file_req_msg = extract_meta_info::<ClientFpReq>(&mut socket).await?;

    let fpath = &file_req_msg.filepath;
    let info_span = info_span!("DataMsgProcessor", %fpath);
    let _guard = info_span.enter();

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
    tracing::info!("MetaStartPos:{file_meta_start_pos:?}");
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
    tracing::info!("FileMetaBytes:{}", file_meta_bytes.len());

    tracing::info!(
        "FileMeta:{:?}",
        String::from_utf8(file_meta_bytes.clone()).map(|v| v.replace("\"", ""))
    );

    socket
        .write_all(&file_meta_bytes)
        .await
        .context("write file meta bytes error")?;
    tracing::info!("write file meta bytes DONE");

    let data_req_msg = extract_meta_info::<ClientDataReq>(&mut socket).await?;

    let channel_end = data_req_msg.channel_end;
    let pos_data_start = data_req_msg.get_pos_data_start();
    let neg_data_start = data_req_msg.get_neg_data_start();
    let mut channel_start = data_req_msg.channel_start;

    let info_span2 = info_span!("DataReq",  %channel_start, %channel_end);
    let _guard2 = info_span2.enter();
    tracing::info!("{}, Start send data", data_req_msg);

    let buf_size = data_req_msg.batch_size
        * (data_req_msg.positive_data_per_channel_length
            + if data_req_msg.use_negative {
                data_req_msg.negative_data_per_channel_length
            } else {
                0
            });

    let mut buf: Vec<u8> = vec![0_u8; buf_size];

    let mut socket = BufWriter::new(socket);

    let mut disk_read_bytes = 0;
    let mut disk_read_elapsed_times = 0;

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

        disk_read_bytes += cur_positive_size;
        let now = Instant::now();
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
        disk_read_elapsed_times += now.elapsed().as_nanos();

        if data_req_msg.use_negative {
            let blocks_per_channel = data_req_msg.negative_data_per_channel_length
                / data_req_msg.negative_consencutive_points();

            let now = Instant::now();
            disk_read_bytes += cur_positive_size;
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
            disk_read_elapsed_times += now.elapsed().as_nanos();
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

    let disk_read_elapsed_times = (disk_read_elapsed_times / 1_000_000) as f64 / 1000.0;

    tracing::info!(
        "Send Done. File Read Speed: {:.4}GiB/s",
        if disk_read_elapsed_times < 1e-6 {
            0.0
        } else {
            disk_read_bytes as f64 / disk_read_elapsed_times / 1024.0 / 1024.0 / 1024.0
        }
    );

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

async fn block_server(retry_times: Arc<AtomicU8>, app_status: Arc<AtomicBool>) {
    loop {
        match data_msg_listener(retry_times.clone(), app_status.clone()).await {
            Ok(_) => {}
            Err(err) => {
                tracing::error!("try: {:?}, err: {}", retry_times, err);
                retry_times.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                app_status.store(false, std::sync::atomic::Ordering::SeqCst);
            }
        }

        tokio::time::sleep(Duration::from_secs(2)).await;

        if retry_times.load(std::sync::atomic::Ordering::SeqCst) > 100 {
            tracing::error!("retry error. break now");
            break;
        }
    }
}

fn main() -> io::Result<()> {
    let time_fmt = time::format_description::parse(
        "[year]-[month padding:zero]-[day padding:zero] [hour]:[minute]:[second]",
    )
    .unwrap();
    let time_offset =
        time::UtcOffset::current_local_offset().unwrap_or_else(|_| time::UtcOffset::UTC);
    let timer = tracing_subscriber::fmt::time::OffsetTime::new(time_offset, time_fmt);

    let cli = Cli::parse();
    let used_cpus = cli.cpus();
    let now = std::time::SystemTime::now();

    let log_dir = cli
        .get_log_dir()
        .map(|dir_path| std::path::Path::new(dir_path));
    let file_name = log_dir.map(|v| {
        v.join(format!(
            "block_server_v2.tt-{}.log",
            now.duration_since(UNIX_EPOCH).unwrap().as_millis()
        ))
    });

    let log_file = file_name.map(|fname| std::fs::File::create(fname).unwrap());

    let _guard = if let Some(file) = log_file {
        let (non_blocking, _guard) = tracing_appender::non_blocking(file);
        tracing_subscriber::fmt::fmt()
            .with_ansi(false)
            .with_writer(non_blocking)
            .with_timer(timer)
            .init();
        Some(_guard)
    } else {
        tracing_subscriber::fmt::fmt()
            .with_ansi(false)
            .with_timer(timer)
            .init();
        None
    };

    assert!(used_cpus.len() >= 3, "at least 3 threads");
    tracing::info!("num_cpus: {}", used_cpus.len());
    tracing::info!("cpu_ids: {:?}", used_cpus);

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

    let app_status = Arc::new(AtomicBool::new(false));
    let retry_times = Arc::new(AtomicU8::new(0));

    let mqtt_addr = cli.mqtt_addr.clone();
    let mqtt_port = cli.mqtt_port;
    let mqtt_software_name = cli.mqtt_software_name.clone();
    let mqtt_client_id = cli.mqtt_client_id.clone();
    let mqtt_username = cli.mqtt_username.clone();
    let mqtt_pwd = cli.mqtt_password.clone();

    rt.block_on(async move {
        tokio::join!(
            block_server(retry_times.clone(), app_status.clone()),
            mqtt_last_will::mqtt_last_will_task(
                app_status.clone(),
                mqtt_addr.as_str(),
                mqtt_port,
                mqtt_software_name.as_str(),
                mqtt_client_id.as_str(),
                mqtt_username.as_str(),
                mqtt_pwd.as_str()
            )
        )
    });
    Ok(())
}
