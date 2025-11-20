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

    fn get_db_path(&self) -> String {
        self.db_path
            .as_ref()
            .unwrap_or(&format!("serving_files.db"))
            .to_string()
    }
}

async fn control_msg_listener(
    serving_files_db: Arc<block_server::db::ServingInfoDb>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:30001").await?;
    tracing::info!("control msg processor listening on 0.0.0.0:30001");

    loop {
        let (socket, _) = listener.accept().await?;
        let serving_files_db = serving_files_db.clone();
        tokio::spawn(async move {
            let _ = control_msg_processor(socket, serving_files_db).await;
        });
    }
}

async fn control_msg_processor(
    socket: TcpStream,
    serving_files_db: Arc<block_server::db::ServingInfoDb>,
) -> anyhow::Result<()> {
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
            serving_files_db.update_file_tag_for_reading(&fpath).await?;
        }
        "new_arrived_files" => {}

        "delete" => {
            let fpath = control_info.fpath.unwrap();

            serving_files_db
                .update_file_tag_for_deletion(&fpath)
                .await?;

            // if let Ok(exist) = tokio::fs::try_exists(&fpath).await {
            //     if exist {
            //         tokio::fs::remove_file(&fpath).await?;
            //         tracing::info!("file deleted: {}", fpath);
            //     } else {
            //         tracing::info!("file not exist: {}", fpath);
            //     }
            // }
        }

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

async fn data_msg_listener(
    serving_files_db: Arc<block_server::db::ServingInfoDb>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:30002").await?;
    tracing::info!("data msg processor listening on 0.0.0.0:30002");
    loop {
        let serving_files_db = serving_files_db.clone();
        match listener.accept().await {
            Ok((socket, _)) => {
                tokio::spawn(async move {
                    match data_msg_processor(socket, serving_files_db).await {
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

async fn data_msg_processor(
    mut socket: TcpStream,
    serving_files_db: Arc<block_server::db::ServingInfoDb>,
) -> anyhow::Result<()> {
    let control_msg = extract_control_info::<ControlInfo>(&mut socket).await?;

    let mut f = tokio::fs::File::open(control_msg.fpath.as_ref().unwrap()).await?;
    serving_files_db
        .update_file_tag_for_reading(control_msg.fpath.as_ref().unwrap())
        .await?;

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

async fn delete_file_daemon(
    serving_files_db: Arc<block_server::db::ServingInfoDb>,
) -> anyhow::Result<()> {
    loop {
        let files_to_delete = serving_files_db.get_files_for_deletion().await?;
        for fpath in files_to_delete {
            if let Ok(exist) = tokio::fs::try_exists(&fpath).await {
                if exist {
                    tokio::fs::remove_file(&fpath).await?;
                    tracing::info!("file deleted: {}", fpath);
                } else {
                    tracing::info!("file not exist: {}", fpath);
                }
                serving_files_db.remove_file_record(&fpath).await?;
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    }
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
        .build()?;

    let db_path = cli.get_db_path();

    rt.block_on(async move {
        let db = block_server::db::ServingInfoDb::new(&db_path)
            .await
            .unwrap();
        let db = Arc::new(db);
        let _ = tokio::join!(
            control_msg_listener(db.clone()),
            data_msg_listener(db.clone()),
            delete_file_daemon(db.clone())
        );
    });
    Ok(())
}
