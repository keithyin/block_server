use std::io;
use tokio;

pub async fn check_file_existence(fpath: &str) -> bool {
    match tokio::fs::try_exists(fpath).await {
        Ok(exist) => exist,
        Err(_) => false,
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct RawSignalFileHeader {
    channel_range: String,
}

pub struct RawSignalFile {
    fpath: String,
    channel_range: String,
    fd: tokio::fs::File,
}

impl RawSignalFile {
    pub async fn new(fpath: String) -> io::Result<Self> {
        let mut fd = tokio::fs::File::open(&fpath).await?;

        Ok(Self {
            fpath,
            channel_range: String::new(),
            fd,
        })
    }
}
