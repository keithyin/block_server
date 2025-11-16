use std::io;


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
        

        Ok(Self { fpath, channel_range: String::new(), fd })
    }

}