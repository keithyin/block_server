use tokio::{io::AsyncReadExt, net::TcpStream};

/// instrument 2 block server
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ControlInfo {
    // instrument 2 block server
    pub command: String, // "stop", "resume", "file_ready", "serving_files", "read_file"
    pub fpath: Option<String>,
    pub start_channel: Option<u32>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ControlResponse {
    pub status: String, // "ok", "error"
    pub message: Option<String>,
}

impl ControlResponse {
    pub fn new(status: String, message: Option<String>) -> Self {
        ControlResponse { status, message }
    }
}


#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ServedFilesResp {
    pub status: String,
    pub files: Vec<String>,
}

impl ServedFilesResp {
    pub fn new(status: String, files: Vec<String>) -> Self {
        ServedFilesResp { status, files }
    }   
}

/// control message. 4bytes for length, and following the json bytes
pub async fn extract_control_info<T>(stream: &mut TcpStream) -> anyhow::Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf);

    let mut msg_buf = vec![0u8; len as usize];
    stream.read_exact(&mut msg_buf).await?;

    serde_json::from_slice::<T>(&msg_buf).map_err(|e| anyhow::anyhow!(e))
}
