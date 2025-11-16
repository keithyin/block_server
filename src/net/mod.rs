
use tokio::{io::AsyncReadExt, net::TcpStream};

/// block server 2 compute server
#[derive(serde::Serialize, serde::Deserialize)]
pub struct FileReadyInfo {
    pub id: String,
    pub channel_range: String, // "0-1023". "0-1023,2048-3071". left closed, right opened
}

/// instrument 2 block server
#[derive(serde::Serialize, serde::Deserialize)]
pub struct InstrumentControlInfo {
    // instrument 2 block server
    pub command: String, // "stop", "resume", "data"
    pub fpath: Option<String>,
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
