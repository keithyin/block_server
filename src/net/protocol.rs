#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct ClientFpReq {
    #[serde(rename = "FP")]
    pub filepath: String,
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct ClientDataReq {
    #[serde(rename = "CS")]
    pub channel_start: usize,
    #[serde(rename = "CE")]
    pub channel_end: usize,
    #[serde(rename = "B")]
    pub batch_size: usize,
    #[serde(rename = "PDS")]
    pub positive_data_start: usize,
    #[serde(rename = "NDS")]
    pub negative_data_start: usize,
    #[serde(rename = "PDCL")]
    pub positive_data_per_channel_length: usize,
    #[serde(rename = "NDCL")]
    pub negative_data_per_channel_length: usize,
    #[serde(rename = "UN")]
    pub use_negative: bool,
}

impl ClientDataReq {
    pub fn get_pos_data_start(&self) -> usize {
        self.positive_data_start + self.channel_start * self.positive_data_per_channel_length
    }

    pub fn get_neg_data_start(&self) -> usize {
        self.negative_data_start + self.channel_start * self.negative_data_per_channel_length
    }
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct DataMetaResp {
    #[serde(rename = "CS")]
    pub channel_start: usize,
    #[serde(rename = "NC")]
    pub num_channels: usize,
    #[serde(rename = "PDL")]
    pub positive_data_length: usize,
    #[serde(rename = "NDL")]
    pub negative_data_length: usize,
}
