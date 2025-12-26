use std::fmt::Display;

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

    #[serde(rename = "TC")]
    pub tot_channels: usize,

    #[serde(rename = "PCP")]
    pub positive_consencutive_points: Option<usize>,

    #[serde(rename = "NCP")]
    pub negative_consencutive_points: Option<usize>,
}

impl ClientDataReq {
    pub fn pos_stride(&self) -> usize {
        self.tot_channels * self.positive_consencutive_points()
    }

    pub fn neg_stride(&self) -> usize {
        self.tot_channels * self.negative_consencutive_points()
    }

    pub fn positive_consencutive_points(&self) -> usize {
        self.positive_consencutive_points
            .unwrap_or(self.positive_data_per_channel_length)
    }

    pub fn negative_consencutive_points(&self) -> usize {
        self.negative_consencutive_points
            .unwrap_or(self.negative_data_per_channel_length)
    }

    pub fn get_pos_data_start(&self) -> usize {
        self.positive_data_start
    }

    pub fn get_neg_data_start(&self) -> usize {
        self.negative_data_start
    }
}

impl Display for ClientDataReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
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

#[cfg(test)]
mod test {
    use crate::net::protocol::ClientDataReq;

    #[test]
    fn test_client_data_req() {
        let jsos_str = r#"
        {
            "CS": 0,
            "CE": 43,
            "B": 32,
            "PDS": 12,
            "NDS": 1999,
            "PDCL": 18,
            "NDCL": 6,
            "UN": false
        }"#;

        let v: ClientDataReq = serde_json::from_str(jsos_str).unwrap();
        println!("{v:?}");

        let jsos_str = r#"
        {
            "CS": 0,
            "CE": 43,
            "B": 32,
            "PDS": 12,
            "NDS": 1999,
            "PDCL": 18,
            "NDCL": 6,
            "UN": false,
            "PSD": 100
        }"#;

        let v: ClientDataReq = serde_json::from_str(jsos_str).unwrap();
        println!("{v:?}");
    }
}
