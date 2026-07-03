use crate::args::Args;
use anyhow::{anyhow, Result};
use log::{debug, info};
use serde::Deserialize;
use tokio::fs;

// ---------- JSON 数据结构 ----------
#[derive(Debug, Deserialize)]
struct JsonChannel {
    channel_id: String,
    name: String,
    user_channel_id: Option<String>,
    multicast_url: String,
    timeshift_support: bool,
    timeshift_length: Option<i64>,
    timeshift_url: Option<String>,
    channel_fcc_ip: Option<String>,
    channel_fcc_port: Option<String>,
    channel_fec_port: Option<String>,
    category: Option<String>,
}

// ---------- 内部 Channel 结构 ----------
pub(crate) struct Channel {
    pub(crate) id: u64,
    pub(crate) name: String,
    pub(crate) rtsp: String,
    pub(crate) igmp: Option<String>,
    pub(crate) epg: Vec<Program>,
    pub(crate) time_shift_url: Option<String>,
    pub(crate) group: Option<String>,
}

pub(crate) struct Program {
    pub(crate) start: i64,
    pub(crate) stop: i64,
    pub(crate) title: String,
    pub(crate) desc: String,
}

// ---------- 核心函数 ----------
pub(crate) async fn get_channels(
    args: &Args,
    _need_epg: bool,
    scheme: &str,
    host: &str,
) -> Result<Vec<Channel>> {
    let json_str = if let Some(url) = &args.channel_list_url {
        info!("Loading channels from URL: {}", url);
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()?;
        let response = client.get(url).send().await?;
        if !response.status().is_success() {
            return Err(anyhow!("HTTP error: {}", response.status()));
        }
        response.text().await?
    } else {
        info!("Loading channels from file: {}", args.channel_list_path);
        fs::read_to_string(&args.channel_list_path).await?
    };

    let json_channels: Vec<JsonChannel> = serde_json::from_str(&json_str)?;
    info!("Loaded {} channels", json_channels.len());

    let global_fcc = calculate_global_fcc(&json_channels);
    debug!("Global fcc: {:?}", global_fcc);

    let mut channels = Vec::new();
    for (idx, jc) in json_channels.into_iter().enumerate() {
        let multicast = jc.multicast_url.trim_start_matches("igmp://");
        let mut rtp_url = format!("rtp://{}", multicast);

        let fcc = if let (Some(ip), Some(port)) = (jc.channel_fcc_ip, jc.channel_fcc_port) {
            format!("{}:{}", ip, port)
        } else if let Some(ref g) = global_fcc {
            g.clone()
        } else {
            String::new()
        };
        if !fcc.is_empty() {
            rtp_url.push_str(&format!("?fcc={}&fcc-type=huawei", fcc));
        }

        let fec_port = if let Some(p) = jc.channel_fec_port {
            p
        } else {
            multicast
                .split(':')
                .nth(1)
                .and_then(|p| p.parse::<u16>().ok())
                .map(|p| (p - 1).to_string())
                .unwrap_or_default()
        };
        if !fec_port.is_empty() {
            if rtp_url.contains('?') {
                rtp_url.push_str(&format!("&fec={}", fec_port));
            } else {
                rtp_url.push_str(&format!("?fec={}", fec_port));
            }
        }

        let channel = Channel {
            id: jc.channel_id.parse::<u64>().unwrap_or(idx as u64 + 1),
            name: jc.name.clone(),
            rtsp: jc.timeshift_url.clone().unwrap_or_default(),
            igmp: Some(rtp_url),
            epg: Vec::new(),
            time_shift_url: jc.timeshift_url,
            group: jc.category,
        };
        channels.push(channel);
    }

    Ok(channels)
}

fn calculate_global_fcc(json_channels: &[JsonChannel]) -> Option<String> {
    let mut candidates = Vec::new();
    for c in json_channels {
        if let (Some(ip), Some(port)) = (&c.channel_fcc_ip, &c.channel_fcc_port) {
            candidates.push(format!("{}:{}", ip, port));
            if candidates.len() >= 5 {
                break;
            }
        }
    }
    if candidates.is_empty() {
        return None;
    }
    let first = &candidates[0];
    for c in candidates.iter().skip(1) {
        if c != first {
            log::warn!("Inconsistent fcc addresses found: {} vs {}", first, c);
            break;
        }
    }
    Some(first.clone())
}

pub(crate) async fn get_icon(_args: &Args, _id: &str) -> Result<Vec<u8>> {
    Err(anyhow!("Icons not supported in this modified version"))
}
