use actix_web::{
    get,
    http::header,
    web::{Data, Path, Query},
    App, HttpRequest, HttpResponse, HttpServer, Responder,
};
use anyhow::{anyhow, Result};
use argh::FromArgs;
use chrono::{FixedOffset, Local, TimeZone, Utc};
use log::{debug, warn};
use reqwest::Client;
use std::{
    collections::BTreeMap,
    io::{BufWriter, Cursor},
    net::SocketAddrV4,
    process::exit,
    str::FromStr,
    sync::Mutex,
};
use xml::{
    reader::XmlEvent as XmlReadEvent,
    writer::{EmitterConfig, XmlEvent as XmlWriteEvent},
    EventReader,
};

use tokio::task::JoinSet;

mod args;
use args::Args;

mod iptv;
use iptv::{get_channels, Channel};   // 移除未使用的 get_icon

mod proxy;

static OLD_PLAYLIST: Mutex<Option<String>> = Mutex::new(None);
static OLD_XMLTV: Mutex<Option<String>> = Mutex::new(None);

fn to_xmltv_time(unix_time: i64) -> Result<String> {
    match Utc.timestamp_millis_opt(unix_time) {
        chrono::LocalResult::Single(t) => Ok(t
            .with_timezone(&FixedOffset::east_opt(8 * 60 * 60).ok_or(anyhow!(""))?)
            .format("%Y%m%d%H%M%S")
            .to_string()),
        _ => Err(anyhow!("fail to parse time")),
    }
}

fn to_xmltv(channels: Vec<Channel>, extra: Vec<EventReader<Cursor<String>>>) -> Result<String> {
    let mut buf = BufWriter::new(Vec::new());
    let mut writer = EmitterConfig::new()
        .perform_indent(false)
        .create_writer(&mut buf);
    writer.write(
        XmlWriteEvent::start_element("tv")
            .attr("generator-info-name", "iptv-proxy")
            .attr("source-info-name", "iptv-proxy"),
    )?;
    for channel in channels.iter() {
        writer.write(
            XmlWriteEvent::start_element("channel").attr("id", &format!("{}", channel.id)),
        )?;
        writer.write(XmlWriteEvent::start_element("display-name"))?;
        writer.write(XmlWriteEvent::characters(&channel.name))?;
        writer.write(XmlWriteEvent::end_element())?;
        writer.write(XmlWriteEvent::end_element())?;
    }
    // 处理 extra xmltv
    for reader in extra {
        for e in reader {
            match e {
                Ok(XmlReadEvent::StartElement {
                    name, attributes, ..
                }) => {
                    let name = name.to_string();
                    let name = name.as_str();
                    if name != "channel"
                        && name != "display-name"
                        && name != "desc"
                        && name != "title"
                        && name != "sub-title"
                        && name != "programme"
                    {
                        continue;
                    }
                    let name = if name == "title" {
                        let mut iter = attributes.iter();
                        loop {
                            let attr = iter.next();
                            if attr.is_none() {
                                break "title";
                            }
                            let attr = attr.unwrap();
                            if attr.name.to_string() == "lang" && attr.value != "chi" {
                                break "title_extra";
                            }
                        }
                    } else {
                        name
                    };
                    let mut tag = XmlWriteEvent::start_element(name);
                    for attr in attributes.iter() {
                        tag = tag.attr(attr.name.borrow(), &attr.value);
                    }
                    writer.write(tag)?;
                }
                Ok(XmlReadEvent::Characters(content)) => {
                    writer.write(XmlWriteEvent::characters(&content))?;
                }
                Ok(XmlReadEvent::EndElement { name }) => {
                    let name = name.to_string();
                    let name = name.as_str();
                    if name != "channel"
                        && name != "display-name"
                        && name != "desc"
                        && name != "title"
                        && name != "sub-title"
                        && name != "programme"
                    {
                        continue;
                    }
                    writer.write(XmlWriteEvent::end_element())?;
                }
                _ => {}
            }
        }
    }
    for channel in channels.iter() {
        for epg in channel.epg.iter() {
            writer.write(
                XmlWriteEvent::start_element("programme")
                    .attr("start", &format!("{} +0800", to_xmltv_time(epg.start)?))
                    .attr("stop", &format!("{} +0800", to_xmltv_time(epg.stop)?))
                    .attr("channel", &format!("{}", channel.id)),
            )?;
            writer.write(XmlWriteEvent::start_element("title").attr("lang", "chi"))?;
            writer.write(XmlWriteEvent::characters(&epg.title))?;
            writer.write(XmlWriteEvent::end_element())?;
            if !epg.desc.is_empty() {
                writer.write(XmlWriteEvent::start_element("desc"))?;
                writer.write(XmlWriteEvent::characters(&epg.desc))?;
                writer.write(XmlWriteEvent::end_element())?;
            }
            writer.write(XmlWriteEvent::end_element())?;
        }
    }
    writer.write(XmlWriteEvent::end_element())?;
    Ok(String::from_utf8(buf.into_inner()?)?)
}

async fn parse_extra_xml(url: &str) -> Result<EventReader<Cursor<String>>> {
    let client = Client::builder().build()?;
    let url = reqwest::Url::parse(url)?;
    let response = client.get(url).send().await?.error_for_status()?;
    let xml = response.text().await?;
    let reader = Cursor::new(xml);
    Ok(EventReader::new(reader))
}

#[get("/xmltv")]
async fn xmltv(args: Data<Args>, req: HttpRequest) -> impl Responder {
    debug!("Get EPG");
    let scheme = req.connection_info().scheme().to_owned();
    let host = req.connection_info().host().to_owned();
    // parse all extra xmltv URLs in parallel
    let extra_readers = if !args.extra_xmltv.is_empty() {
        let mut set = JoinSet::new();
        for (i, u) in args.extra_xmltv.iter().enumerate() {
            let u = u.clone();
            set.spawn(async move { (i, parse_extra_xml(&u).await) });
        }
        let mut readers = Vec::new();
        while let Some(res) = set.join_next().await {
            match res {
                Ok((_, Ok(reader))) => readers.push(reader),
                Ok((i, Err(e))) => warn!(
                    "Failed to parse extra xmltv ({}): {}",
                    args.extra_xmltv[i], e
                ),
                Err(e) => warn!("Task join error parsing extra xmltv: {}", e),
            }
        }
        readers
    } else {
        Vec::new()
    };
    let xml = get_channels(&args, true, &scheme, &host)
        .await
        .and_then(|ch| to_xmltv(ch, extra_readers));
    match xml {
        Err(e) => {
            if let Some(old_xmltv) = OLD_XMLTV.try_lock().ok().and_then(|f| f.to_owned()) {
                HttpResponse::Ok().content_type("text/xml").body(old_xmltv)
            } else {
                HttpResponse::InternalServerError().body(format!("Error getting channels: {}", e))
            }
        }
        Ok(xml) => HttpResponse::Ok().content_type("text/xml").body(xml),
    }
}

async fn parse_extra_playlist(url: &str) -> Result<String> {
    let client = Client::builder().build()?;
    let url = reqwest::Url::parse(url)?;
    let response = client.get(url).send().await?.error_for_status()?;
    let response = response.text().await?;
    if response.starts_with("#EXTM3U") {
        response
            .find('\n')
            .map(|i| response[i..].to_owned())
            .ok_or(anyhow!("Empty playlist"))
    } else {
        Err(anyhow!("Playlist does not start with #EXTM3U"))
    }
}

#[get("/logo/{id}.png")]
async fn logo(_args: Data<Args>, _path: Path<String>) -> impl Responder {   // 改为 _path
    HttpResponse::NotFound().body("Logo not available")
}

#[get("/playlist")]
async fn playlist(args: Data<Args>, req: HttpRequest) -> impl Responder {
    debug!("Get playlist");
    let scheme = req.connection_info().scheme().to_owned();
    let host = req.connection_info().host().to_owned();
    let user_agent = req
        .headers()
        .get(header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("Unknown");
    let playseek = if user_agent.to_lowercase().contains("kodi") {
        "{utc:YmdHMS}-{utcend:YmdHMS}"
    } else {
        "${(b)yyyyMMddHHmmss}-${(e)yyyyMMddHHmmss}"
    };

    match get_channels(&args, false, &scheme, &host).await {
        Err(e) => {
            if let Some(old_playlist) = OLD_PLAYLIST.try_lock().ok().and_then(|f| f.to_owned()) {
                HttpResponse::Ok()
                    .content_type("application/vnd.apple.mpegurl")
                    .body(old_playlist)
            } else {
                HttpResponse::InternalServerError().body(format!("Error getting channels: {}", e))
            }
        }
        Ok(ch) => {
            let playlist = format!("#EXTM3U x-tvg-url=\"{}://{}/xmltv\"\n", scheme, host)
                + &ch
                    .into_iter()
                    .map(|c| {
                        let group = c.group.as_deref().unwrap_or("其他");
                        // 回看地址：如果存在 time_shift_url，则构造代理地址（用于302重定向）
                        let catch_up = if let Some(ref url) = c.time_shift_url {
                            // 将原始 rtsp:// 替换为 http://代理/rtsp/
                            let proxy_url = url.replacen("rtsp://", &format!("{}://{}/rtsp/", scheme, host), 1);
                            format!(
                                r#" catchup="default" catchup-source="{}&playseek={}""#,
                                proxy_url, playseek
                            )
                        } else {
                            String::new()
                        };
                        let play_url = c.igmp.as_ref().unwrap_or(&c.rtsp);
                        format!(
                            r#"#EXTINF:-1 tvg-id="{0}" tvg-name="{1}" tvg-chno="{0}"{3}tvg-logo="" group-title="{2}",{1}"#,
                            c.id, c.name, group, catch_up
                        ) + "\n" + play_url
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
                + &{
                    if !args.extra_playlist.is_empty() {
                        let mut set = JoinSet::new();
                        for (i, u) in args.extra_playlist.iter().enumerate() {
                            let u = u.clone();
                            set.spawn(async move { (i, parse_extra_playlist(&u).await) });
                        }
                        let mut parts = Vec::new();
                        while let Some(res) = set.join_next().await {
                            match res {
                                Ok((_, Ok(s))) => parts.push(s),
                                Ok((i, Err(e))) => warn!("Failed to parse extra playlist ({}): {}", args.extra_playlist[i], e),
                                Err(e) => warn!("Task join error parsing extra playlist: {}", e),
                            }
                        }
                        parts.join("\n")
                    } else {
                        String::from("")
                    }
                };
            if let Ok(mut old_playlist) = OLD_PLAYLIST.try_lock() {
                *old_playlist = Some(playlist.clone());
            }
            HttpResponse::Ok()
                .content_type("application/vnd.apple.mpegurl")
                .body(playlist)
        }
    }
}

// /rtsp 路由：302 重定向到真实 rtsp 地址
#[get("/rtsp/{tail:.*}")]
async fn rtsp(
    _args: Data<Args>,
    params: Query<BTreeMap<String, String>>,
    req: HttpRequest,
) -> impl Responder {
    let path: String = req.match_info().query("tail").into();
    let mut query = req.query_string().to_string();
    if !params.contains_key("playseek") {
        if params.contains_key("utc") {
            let start = params
                .get("utc")
                .map(|utc| utc.parse::<i64>().expect("Invalid number") * 1000)
                .map(|utc| to_xmltv_time(utc).unwrap())
                .unwrap();
            let end = params
                .get("lutc")
                .map(|lutc| lutc.parse::<i64>().expect("Invalid number") * 1000)
                .map(|lutc| to_xmltv_time(lutc).unwrap())
                .unwrap_or(to_xmltv_time(Local::now().timestamp_millis()).unwrap());
            if !query.is_empty() {
                query.push('&');
            }
            query.push_str(&format!("playseek={}-{}", start, end));
        }
    }
    let real_url = format!("rtsp://{}?{}", path, query);
    HttpResponse::Found()
        .append_header((header::LOCATION, real_url))
        .finish()
}

#[get("/udp/{addr}")]
async fn udp(args: Data<Args>, addr: Path<String>) -> impl Responder {
    let addr = &*addr;
    let addr = match SocketAddrV4::from_str(addr) {
        Ok(addr) => addr,
        Err(e) => return HttpResponse::BadRequest().body(format!("Error: {}", e)),
    };
    HttpResponse::Ok().streaming(proxy::udp(addr, args.interface.clone()))
}

fn usage(cmd: &str) -> std::io::Result<()> {
    let usage = format!(
        r#"Usage: {} [OPTIONS] --user <USER> --passwd <PASSWD> --mac <MAC>

Options:
    -u, --user <USER>                      Login username (ignored)
    -p, --passwd <PASSWD>                  Login password (ignored)
    -m, --mac <MAC>                        MAC address (ignored)
    -i, --imei <IMEI>                      IMEI [default: ]
    -b, --bind <BIND>                      Bind address:port [default: 0.0.0.0:7878]
    -a, --address <ADDRESS>                IP address/interface name [default: ]
    -I, --interface <INTERFACE>            Interface to request
        --extra-playlist <EXTRA_PLAYLIST>  Url to extra m3u
        --extra-xmltv <EXTRA_XMLTV>        Url to extra xmltv
        --udp-proxy                        Use UDP proxy (not recommended)
        --rtsp-proxy                       Use rtsp proxy (deprecated, now redirects)
        --channel-list-path <PATH>         Local path to channel_list.json [default: channel_list.json]
        --channel-list-url <URL>           HTTP URL to channel_list.json (takes precedence over path)
    -h, --help                             Print help
"#,
        cmd
    );
    eprint!("{}", usage);
    exit(0);
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    let args = std::env::args().collect::<Vec<_>>();
    let args = args.iter().map(|s| s.as_str()).collect::<Vec<_>>();
    let args: &[&str] = args.as_ref();
    if args.is_empty() {
        return usage("iptv");
    }
    let args = match Args::from_args(&args[0..1], &args[1..]) {
        Ok(args) => args,
        Err(_) => {
            return usage(args[0]);
        }
    };

    HttpServer::new(move || {
        let args = Data::new(argh::from_env::<Args>());
        App::new()
            .service(xmltv)
            .service(playlist)
            .service(logo)
            .service(rtsp)
            .service(udp)
            .app_data(args)
    })
    .bind(args.bind)?
    .run()
    .await
}
