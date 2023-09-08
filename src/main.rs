use binance_spot_connector_rust::{
    market::klines::KlineInterval, market_stream::kline::KlineStream,
    tungstenite::BinanceWebSocketClient,
};
use env_logger::Builder;
use serde_json;
use serde::{Deserializer, Serialize, Deserialize, de::{Unexpected, Visitor}};
use std::{fmt, usize};
use std::ptr::null;
use std::fs::{File, OpenOptions};
use std::path::Path;
use chrono;
use csv;
use std::{thread, time};
use chrono::format::parse;
use log;

//const BINANCE_WSS_BASE_URL: &str = "wss://stream.binance.com:9443/ws";

fn main() {
    Builder::from_default_env()
        .filter(None, log::LevelFilter::Debug)
        .init();
    // Establish connection
    //let mut conn = BinanceWebSocketClient::connect_with_url(BINANCE_WSS_BASE_URL).expect("Failed to connect");
    let mut conn = BinanceWebSocketClient::connect().expect("Failed to connect");

    /*
    let mut params_str = SYMBOLS
        .into_iter()
        .map(|symbol| &KlineStream::new(&symbol, KlineInterval::Minutes1).into())
        .collect::<Vec<&Stream>>();
    conn.subscribe(params_str);
     */

    //let mut stream: String = KlineStream::new("ETHBTC", KlineInterval::Minutes1).into();
    //let mut stream_list = vec![];
    //stream_list.push(&stream);
    //let mut stream = KlineStream::new("ETHBTC", KlineInterval::Minutes1).into();
    //for symbol in SYMBOLS {
    //    stream = KlineStream::new(symbol, KlineInterval::Minutes1).into();
    //    stream_list.push(&stream);
    //}
    //stream_list.push(&stream.into());
    //conn.subscribe(stream_list);

    for symbol in SYMBOLS {
        conn.subscribe(vec![
            &KlineStream::new(symbol, KlineInterval::Hours1).into(),
        ]);

        thread::sleep(time::Duration::from_millis(500));
    }

    // Read messages
    while let Ok(message) = conn.as_mut().read_message() {
        let data = message.into_data();
        let string_data = String::from_utf8(data).expect("Found invalid UTF-8 chars");

        let parse_result = serde_json::from_str(&string_data);
        let kline_result: WSKlineHour = match parse_result {
            Ok(kline) => kline,
            Err(_) => {
                println!("kline data cannot parse: {:#?}", string_data);
                continue;
            },
        };
        let result = kline_result.data;
        if !result.data.is_end {
            continue;
        }

        println!("{:#?}", result);

        let time = chrono::offset::Utc::now();
        println!("{:#?}", time);
        let date = time.format("%Y%m%d_utc0_%H").to_string();
        let file_path = format!("out/{}.csv", date);
        //let file_path = "out/".to_owned() + &date + ".csv";
        //println!("{:#?}", file_path);
        let mut is_new_file = false;
        {
            let path = Path::new(&file_path);
            is_new_file = path.exists();
        }

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(file_path)
            .unwrap();
        let mut csv_writer = csv::Writer::from_writer(file);
        //let mut csv_writer = csv::Writer::from_path(file_path).unwrap();
        if !is_new_file {
            csv_writer.write_record(&["ticker", "open_time", "open", "high", "low", "close", "volume", "close_time", "quote_time", "count", "taker_buy_volume", "taker_buy_quote_volume"]).unwrap();
        }
        csv_writer.serialize((result.symbol, result.data.open_time, result.data.first_price, result.data.high, result.data.low, result.data.last_price, result.data.volume, result.data.close_time, result.data.qty, result.data.trade_num, result.data.take_volume, result.data.take_qty)).unwrap();
        csv_writer.flush().unwrap();

    }
    // Disconnect
    conn.close().expect("Failed to disconnect");
}

/*
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WSKlineOut {
    /// 交易对
    pub symbol: String,
    /// 这根K线的起始时间
    pub open_time: i64,
    /// 这根K线开盘价
    pub open: f64,
    /// 这根K线期间最高成交价
    pub high: f64,
    /// 这根K线期间最低成交价
    pub low: f64,
    /// 这根K线收盘价
    pub close: f64,
    /// 这根K线期间成交量
    pub volume: f64,
    /// 这根K线的结束时间
    pub close_time: i64,
    /// 报价币的成交量
    pub quote_volume: f64,
    /// 这根K线期间成交笔数
    pub count: usize,
    /// 这根K线期间吃单方买入的报价币数量
    pub taker_buy_volume: f64,
    /// 这根K线期间吃单方买入的基础币数量
    pub taker_buy_quote_volume: f64,
}
*/

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WSKlineHour {
    /// 订阅类型
    pub stream: String,
    /// 数据
    pub data: WSKline,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WSKline {
    /// 事件类型 kline
    #[serde(rename = "e")]
    pub event_type: String,
    /// 事件时间
    #[serde(rename = "E")]
    pub event_time: i64,
    /// 交易对
    #[serde(rename = "s")]
    pub symbol: String,
    /// K 线数据
    #[serde(rename = "k")]
    pub data: KData,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KData {
    /// 这根K线的起始时间
    #[serde(rename = "t")]
    pub open_time: i64,
    /// 这根K线的结束时间
    #[serde(rename = "T")]
    pub close_time: i64,
    /// 交易对
    #[serde(rename = "s")]
    pub symbol: String,
    /// K线间隔
    #[serde(rename = "i")]
    pub interval: Interval,
    /// 这根K线期间第一笔成交ID
    #[serde(rename = "f")]
    pub first_id: i64,
    /// 这根K线期间末一笔成交ID
    #[serde(rename = "L")]
    pub last_id: i64,
    /// 这根K线期间第一笔成交价
    #[serde(rename = "o", deserialize_with = "string_as_f64")]
    pub first_price: f64,
    /// 这根K线期间末一笔成交价
    #[serde(rename = "c", deserialize_with = "string_as_f64")]
    pub last_price: f64,
    /// 这根K线期间最高成交价
    #[serde(rename = "h", deserialize_with = "string_as_f64")]
    pub high: f64,
    /// 这根K线期间最低成交价
    #[serde(rename = "l", deserialize_with = "string_as_f64")]
    pub low: f64,
    /// 这根K线期间成交量
    #[serde(rename = "v", deserialize_with = "string_as_f64")]
    pub volume: f64,
    /// 这根K线期间成交笔数
    #[serde(rename = "n")]
    pub trade_num: usize,
    /// 这根K线是否完结(是否已经开始下一根K线)
    #[serde(rename = "x")]
    pub is_end: bool,
    /// 这根K线期间成交额
    #[serde(rename = "q", deserialize_with = "string_as_f64")]
    pub qty: f64,
    /// 主动买入的成交量
    #[serde(rename = "V", deserialize_with = "string_as_f64")]
    pub take_volume: f64,
    /// 主动买入的成交额
    #[serde(rename = "Q", deserialize_with = "string_as_f64")]
    pub take_qty: f64,
    /// 忽略此参数
    #[serde(rename = "B")]
    pub __ignore: String,
}

struct F64Visitor;

impl<'de> Visitor<'de> for F64Visitor {
    type Value = f64;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string representation of a f64")
    }
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
    {
        if v.is_empty() {
            Ok(0.0)
        } else {
            v.parse::<f64>().map_err(|_| {
                E::invalid_value(Unexpected::Str(v), &"a string representation as f64")
            })
        }
    }
}

fn string_as_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
    where
        D: Deserializer<'de>,
{
    deserializer.deserialize_str(F64Visitor)
}

struct UsizeVisitor;

impl<'de> Visitor<'de> for UsizeVisitor {
    type Value = usize;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string representation of a usize")
    }
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
    {
        if v.is_empty() {
            Ok(0)
        } else {
            v.parse::<usize>().map_err(|_| {
                E::invalid_value(Unexpected::Str(v), &"a string representation as usize")
            })
        }
    }
}

fn string_as_usize<'de, D>(deserializer: D) -> Result<usize, D::Error>
    where
        D: Deserializer<'de>,
{
    deserializer.deserialize_str(UsizeVisitor)
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Interval {
    #[serde(rename = "1m")]
    Min1,
    #[serde(rename = "3m")]
    Min3,
    #[serde(rename = "5m")]
    Min5,
    #[serde(rename = "15m")]
    Min15,
    #[serde(rename = "30m")]
    Min30,
    #[serde(rename = "1h")]
    Hour1,
    #[serde(rename = "2h")]
    Hour2,
    #[serde(rename = "4h")]
    Hour4,
    #[serde(rename = "6h")]
    Hour6,
    #[serde(rename = "8h")]
    Hour8,
    #[serde(rename = "12h")]
    Hour12,
    #[serde(rename = "1d")]
    Day1,
    #[serde(rename = "3d")]
    Day3,
    #[serde(rename = "1w")]
    Week1,
    #[serde(rename = "1M")]
    Month1,
}
pub const SYMBOLS: [&str; 464] = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "BCCUSDT", "NEOUSDT", "LTCUSDT", "QTUMUSDT", "ADAUSDT", "XRPUSDT", "EOSUSDT", "TUSDUSDT", "IOTAUSDT", "XLMUSDT", "ONTUSDT", "TRXUSDT", "ETCUSDT", "ICXUSDT", "VENUSDT", "NULSUSDT", "VETUSDT", "PAXUSDT", "BCHABCUSDT", "BCHSVUSDT", "USDCUSDT", "LINKUSDT", "WAVESUSDT", "BTTUSDT", "USDSUSDT", "ONGUSDT", "HOTUSDT", "ZILUSDT", "ZRXUSDT", "FETUSDT", "BATUSDT", "XMRUSDT", "ZECUSDT", "IOSTUSDT", "CELRUSDT", "DASHUSDT", "NANOUSDT", "OMGUSDT", "THETAUSDT", "ENJUSDT", "MITHUSDT", "MATICUSDT", "ATOMUSDT", "TFUELUSDT", "ONEUSDT", "FTMUSDT", "ALGOUSDT", "USDSBUSDT", "GTOUSDT", "ERDUSDT", "DOGEUSDT", "DUSKUSDT", "ANKRUSDT", "WINUSDT", "COSUSDT", "NPXSUSDT", "COCOSUSDT", "MTLUSDT", "TOMOUSDT", "PERLUSDT", "DENTUSDT", "MFTUSDT", "KEYUSDT", "STORMUSDT", "DOCKUSDT", "WANUSDT", "FUNUSDT", "CVCUSDT", "CHZUSDT", "BANDUSDT", "BUSDUSDT", "BEAMUSDT", "XTZUSDT", "RENUSDT", "RVNUSDT", "HCUSDT", "HBARUSDT", "NKNUSDT", "STXUSDT", "KAVAUSDT", "ARPAUSDT", "IOTXUSDT", "RLCUSDT", "MCOUSDT", "CTXCUSDT", "BCHUSDT", "TROYUSDT", "VITEUSDT", "FTTUSDT", "BUSDTRY", "USDTTRY", "USDTRUB", "EURUSDT", "OGNUSDT", "DREPUSDT", "BULLUSDT", "BEARUSDT", "ETHBULLUSDT", "ETHBEARUSDT", "TCTUSDT", "WRXUSDT", "BTSUSDT", "LSKUSDT", "BNTUSDT", "LTOUSDT", "EOSBULLUSDT", "EOSBEARUSDT", "XRPBULLUSDT", "XRPBEARUSDT", "STRATUSDT", "AIONUSDT", "MBLUSDT", "COTIUSDT", "BNBBULLUSDT", "BNBBEARUSDT", "STPTUSDT", "USDTZAR", "WTCUSDT", "DATAUSDT", "XZCUSDT", "SOLUSDT", "USDTIDRT", "CTSIUSDT", "HIVEUSDT", "CHRUSDT", "BTCUPUSDT", "BTCDOWNUSDT", "GXSUSDT", "ARDRUSDT", "LENDUSDT", "MDTUSDT", "STMXUSDT", "KNCUSDT", "REPUSDT", "LRCUSDT", "PNTUSDT", "USDTUAH", "COMPUSDT", "USDTBIDR", "BKRWUSDT", "SCUSDT", "ZENUSDT", "SNXUSDT", "ETHUPUSDT", "ETHDOWNUSDT", "ADAUPUSDT", "ADADOWNUSDT", "LINKUPUSDT", "LINKDOWNUSDT", "VTHOUSDT", "DGBUSDT", "GBPUSDT", "SXPUSDT", "MKRUSDT", "DAIUSDT", "DCRUSDT", "STORJUSDT", "BNBUPUSDT", "BNBDOWNUSDT", "XTZUPUSDT", "XTZDOWNUSDT", "USDTBKRW", "MANAUSDT", "AUDUSDT", "YFIUSDT", "BALUSDT", "BLZUSDT", "IRISUSDT", "KMDUSDT", "USDTDAI", "JSTUSDT", "SRMUSDT", "ANTUSDT", "CRVUSDT", "SANDUSDT", "OCEANUSDT", "NMRUSDT", "DOTUSDT", "LUNAUSDT", "RSRUSDT", "PAXGUSDT", "WNXMUSDT", "TRBUSDT", "BZRXUSDT", "SUSHIUSDT", "YFIIUSDT", "KSMUSDT", "EGLDUSDT", "DIAUSDT", "RUNEUSDT", "FIOUSDT", "UMAUSDT", "EOSUPUSDT", "EOSDOWNUSDT", "TRXUPUSDT", "TRXDOWNUSDT", "XRPUPUSDT", "XRPDOWNUSDT", "DOTUPUSDT", "DOTDOWNUSDT", "USDTNGN", "BELUSDT", "WINGUSDT", "LTCUPUSDT", "LTCDOWNUSDT", "UNIUSDT", "NBSUSDT", "OXTUSDT", "SUNUSDT", "AVAXUSDT", "HNTUSDT", "FLMUSDT", "UNIUPUSDT", "UNIDOWNUSDT", "ORNUSDT", "UTKUSDT", "XVSUSDT", "ALPHAUSDT", "USDTBRL", "AAVEUSDT", "NEARUSDT", "SXPUPUSDT", "SXPDOWNUSDT", "FILUSDT", "FILUPUSDT", "FILDOWNUSDT", "YFIUPUSDT", "YFIDOWNUSDT", "INJUSDT", "AUDIOUSDT", "CTKUSDT", "BCHUPUSDT", "BCHDOWNUSDT", "AKROUSDT", "AXSUSDT", "HARDUSDT", "DNTUSDT", "STRAXUSDT", "UNFIUSDT", "ROSEUSDT", "AVAUSDT", "XEMUSDT", "AAVEUPUSDT", "AAVEDOWNUSDT", "SKLUSDT", "SUSDUSDT", "SUSHIUPUSDT", "SUSHIDOWNUSDT", "XLMUPUSDT", "XLMDOWNUSDT", "GRTUSDT", "JUVUSDT", "PSGUSDT", "USDTBVND", "1INCHUSDT", "REEFUSDT", "OGUSDT", "ATMUSDT", "ASRUSDT", "CELOUSDT", "RIFUSDT", "BTCSTUSDT", "TRUUSDT", "CKBUSDT", "TWTUSDT", "FIROUSDT", "LITUSDT", "SFPUSDT", "DODOUSDT", "CAKEUSDT", "ACMUSDT", "BADGERUSDT", "FISUSDT", "OMUSDT", "PONDUSDT", "DEGOUSDT", "ALICEUSDT", "LINAUSDT", "PERPUSDT", "RAMPUSDT", "SUPERUSDT", "CFXUSDT", "EPSUSDT", "AUTOUSDT", "TKOUSDT", "PUNDIXUSDT", "TLMUSDT", "1INCHUPUSDT", "1INCHDOWNUSDT", "BTGUSDT", "MIRUSDT", "BARUSDT", "FORTHUSDT", "BAKEUSDT", "BURGERUSDT", "SLPUSDT", "SHIBUSDT", "ICPUSDT", "ARUSDT", "POLSUSDT", "MDXUSDT", "MASKUSDT", "LPTUSDT", "NUUSDT", "XVGUSDT", "ATAUSDT", "GTCUSDT", "TORNUSDT", "KEEPUSDT", "ERNUSDT", "KLAYUSDT", "PHAUSDT", "BONDUSDT", "MLNUSDT", "DEXEUSDT", "C98USDT", "CLVUSDT", "QNTUSDT", "FLOWUSDT", "TVKUSDT", "MINAUSDT", "RAYUSDT", "FARMUSDT", "ALPACAUSDT", "QUICKUSDT", "MBOXUSDT", "FORUSDT", "REQUSDT", "GHSTUSDT", "WAXPUSDT", "TRIBEUSDT", "GNOUSDT", "XECUSDT", "ELFUSDT", "DYDXUSDT", "POLYUSDT", "IDEXUSDT", "VIDTUSDT", "USDPUSDT", "GALAUSDT", "ILVUSDT", "YGGUSDT", "SYSUSDT", "DFUSDT", "FIDAUSDT", "FRONTUSDT", "CVPUSDT", "AGLDUSDT", "RADUSDT", "BETAUSDT", "RAREUSDT", "LAZIOUSDT", "CHESSUSDT", "ADXUSDT", "AUCTIONUSDT", "DARUSDT", "BNXUSDT", "RGTUSDT", "MOVRUSDT", "CITYUSDT", "ENSUSDT", "KP3RUSDT", "QIUSDT", "PORTOUSDT", "POWRUSDT", "VGXUSDT", "JASMYUSDT", "AMPUSDT", "PLAUSDT", "PYRUSDT", "RNDRUSDT", "ALCXUSDT", "SANTOSUSDT", "MCUSDT", "ANYUSDT", "BICOUSDT", "FLUXUSDT", "FXSUSDT", "VOXELUSDT", "HIGHUSDT", "CVXUSDT", "PEOPLEUSDT", "OOKIUSDT", "SPELLUSDT", "USTUSDT", "JOEUSDT", "ACHUSDT", "IMXUSDT", "GLMRUSDT", "LOKAUSDT", "SCRTUSDT", "API3USDT", "BTTCUSDT", "ACAUSDT", "ANCUSDT", "XNOUSDT", "WOOUSDT", "ALPINEUSDT", "TUSDT", "ASTRUSDT", "GMTUSDT", "KDAUSDT", "APEUSDT", "BSWUSDT", "BIFIUSDT", "MULTIUSDT", "STEEMUSDT", "MOBUSDT", "NEXOUSDT", "REIUSDT", "GALUSDT", "LDOUSDT", "EPXUSDT", "OPUSDT", "LEVERUSDT", "STGUSDT", "LUNCUSDT", "GMXUSDT", "NEBLUSDT", "POLYXUSDT", "APTUSDT", "OSMOUSDT", "HFTUSDT", "PHBUSDT", "HOOKUSDT", "MAGICUSDT", "HIFIUSDT", "RPLUSDT", "PROSUSDT", "AGIXUSDT", "GNSUSDT", "SYNUSDT", "VIBUSDT", "SSVUSDT", "LQTYUSDT", "AMBUSDT", "BETHUSDT", "USTCUSDT", "GASUSDT", "GLMUSDT", "PROMUSDT", "QKCUSDT", "UFTUSDT", "IDUSDT", "ARBUSDT", "LOOMUSDT", "OAXUSDT", "RDNTUSDT", "USDTPLN", "USDTRON", "USDTARS", "WBTCUSDT", "EDUUSDT", "SUIUSDT", "AERGOUSDT", "PEPEUSDT", "FLOKIUSDT", "ASTUSDT", "SNTUSDT", "COMBOUSDT", "MAVUSDT"];

//{"e":"kline","E":1686127026815,"s":"BTCUSDT","k":{"t":1686127020000,"T":1686127079999,"s":"BTCUSDT","i":"1m","f":3136635430,"L":3136635471,"o":"26795.00000000","c":"26795.00000000","h":"26795.00000000","l":"26794.99000000","v":"0.52590000","n":42,"x":false,"q":"14091.48751970","V":"0.22787000","Q":"6105.77665000","B":"0"}}
