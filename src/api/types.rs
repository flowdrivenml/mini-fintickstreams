use crate::app::state::StreamKnobs;
use crate::app::stream_types::{ExchangeId, StreamKind, StreamTransport};
use crate::ingest::instruments::spec::InstrumentKind;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
pub struct AddStreamRequest {
    pub exchange: ExchangeId,
    pub transport: StreamTransport,
    pub kind: StreamKind,
    pub symbol: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RemoveStreamRequest {
    pub exchange: ExchangeId,
    pub transport: StreamTransport,
    pub kind: StreamKind,
    pub symbol: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListStreamsQuery {
    pub exchange: Option<String>,
    pub symbol: Option<String>,
    pub kind: Option<StreamKind>,
    pub transport: Option<StreamTransport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamRow {
    pub id: String,
    pub status: String,

    pub exchange: String,
    pub symbol: String,
    pub kind: StreamKind,
    pub transport: StreamTransport,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthResp {
    pub ok: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct KnobsResp {
    pub knobs: StreamKnobs,
}

/// PATCH semantics: only set fields that are Some(...)
#[derive(Debug, Clone, Deserialize)]
pub struct PatchKnobsRequest {
    pub disable_db_writes: Option<bool>,
    pub disable_redis_publishes: Option<bool>,
    pub flush_rows: Option<usize>,
    pub flush_interval_ms: Option<u64>,
    pub chunk_rows: Option<usize>,
    pub hard_cap_rows: Option<usize>,
}

// --------------------
// Streams
// --------------------

#[derive(Debug, Clone, Serialize)]
pub struct StreamStatusResp {
    pub id: String,
    pub status: String,
    pub spec: StreamSpecDto,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamSpecDto {
    pub exchange: String,
    pub symbol: String,
    pub kind: String,
    pub transport: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamCountResp {
    pub count: usize,
}

// Path params: /streams/:exchange/:symbol/:kind/:transport
#[derive(Debug, Clone, Deserialize)]
pub struct StreamPath {
    pub exchange: String,
    pub symbol: String,
    pub kind: String,
    pub transport: String,
}

// --------------------
// Instruments
// --------------------

#[derive(Debug, Clone, Deserialize)]
pub struct InstrumentsQuery {
    pub exchange: Option<String>,
    pub kind: Option<InstrumentKind>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InstrumentsCountResp {
    pub count: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InstrumentExistsQuery {
    pub exchange: String,
    pub symbol: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct InstrumentExistsResp {
    pub exists: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct AvailableStreamDto {
    pub exchange: String,
    pub transport: String,
    pub kind: String,
    pub note: Option<&'static str>,
}
