use crate::error::AppResult;
use crate::ingest::datamap::ctx::MapCtx;
use crate::ingest::datamap::event::MapEnvelope;
use crate::ingest::datamap::event::MarketEvent;

pub trait FromJsonStr: Sized {
    fn from_json_str(s: &str) -> AppResult<Self>;
}

pub trait MapToEvents {
    fn map_to_events(self, ctx: &MapCtx, env: Option<MapEnvelope>) -> AppResult<Vec<MarketEvent>>;
}
