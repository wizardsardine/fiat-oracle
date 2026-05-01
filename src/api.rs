use std::sync::{Arc, RwLock};

use axum::{extract::State, http::StatusCode, routing, Json};
use serde::{Deserialize, Serialize};

use crate::{
    cache::{PriceCache, PriceResult},
    currency::Currency,
    state::{ServiceState, Status},
};

#[derive(Debug, Deserialize)]
pub struct PriceRequest {
    pub currency: Currency,
    pub timestamps: Vec<u64>,
}

#[derive(Debug, Serialize)]
pub struct PriceResponse {
    pub prices: Vec<PriceResult>,
}

#[derive(Serialize)]
struct InfoResponse {
    state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    eta_secs: Option<u64>,
    currencies: Vec<String>,
    history_starts: Option<u64>,
}

#[derive(Clone)]
pub struct AppState {
    pub cache: Arc<PriceCache>,
    pub service_state: Arc<RwLock<ServiceState>>,
}

pub async fn price_handler(
    State(state): State<AppState>,
    Json(req): Json<PriceRequest>,
) -> Result<Json<PriceResponse>, (StatusCode, String)> {
    let timestamps: Vec<u64> = req.timestamps.into_iter().take(10_000).collect();
    match state.cache.lookup_batch(req.currency, &timestamps) {
        Ok(prices) => Ok(Json(PriceResponse { prices })),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn info_handler(State(state): State<AppState>) -> Json<InfoResponse> {
    let s = state.service_state.read().expect("poisoned");
    let (phase, eta_secs) = match &s.status {
        Status::Sync { eta_secs, .. } => ("fetching_historical", *eta_secs),
        Status::Serving => ("serving", None),
    };
    Json(InfoResponse {
        state: phase,
        eta_secs,
        currencies: s.currencies.iter().map(|c| c.to_string()).collect(),
        history_starts: s.history_start,
    })
}

pub fn router(state: AppState) -> axum::Router {
    axum::Router::new()
        .route("/price", routing::post(price_handler))
        .route("/info", routing::get(info_handler))
        .route("/healthz", routing::get(|| async { "OK" }))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tempfile::tempdir;
    use tower::ServiceExt;

    use super::*;
    use crate::state::Status;
    use crate::storage::Storage;
    use std::sync::Arc;

    fn make_state(dir: &Path, currencies: &[Currency]) -> AppState {
        let storage = Arc::new(Storage::open(dir, currencies).unwrap());
        let cache = PriceCache::new(currencies, storage);
        let service_state = Arc::new(RwLock::new(ServiceState {
            currencies: currencies.to_vec(),
            history_start: None,
            status: Status::Serving,
        }));
        AppState {
            cache,
            service_state,
        }
    }

    async fn post_price(
        app: axum::Router,
        body: serde_json::Value,
    ) -> (StatusCode, serde_json::Value) {
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/price")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value =
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, json)
    }

    async fn get_info(app: axum::Router) -> (StatusCode, serde_json::Value) {
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/info")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value =
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, json)
    }

    #[tokio::test]
    async fn hit_and_miss() {
        let dir = tempdir().unwrap();
        let state = make_state(dir.path(), &[Currency::Usd]);
        let writer = state.cache.writer(Currency::Usd);
        let ts_hit: u64 = 64 * 10;
        writer.write_price(ts_hit, 50_000.0).expect("write");
        let ts_miss: u64 = 64 * 9_999;

        let app = router(state);
        let (status, resp) = post_price(
            app,
            json!({ "currency": "USD", "timestamps": [ts_hit, ts_miss] }),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        let prices = resp["prices"].as_array().unwrap();
        assert_eq!(prices.len(), 2);
        let val = prices[0]["price"]
            .as_f64()
            .expect("hit should be {\"price\": _}");
        assert!(
            (val - 50_000.0).abs() <= 0.01,
            "expected ~50000.0, got {val}"
        );
        assert_eq!(prices[1], "no_data", "miss should be \"no_data\"");
    }

    #[tokio::test]
    async fn empty_timestamps() {
        let dir = tempdir().unwrap();
        let state = make_state(dir.path(), &[Currency::Usd]);
        let app = router(state);
        let (status, resp) = post_price(app, json!({ "currency": "USD", "timestamps": [] })).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resp["prices"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn clips_to_10000() {
        let dir = tempdir().unwrap();
        let state = make_state(dir.path(), &[Currency::Usd]);
        let app = router(state);
        let timestamps: Vec<u64> = (0u64..11_000).map(|i| i * 64).collect();
        let (status, resp) =
            post_price(app, json!({ "currency": "USD", "timestamps": timestamps })).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resp["prices"].as_array().unwrap().len(), 10_000);
    }

    #[tokio::test]
    async fn malformed_json_returns_400() {
        let dir = tempdir().unwrap();
        let state = make_state(dir.path(), &[Currency::Usd]);
        let app = router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/price")
                    .header("content-type", "application/json")
                    .body(Body::from("not valid json"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn unknown_currency_returns_422() {
        let dir = tempdir().unwrap();
        let state = make_state(dir.path(), &[Currency::Usd]);
        let app = router(state);
        let (status, _) = post_price(app, json!({ "currency": "UNKNOWN", "timestamps": [] })).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn unsupported_currency_returns_200_with_unsupported_slots() {
        // Cache only configured for USD; query EUR returns 200 with
        // every slot reporting "unsupported" in-band.
        let dir = tempdir().unwrap();
        let state = make_state(dir.path(), &[Currency::Usd]);
        let app = router(state);
        let (status, resp) = post_price(
            app,
            json!({ "currency": "EUR", "timestamps": [64, 128, 192] }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let prices = resp["prices"].as_array().unwrap();
        assert_eq!(prices.len(), 3);
        for p in prices {
            assert_eq!(p, "unsupported");
        }
    }

    #[tokio::test]
    async fn info_serving_no_data() {
        let dir = tempdir().unwrap();
        let state = make_state(dir.path(), &[Currency::Usd, Currency::Eur]);
        let app = router(state);
        let (status, resp) = get_info(app).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resp["state"], "serving");
        assert!(
            resp.get("eta_secs").is_none(),
            "eta_secs must be absent when serving"
        );
        assert_eq!(resp["history_starts"], serde_json::Value::Null);
        let currencies = resp["currencies"].as_array().unwrap();
        assert_eq!(currencies.len(), 2);
    }

    #[tokio::test]
    async fn info_serving_with_history() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(Storage::open(dir.path(), &[Currency::Usd]).unwrap());
        let cache = PriceCache::new(&[Currency::Usd], Arc::clone(&storage));
        let ts: u64 = 64 * 100;
        cache
            .writer(Currency::Usd)
            .write_price(ts, 50_000.0)
            .expect("write");
        let history_start = storage.first_timestamp(Currency::Usd).unwrap();
        let service_state = Arc::new(RwLock::new(ServiceState {
            currencies: vec![Currency::Usd],
            history_start,
            status: Status::Serving,
        }));
        let app = router(AppState {
            cache,
            service_state,
        });
        let (status, resp) = get_info(app).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resp["state"], "serving");
        assert!(
            resp["history_starts"].as_u64().is_some(),
            "history_starts should be set"
        );
    }

    #[tokio::test]
    async fn info_syncing() {
        use crate::estimator::BackfillEstimator;
        let dir = tempdir().unwrap();
        let state_inner = ServiceState {
            currencies: vec![Currency::Usd],
            history_start: Some(1_000_000),
            status: Status::Sync {
                total_minutes: 100_000,
                done_minutes: 5_000,
                eta_secs: Some(3600),
                estimator: BackfillEstimator::with_defaults(),
            },
        };
        let service_state = Arc::new(RwLock::new(state_inner));
        let storage = Arc::new(Storage::open(dir.path(), &[Currency::Usd]).unwrap());
        let cache = PriceCache::new(&[Currency::Usd], storage);
        let app = router(AppState {
            cache,
            service_state,
        });
        let (status, resp) = get_info(app).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resp["state"], "fetching_historical");
        assert_eq!(resp["eta_secs"].as_u64(), Some(3600));
        assert_eq!(resp["history_starts"].as_u64(), Some(1_000_000));
    }
}
