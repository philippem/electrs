use std::collections::HashMap;
use std::env;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use elements::AssetId;
use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use serde_json::{Map as JsonMap, Value as JsonValue};
use tokio::sync::{oneshot, Mutex, OwnedSemaphorePermit, Semaphore};
use url::Url;

use crate::errors::*;

const DEFAULT_REGISTRY_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_REGISTRY_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const REGISTRY_CONNECT_TIMEOUT_ENV: &str = "ELECTRS_ASSET_REGISTRY_CONNECT_TIMEOUT_MS";
const REGISTRY_REQUEST_TIMEOUT_ENV: &str = "ELECTRS_ASSET_REGISTRY_REQUEST_TIMEOUT_MS";
const REGISTRY_MAX_PAGE_SIZE: usize = 500;
const REGISTRY_MAX_PAGE: usize = 1_000_000;
const REGISTRY_ASSET_CACHE_TTL: Duration = Duration::from_secs(1);
const REGISTRY_ASSET_CACHE_MAX_ENTRIES: usize = 1024;
const REGISTRY_MAX_CONCURRENT_REQUESTS: usize = 16;
const REGISTRY_MAX_ASSET_RESPONSE_SIZE: usize = 1024 * 1024;
const REGISTRY_MAX_LIST_RESPONSE_SIZE: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug)]
pub enum RegistryError {
    InvalidBaseUrl(String),
    InvalidRequest(String),
    Timeout(String),
    Transport(String),
    HttpStatus(u16),
    InvalidResponse(String),
    Overloaded(String),
    MissingLocalAsset(AssetId),
    LocalLookup(String),
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBaseUrl(message)
            | Self::InvalidRequest(message)
            | Self::Timeout(message)
            | Self::Transport(message)
            | Self::InvalidResponse(message)
            | Self::Overloaded(message)
            | Self::LocalLookup(message) => f.write_str(message),
            Self::HttpStatus(status) => {
                write!(f, "asset registry returned HTTP status {}", status)
            }
            Self::MissingLocalAsset(asset_id) => {
                write!(
                    f,
                    "registered asset {} is missing from the local index",
                    asset_id
                )
            }
        }
    }
}

impl std::error::Error for RegistryError {}

type RegistryAssetResult = std::result::Result<Option<RegistryAsset>, RegistryError>;

enum AssetCacheEntry {
    Ready {
        fetched_at: Instant,
        asset: Option<Arc<RegistryAsset>>,
    },
    Fetching(Vec<oneshot::Sender<RegistryAssetResult>>),
}

#[derive(Clone)]
pub struct RegistryClient {
    base_url: Url,
    http: Client,
    asset_cache: Arc<Mutex<HashMap<AssetId, AssetCacheEntry>>>,
    concurrency: Arc<Semaphore>,
    asset_cache_ttl: Duration,
    asset_cache_max_entries: usize,
}

impl RegistryClient {
    pub fn new(base_url: Url) -> std::result::Result<Self, RegistryError> {
        Self::with_timeouts(
            base_url,
            registry_timeout_from_env(
                REGISTRY_CONNECT_TIMEOUT_ENV,
                DEFAULT_REGISTRY_CONNECT_TIMEOUT,
            )?,
            registry_timeout_from_env(
                REGISTRY_REQUEST_TIMEOUT_ENV,
                DEFAULT_REGISTRY_REQUEST_TIMEOUT,
            )?,
        )
    }

    fn with_timeouts(
        base_url: Url,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> std::result::Result<Self, RegistryError> {
        Self::with_options(
            base_url,
            connect_timeout,
            request_timeout,
            REGISTRY_ASSET_CACHE_TTL,
            REGISTRY_ASSET_CACHE_MAX_ENTRIES,
            REGISTRY_MAX_CONCURRENT_REQUESTS,
        )
    }

    fn with_options(
        mut base_url: Url,
        connect_timeout: Duration,
        request_timeout: Duration,
        asset_cache_ttl: Duration,
        asset_cache_max_entries: usize,
        max_concurrent_requests: usize,
    ) -> std::result::Result<Self, RegistryError> {
        if !matches!(base_url.scheme(), "http" | "https") {
            return Err(RegistryError::InvalidBaseUrl(format!(
                "asset registry URL must use http or https: {}",
                base_url
            )));
        }

        if !base_url.path().ends_with('/') {
            let path = format!("{}/", base_url.path());
            base_url.set_path(&path);
        }
        base_url.set_query(None);
        base_url.set_fragment(None);

        let http = Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(concat!("electrs/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|error| RegistryError::Transport(error.to_string()))?;

        Ok(Self {
            base_url,
            http,
            asset_cache: Arc::new(Mutex::new(HashMap::new())),
            concurrency: Arc::new(Semaphore::new(max_concurrent_requests.max(1))),
            asset_cache_ttl,
            asset_cache_max_entries: asset_cache_max_entries.max(1),
        })
    }

    pub async fn get_asset(
        &self,
        asset_id: &AssetId,
    ) -> std::result::Result<Option<RegistryAsset>, RegistryError> {
        let mut cache = self.asset_cache.lock().await;
        let now = Instant::now();

        if let Some(AssetCacheEntry::Ready { fetched_at, asset }) = cache.get(asset_id) {
            if now.duration_since(*fetched_at) < self.asset_cache_ttl {
                return Ok(asset.as_ref().map(|asset| asset.as_ref().clone()));
            }
        }

        let receiver = if let Some(AssetCacheEntry::Fetching(waiters)) = cache.get_mut(asset_id) {
            let (sender, receiver) = oneshot::channel();
            waiters.push(sender);
            receiver
        } else {
            cache.remove(asset_id);
            prune_asset_cache(
                &mut cache,
                now,
                self.asset_cache_ttl,
                self.asset_cache_max_entries,
            );

            // Same-key callers join the in-flight request above without consuming a permit.
            // New requests are rejected instead of accumulating behind the registry.
            let permit = self.try_acquire_permit()?;
            let (sender, receiver) = oneshot::channel();
            cache.insert(*asset_id, AssetCacheEntry::Fetching(vec![sender]));

            let client = self.clone();
            let asset_id = *asset_id;
            tokio::spawn(async move {
                let result = client.fetch_asset(&asset_id).await;
                if let Err(error) = &result {
                    warn!("asset registry lookup failed for {}: {}", asset_id, error);
                }
                client.complete_asset_fetch(asset_id, result).await;
                drop(permit);
            });
            receiver
        };
        drop(cache);

        receiver.await.unwrap_or_else(|_| {
            Err(RegistryError::Transport(
                "asset registry lookup worker stopped unexpectedly".to_string(),
            ))
        })
    }

    async fn fetch_asset(&self, asset_id: &AssetId) -> RegistryAssetResult {
        let url = self
            .base_url
            .join(&format!("v2/assets/{}", asset_id))
            .map_err(|error| RegistryError::InvalidBaseUrl(error.to_string()))?;
        let response = self.http.get(url).send().await.map_err(request_error)?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(RegistryError::HttpStatus(response.status().as_u16()));
        }

        let mut asset: RegistryAsset =
            decode_json_response(response, REGISTRY_MAX_ASSET_RESPONSE_SIZE).await?;
        if asset.asset_id != *asset_id {
            return Err(RegistryError::InvalidResponse(format!(
                "asset registry returned {} for requested asset {}",
                asset.asset_id, asset_id
            )));
        }
        self.make_icon_absolute(&mut asset)?;
        Ok(Some(asset))
    }

    async fn complete_asset_fetch(&self, asset_id: AssetId, result: RegistryAssetResult) {
        let mut cache = self.asset_cache.lock().await;
        let waiters = match cache.remove(&asset_id) {
            Some(AssetCacheEntry::Fetching(waiters)) => waiters,
            _ => vec![],
        };
        if let Ok(asset) = &result {
            cache.insert(
                asset_id,
                AssetCacheEntry::Ready {
                    fetched_at: Instant::now(),
                    asset: asset.clone().map(Arc::new),
                },
            );
        }
        drop(cache);

        for waiter in waiters {
            let _ = waiter.send(result.clone());
        }
    }

    fn make_icon_absolute(
        &self,
        asset: &mut RegistryAsset,
    ) -> std::result::Result<(), RegistryError> {
        if let Some(icon) = &mut asset.icon {
            let href = self
                .base_url
                .join(&icon.href)
                .map_err(|error| RegistryError::InvalidResponse(error.to_string()))?;
            if !matches!(href.scheme(), "http" | "https") || href.origin() != self.base_url.origin()
            {
                return Err(RegistryError::InvalidResponse(format!(
                    "asset registry returned an invalid icon URL: {}",
                    icon.href
                )));
            }
            icon.href = href.to_string();
        }
        Ok(())
    }

    fn try_acquire_permit(&self) -> std::result::Result<OwnedSemaphorePermit, RegistryError> {
        self.concurrency.clone().try_acquire_owned().map_err(|_| {
            RegistryError::Overloaded("too many concurrent asset registry requests".to_string())
        })
    }

    pub async fn list_assets(
        &self,
        start_index: usize,
        limit: usize,
        sorting: AssetSorting,
    ) -> std::result::Result<RegistryAssetList, RegistryError> {
        if limit > REGISTRY_MAX_PAGE_SIZE {
            return Err(RegistryError::InvalidRequest(format!(
                "asset registry page size cannot exceed {}",
                REGISTRY_MAX_PAGE_SIZE
            )));
        }

        // The v2 API has no zero-sized page, but electrs historically accepts limit=0 and
        // still returns the total count.
        let page_size = limit.max(1);
        let page = if limit == 0 {
            1
        } else {
            (start_index / page_size).checked_add(1).ok_or_else(|| {
                RegistryError::InvalidRequest("asset registry page overflow".to_string())
            })?
        };
        if page > REGISTRY_MAX_PAGE {
            return Err(RegistryError::InvalidRequest(format!(
                "asset registry page cannot exceed {}",
                REGISTRY_MAX_PAGE
            )));
        }

        let offset = if limit == 0 {
            0
        } else {
            start_index % page_size
        };
        let _permit = self.try_acquire_permit()?;
        let first = self.fetch_page(page, page_size, sorting).await?;
        let total_count = first.total_count.ok_or_else(|| {
            RegistryError::InvalidResponse(
                "asset registry response is missing total_count".to_string(),
            )
        })?;

        if limit == 0 || start_index >= total_count {
            return Ok(RegistryAssetList {
                total_count,
                items: vec![],
            });
        }

        let mut items = first.items;
        if offset.saturating_add(limit) > items.len()
            && page < REGISTRY_MAX_PAGE
            && start_index.saturating_add(items.len().saturating_sub(offset)) < total_count
        {
            let second = self.fetch_page(page + 1, page_size, sorting).await?;
            if second.total_count.is_none() {
                return Err(RegistryError::InvalidResponse(
                    "asset registry response is missing total_count".to_string(),
                ));
            }
            items.extend(second.items);
        }

        Ok(RegistryAssetList {
            total_count,
            items: items.into_iter().skip(offset).take(limit).collect(),
        })
    }

    async fn fetch_page(
        &self,
        page: usize,
        page_size: usize,
        sorting: AssetSorting,
    ) -> std::result::Result<RegistryListResponse, RegistryError> {
        let url = self
            .base_url
            .join("v2/assets")
            .map_err(|error| RegistryError::InvalidBaseUrl(error.to_string()))?;
        let response = self
            .http
            .get(url)
            .query(&[
                ("page", page.to_string()),
                ("page_size", page_size.to_string()),
                ("sort", sorting.as_str().to_string()),
            ])
            .send()
            .await
            .map_err(request_error)?;

        if !response.status().is_success() {
            return Err(RegistryError::HttpStatus(response.status().as_u16()));
        }

        let mut page_response: RegistryListResponse =
            decode_json_response(response, REGISTRY_MAX_LIST_RESPONSE_SIZE).await?;
        if page_response.page != page || page_response.page_size != page_size {
            return Err(RegistryError::InvalidResponse(format!(
                "asset registry returned page {}/{} for requested page {}/{}",
                page_response.page, page_response.page_size, page, page_size
            )));
        }
        for asset in &mut page_response.items {
            self.make_icon_absolute(asset)?;
        }
        Ok(page_response)
    }
}

fn registry_timeout_from_env(
    variable: &str,
    default: Duration,
) -> std::result::Result<Duration, RegistryError> {
    match env::var(variable) {
        Ok(value) => parse_registry_timeout(variable, &value),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => Err(RegistryError::InvalidRequest(format!(
            "{} must be valid Unicode",
            variable
        ))),
    }
}

fn parse_registry_timeout(
    variable: &str,
    value: &str,
) -> std::result::Result<Duration, RegistryError> {
    let millis = value.parse::<u64>().map_err(|_| {
        RegistryError::InvalidRequest(format!("{} must be a positive integer", variable))
    })?;
    if millis == 0 {
        return Err(RegistryError::InvalidRequest(format!(
            "{} must be greater than zero",
            variable
        )));
    }
    Ok(Duration::from_millis(millis))
}

fn prune_asset_cache(
    cache: &mut HashMap<AssetId, AssetCacheEntry>,
    now: Instant,
    ttl: Duration,
    max_entries: usize,
) {
    cache.retain(|_, entry| match entry {
        AssetCacheEntry::Ready { fetched_at, .. } => now.duration_since(*fetched_at) < ttl,
        AssetCacheEntry::Fetching(_) => true,
    });

    while cache.len() >= max_entries {
        let oldest = cache
            .iter()
            .filter_map(|(asset_id, entry)| match entry {
                AssetCacheEntry::Ready { fetched_at, .. } => Some((*asset_id, *fetched_at)),
                AssetCacheEntry::Fetching(_) => None,
            })
            .min_by_key(|(_, fetched_at)| *fetched_at)
            .map(|(asset_id, _)| asset_id);
        match oldest {
            Some(asset_id) => {
                cache.remove(&asset_id);
            }
            None => break,
        }
    }
}

async fn decode_json_response<T: DeserializeOwned>(
    mut response: reqwest::Response,
    max_size: usize,
) -> std::result::Result<T, RegistryError> {
    if let Some(length) = response.content_length() {
        if length > max_size as u64 {
            return Err(RegistryError::InvalidResponse(format!(
                "asset registry response exceeds {} bytes",
                max_size
            )));
        }
    }

    let mut body =
        Vec::with_capacity(response.content_length().unwrap_or(0).min(max_size as u64) as usize);
    while let Some(chunk) = response.chunk().await.map_err(response_error)? {
        let new_len = body.len().checked_add(chunk.len()).ok_or_else(|| {
            RegistryError::InvalidResponse("asset registry response size overflow".to_string())
        })?;
        if new_len > max_size {
            return Err(RegistryError::InvalidResponse(format!(
                "asset registry response exceeds {} bytes",
                max_size
            )));
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|error| RegistryError::InvalidResponse(error.to_string()))
}

fn request_error(error: reqwest::Error) -> RegistryError {
    if error.is_timeout() {
        RegistryError::Timeout(error.to_string())
    } else {
        RegistryError::Transport(error.to_string())
    }
}

fn response_error(error: reqwest::Error) -> RegistryError {
    if error.is_timeout() {
        RegistryError::Timeout(error.to_string())
    } else {
        RegistryError::InvalidResponse(error.to_string())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RegistryContract {
    pub entity: JsonValue,
    pub name: String,
    pub precision: u8,
    #[serde(default)]
    pub ticker: Option<String>,
    pub version: u64,
    #[serde(default)]
    pub initial_issuer_pubkey: Option<String>,
    #[serde(default)]
    pub issuer_pubkey: Option<String>,
    #[serde(flatten)]
    pub extra: JsonMap<String, JsonValue>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RegistryIcon {
    pub href: String,
    #[serde(flatten)]
    pub extra: JsonMap<String, JsonValue>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RegistryAsset {
    pub asset_id: AssetId,
    pub contract: RegistryContract,
    pub initial_issuer_pubkey: String,
    pub initial_issuer_pubkey_source: String,
    pub current_issuer_pubkey: String,
    #[serde(default)]
    pub issuer_pubkey_history: Vec<JsonValue>,
    pub mutable: JsonValue,
    #[serde(default)]
    pub admin: Option<JsonValue>,
    #[serde(default)]
    pub icon: Option<RegistryIcon>,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
    #[serde(flatten)]
    pub extra: JsonMap<String, JsonValue>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AssetMeta {
    #[serde(skip_serializing_if = "JsonValue::is_null")]
    pub contract: JsonValue,
    #[serde(skip_serializing_if = "JsonValue::is_null")]
    pub entity: JsonValue,
    pub precision: u8,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ticker: Option<String>,
    pub registry: RegistryAsset,
}

impl AssetMeta {
    pub fn from_registry_asset(
        registry: RegistryAsset,
    ) -> std::result::Result<Self, RegistryError> {
        let contract = serde_json::to_value(&registry.contract)
            .map_err(|error| RegistryError::InvalidResponse(error.to_string()))?;
        Ok(Self {
            contract,
            entity: registry.contract.entity.clone(),
            precision: registry.contract.precision,
            name: registry.contract.name.clone(),
            ticker: registry.contract.ticker.clone(),
            registry,
        })
    }
}

#[derive(Debug)]
pub struct RegistryAssetList {
    pub total_count: usize,
    pub items: Vec<RegistryAsset>,
}

#[derive(Deserialize, Debug)]
struct RegistryListResponse {
    items: Vec<RegistryAsset>,
    page: usize,
    page_size: usize,
    total_count: Option<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssetSorting {
    AssetIdAsc,
    NameAsc,
    NameDesc,
    DomainAsc,
    DomainDesc,
    TickerAsc,
    TickerDesc,
    CreatedAtDesc,
    UpdatedAtDesc,
}

impl AssetSorting {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AssetIdAsc => "asset_id_asc",
            Self::NameAsc => "name_asc",
            Self::NameDesc => "name_desc",
            Self::DomainAsc => "domain_asc",
            Self::DomainDesc => "domain_desc",
            Self::TickerAsc => "ticker_asc",
            Self::TickerDesc => "ticker_desc",
            Self::CreatedAtDesc => "created_at_desc",
            Self::UpdatedAtDesc => "updated_at_desc",
        }
    }

    pub fn from_query_params(query: &HashMap<String, String>) -> Result<Self> {
        if let Some(sort) = query.get("sort") {
            ensure!(
                !query.contains_key("sort_field") && !query.contains_key("sort_dir"),
                "cannot combine sort with sort_field or sort_dir"
            );
            return match sort.as_str() {
                "asset_id_asc" => Ok(Self::AssetIdAsc),
                "name_asc" => Ok(Self::NameAsc),
                "name_desc" => Ok(Self::NameDesc),
                "domain_asc" => Ok(Self::DomainAsc),
                "domain_desc" => Ok(Self::DomainDesc),
                "ticker_asc" => Ok(Self::TickerAsc),
                "ticker_desc" => Ok(Self::TickerDesc),
                "created_at_desc" => Ok(Self::CreatedAtDesc),
                "updated_at_desc" => Ok(Self::UpdatedAtDesc),
                _ => bail!("invalid asset registry sort"),
            };
        }

        let field = query
            .get("sort_field")
            .map(String::as_str)
            .unwrap_or("ticker");
        let direction = query.get("sort_dir").map(String::as_str).unwrap_or("asc");
        match (field, direction) {
            ("name", "asc") => Ok(Self::NameAsc),
            ("name", "desc") => Ok(Self::NameDesc),
            ("domain", "asc") => Ok(Self::DomainAsc),
            ("domain", "desc") => Ok(Self::DomainDesc),
            ("ticker", "asc") => Ok(Self::TickerAsc),
            ("ticker", "desc") => Ok(Self::TickerDesc),
            ("name" | "domain" | "ticker", _) => bail!("invalid sort direction"),
            _ => bail!("invalid sort field"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use elements::issuance::ContractHash;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::str::FromStr;
    use std::sync::mpsc;
    use std::thread;

    const ASSET_ID_A: &str = "0000000000000000000000000000000000000000000000000000000000000001";
    const ASSET_ID_B: &str = "0000000000000000000000000000000000000000000000000000000000000002";

    fn asset_response(asset_id: &str, name: &str, ticker: Option<&str>) -> JsonValue {
        json!({
            "asset_id": asset_id,
            "contract": {
                "entity": {"domain": "example.com"},
                "name": name,
                "precision": 8,
                "ticker": ticker,
                "version": 1,
                "custom_contract_field": "preserved"
            },
            "initial_issuer_pubkey": format!("02{}", "11".repeat(32)),
            "initial_issuer_pubkey_source": "contract",
            "current_issuer_pubkey": format!("02{}", "11".repeat(32)),
            "issuer_pubkey_history": [],
            "mutable": {"category_tags": ["stablecoin"], "custom": {"website": "https://example.com"}},
            "admin": {"featured": true},
            "icon": {"href": format!("/v2/assets/{}/icon/{}.png", asset_id, "22".repeat(32))},
            "status": "active",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-02T00:00:00Z",
            "future_field": {"preserved": true}
        })
    }

    fn mock_server(
        responses: Vec<(u16, JsonValue)>,
    ) -> (Url, mpsc::Receiver<String>, thread::JoinHandle<()>) {
        mock_server_with_delays(
            responses
                .into_iter()
                .map(|(status, body)| (status, body, Duration::ZERO))
                .collect(),
        )
    }

    fn mock_server_with_delays(
        responses: Vec<(u16, JsonValue, Duration)>,
    ) -> (Url, mpsc::Receiver<String>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (request_tx, request_rx) = mpsc::channel();
        let thread = thread::spawn(move || {
            for (status, body, delay) in responses {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                let mut request = vec![0u8; 8192];
                let len = stream.read(&mut request).unwrap();
                request.truncate(len);
                request_tx.send(String::from_utf8(request).unwrap()).ok();
                thread::sleep(delay);

                let body = serde_json::to_string(&body).unwrap();
                let reason = match status {
                    200 => "OK",
                    404 => "Not Found",
                    503 => "Service Unavailable",
                    _ => "Error",
                };
                let response = format!(
                    "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    status,
                    reason,
                    body.len(),
                    body
                );
                stream.write_all(response.as_bytes()).unwrap();
            }
        });
        (
            Url::parse(&format!("http://{}/api", addr)).unwrap(),
            request_rx,
            thread,
        )
    }

    fn mock_unbounded_body(body: Vec<u8>) -> (Url, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let thread = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = vec![0u8; 8192];
            let _ = stream.read(&mut request);
            let header =
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n";
            let _ = stream.write_all(header.as_bytes());
            let _ = stream.write_all(&body);
        });
        (Url::parse(&format!("http://{}/api", addr)).unwrap(), thread)
    }

    fn mock_redirect() -> (Url, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let thread = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = vec![0u8; 8192];
            let _ = stream.read(&mut request);
            let response = format!(
                "HTTP/1.1 302 Found\r\nLocation: /api/v2/assets/{}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                ASSET_ID_A
            );
            stream.write_all(response.as_bytes()).unwrap();
        });
        (Url::parse(&format!("http://{}/api", addr)).unwrap(), thread)
    }

    #[tokio::test]
    async fn get_asset_projects_legacy_fields_and_preserves_v2_data() {
        let body = asset_response(ASSET_ID_A, "Asset A", None);
        let (url, requests, server) = mock_server(vec![(200, body)]);
        let expected_icon = url
            .join(&format!(
                "/v2/assets/{}/icon/{}.png",
                ASSET_ID_A,
                "22".repeat(32)
            ))
            .unwrap()
            .to_string();
        let client = RegistryClient::new(url).unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();

        let asset = client.get_asset(&id).await.unwrap().unwrap();
        let metadata = AssetMeta::from_registry_asset(asset).unwrap();

        assert_eq!(metadata.name, "Asset A");
        assert_eq!(metadata.ticker, None);
        assert_eq!(metadata.contract["custom_contract_field"], "preserved");
        assert_eq!(metadata.registry.mutable["category_tags"][0], "stablecoin");
        assert_eq!(metadata.registry.extra["future_field"]["preserved"], true);
        assert_eq!(metadata.registry.icon.as_ref().unwrap().href, expected_icon);
        assert!(requests
            .recv()
            .unwrap()
            .starts_with(&format!("GET /api/v2/assets/{} HTTP/1.1", ASSET_ID_A)));
        server.join().unwrap();
    }

    // ------------------------------------------------------------------
    // Contract-hash invariant.
    //
    // Everything down to `republish()` is registry-implementation agnostic and is kept
    // byte-identical with the same block on `new-index`, so the invariant and its fixtures
    // cannot drift between the filesystem `AssetRegistry` and the v2 `RegistryClient`.
    // `republish()` is the only part that knows which implementation it is talking to.
    // ------------------------------------------------------------------

    /// Contracts a registry can legitimately serve. Each omits optional keys that a typed
    /// representation may reintroduce as explicit nulls. `full` is the control: it has
    /// every optional key present, so it must pass under either implementation.
    fn contract_fixtures() -> Vec<(&'static str, JsonValue)> {
        vec![
            (
                "minimal",
                json!({
                    "entity": {"domain": "example.com"},
                    "name": "Asset A",
                    "precision": 8,
                    "version": 1
                }),
            ),
            (
                "ticker only",
                json!({
                    "entity": {"domain": "example.com"},
                    "name": "Asset A",
                    "precision": 8,
                    "ticker": "AAA",
                    "version": 1
                }),
            ),
            (
                "unknown future key",
                json!({
                    "entity": {"domain": "example.com"},
                    "name": "Asset A",
                    "precision": 8,
                    "version": 1,
                    "custom_contract_field": {"nested": ["preserved", 1]}
                }),
            ),
            (
                "full",
                json!({
                    "entity": {"domain": "example.com"},
                    "initial_issuer_pubkey": "02aabb",
                    "issuer_pubkey": "02ccdd",
                    "name": "Asset A",
                    "precision": 8,
                    "ticker": "AAA",
                    "version": 1
                }),
            ),
        ]
    }

    /// A Liquid asset ID commits to the contract, so whatever electrs republishes as
    /// `meta.contract` must hash to the same `ContractHash` as the contract the registry
    /// served. Otherwise clients that re-derive the asset ID to verify it reject the asset.
    ///
    /// Key ordering cannot cause a mismatch here: `from_json_contract` canonicalises
    /// through a `BTreeMap` before hashing. Only added or dropped keys can.
    fn assert_contract_hash_preserved(label: &str, served: &JsonValue, meta: &AssetMeta) {
        let served = serde_json::to_string(served).unwrap();
        let republished = serde_json::to_string(&meta.contract).unwrap();

        assert_eq!(
            ContractHash::from_json_contract(&republished).unwrap(),
            ContractHash::from_json_contract(&served).unwrap(),
            "[{}] republished contract no longer commits to the same asset id\n  \
             served:      {}\n  republished: {}",
            label,
            served,
            republished,
        );
    }

    #[tokio::test]
    async fn registry_contract_republishing_preserves_the_asset_id() {
        for (label, served) in contract_fixtures() {
            let meta = republish(&served).await;
            assert_contract_hash_preserved(label, &served, &meta);
        }
    }

    // ---- adapter: the only implementation-specific part of the invariant above ----

    /// Round-trip `served` through the v2 registry client the way a `GET /asset/:id`
    /// request does, and return the `AssetMeta` electrs would republish.
    async fn republish(served: &JsonValue) -> AssetMeta {
        let mut body = asset_response(ASSET_ID_A, "Asset A", None);
        body["contract"] = served.clone();

        let (url, _requests, server) = mock_server(vec![(200, body)]);
        let client = RegistryClient::new(url).unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();

        let asset = client.get_asset(&id).await.unwrap().unwrap();
        let meta = AssetMeta::from_registry_asset(asset).unwrap();
        server.join().unwrap();
        meta
    }

    #[tokio::test]
    async fn get_asset_maps_not_found_and_rejects_mismatched_id() {
        let mismatched = asset_response(ASSET_ID_A, "Asset A", Some("AAA"));
        let (url, _, server) = mock_server(vec![(404, json!({})), (200, mismatched)]);
        let client = RegistryClient::new(url).unwrap();
        let id_a = AssetId::from_str(ASSET_ID_A).unwrap();
        let id_b = AssetId::from_str(ASSET_ID_B).unwrap();

        assert!(client.get_asset(&id_a).await.unwrap().is_none());
        assert!(matches!(
            client.get_asset(&id_b).await,
            Err(RegistryError::InvalidResponse(_))
        ));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn asset_cache_hits_expires_and_coalesces_concurrent_misses() {
        let body = asset_response(ASSET_ID_A, "Asset A", Some("AAA"));
        let (url, requests, server) =
            mock_server_with_delays(vec![(200, body, Duration::from_millis(40))]);
        let client = RegistryClient::new(url).unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();

        let (first, second) = tokio::join!(client.get_asset(&id), client.get_asset(&id));
        assert!(first.unwrap().is_some());
        assert!(second.unwrap().is_some());
        assert!(client.get_asset(&id).await.unwrap().is_some());
        assert!(requests.recv().is_ok());
        server.join().unwrap();

        let first = asset_response(ASSET_ID_A, "Asset A", Some("AAA"));
        let second = asset_response(ASSET_ID_A, "Updated Asset A", Some("AAA"));
        let (url, requests, server) = mock_server(vec![(200, first), (200, second)]);
        let client = RegistryClient::with_options(
            url,
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_millis(10),
            10,
            2,
        )
        .unwrap();

        assert_eq!(
            client.get_asset(&id).await.unwrap().unwrap().contract.name,
            "Asset A"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            client.get_asset(&id).await.unwrap().unwrap().contract.name,
            "Updated Asset A"
        );
        assert!(requests.recv().is_ok());
        assert!(requests.recv().is_ok());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn client_rejects_excess_concurrent_requests() {
        let body = asset_response(ASSET_ID_A, "Asset A", Some("AAA"));
        let (url, requests, server) =
            mock_server_with_delays(vec![(200, body, Duration::from_millis(100))]);
        let client = RegistryClient::with_options(
            url,
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            10,
            1,
        )
        .unwrap();
        let id_a = AssetId::from_str(ASSET_ID_A).unwrap();
        let id_b = AssetId::from_str(ASSET_ID_B).unwrap();
        let first_client = client.clone();
        let first = tokio::spawn(async move { first_client.get_asset(&id_a).await });

        loop {
            match requests.try_recv() {
                Ok(_) => break,
                Err(mpsc::TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(1)).await
                }
                Err(error) => panic!("mock registry stopped early: {}", error),
            }
        }
        assert!(matches!(
            client.get_asset(&id_b).await,
            Err(RegistryError::Overloaded(_))
        ));
        assert!(first.await.unwrap().unwrap().is_some());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn client_limits_streamed_response_bodies() {
        let (url, server) = mock_unbounded_body(vec![b' '; REGISTRY_MAX_ASSET_RESPONSE_SIZE + 1]);
        let client = RegistryClient::new(url).unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();

        assert!(matches!(
            client.get_asset(&id).await,
            Err(RegistryError::InvalidResponse(message))
                if message.contains("exceeds")
        ));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn client_rejects_redirects() {
        let (url, server) = mock_redirect();
        let client = RegistryClient::new(url).unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();

        assert!(matches!(
            client.get_asset(&id).await,
            Err(RegistryError::HttpStatus(302))
        ));
        server.join().unwrap();
    }

    #[test]
    fn registry_timeout_environment_values_are_milliseconds() {
        assert_eq!(
            parse_registry_timeout(REGISTRY_CONNECT_TIMEOUT_ENV, "2500").unwrap(),
            Duration::from_millis(2500)
        );
        assert!(parse_registry_timeout(REGISTRY_CONNECT_TIMEOUT_ENV, "0").is_err());
        assert!(parse_registry_timeout(REGISTRY_REQUEST_TIMEOUT_ENV, "invalid").is_err());
    }

    #[tokio::test]
    async fn list_assets_translates_unaligned_offsets_across_pages() {
        let first = json!({
            "items": [
                asset_response(ASSET_ID_A, "Asset A", Some("AAA")),
                asset_response(ASSET_ID_B, "Asset B", Some("BBB"))
            ],
            "page": 2,
            "page_size": 2,
            "total_count": 5,
            "total_pages": 3
        });
        let second = json!({
            "items": [asset_response(ASSET_ID_B, "Asset B", Some("BBB"))],
            "page": 3,
            "page_size": 2,
            "total_count": 5,
            "total_pages": 3
        });
        let (url, requests, server) = mock_server(vec![(200, first), (200, second)]);
        let client = RegistryClient::new(url).unwrap();

        let result = client
            .list_assets(3, 2, AssetSorting::NameDesc)
            .await
            .unwrap();

        assert_eq!(result.total_count, 5);
        assert_eq!(result.items.len(), 2);
        let first_request = requests.recv().unwrap();
        let second_request = requests.recv().unwrap();
        assert!(first_request.contains("page=2"));
        assert!(first_request.contains("page_size=2"));
        assert!(first_request.contains("sort=name_desc"));
        assert!(second_request.contains("page=3"));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn list_assets_requires_total_count() {
        let body = json!({
            "items": [],
            "page": 1,
            "page_size": 25,
            "total_count": null,
            "total_pages": null
        });
        let (url, _, server) = mock_server(vec![(200, body)]);
        let client = RegistryClient::new(url).unwrap();

        assert!(matches!(
            client.list_assets(0, 25, AssetSorting::TickerAsc).await,
            Err(RegistryError::InvalidResponse(_))
        ));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn zero_limit_returns_only_the_total_count() {
        let body = json!({
            "items": [asset_response(ASSET_ID_A, "Asset A", Some("AAA"))],
            "page": 1,
            "page_size": 1,
            "total_count": 5,
            "total_pages": 5
        });
        let (url, requests, server) = mock_server(vec![(200, body)]);
        let client = RegistryClient::new(url).unwrap();

        let result = client
            .list_assets(usize::MAX, 0, AssetSorting::TickerAsc)
            .await
            .unwrap();
        assert_eq!(result.total_count, 5);
        assert!(result.items.is_empty());
        let request = requests.recv().unwrap();
        assert!(request.contains("page=1"));
        assert!(request.contains("page_size=1"));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn pagination_rejects_overflow_without_an_http_request() {
        let client = RegistryClient::new(Url::parse("http://127.0.0.1:1/").unwrap()).unwrap();
        assert!(matches!(
            client
                .list_assets(usize::MAX, 1, AssetSorting::TickerAsc)
                .await,
            Err(RegistryError::InvalidRequest(_))
        ));
    }

    #[tokio::test]
    async fn client_distinguishes_http_status_and_timeout() {
        let (url, _, server) = mock_server(vec![(503, json!({"detail": "unavailable"}))]);
        let client = RegistryClient::new(url).unwrap();
        let id = AssetId::from_str(ASSET_ID_A).unwrap();
        assert!(matches!(
            client.get_asset(&id).await,
            Err(RegistryError::HttpStatus(503))
        ));
        server.join().unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            thread::sleep(Duration::from_millis(100));
        });
        let client = RegistryClient::with_timeouts(
            Url::parse(&format!("http://{}/", addr)).unwrap(),
            Duration::from_millis(20),
            Duration::from_millis(20),
        )
        .unwrap();
        assert!(matches!(
            client.get_asset(&id).await,
            Err(RegistryError::Timeout(_))
        ));
        server.join().unwrap();
    }

    #[test]
    fn sorting_supports_legacy_and_native_parameters() {
        let mut query = HashMap::new();
        query.insert("sort_field".to_string(), "domain".to_string());
        query.insert("sort_dir".to_string(), "desc".to_string());
        assert_eq!(
            AssetSorting::from_query_params(&query).unwrap(),
            AssetSorting::DomainDesc
        );

        let mut query = HashMap::new();
        query.insert("sort".to_string(), "updated_at_desc".to_string());
        assert_eq!(
            AssetSorting::from_query_params(&query).unwrap(),
            AssetSorting::UpdatedAtDesc
        );
        query.insert("sort_dir".to_string(), "asc".to_string());
        assert!(AssetSorting::from_query_params(&query).is_err());
    }
}
