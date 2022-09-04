#[macro_use]
extern crate mime;

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};
use std::borrow::Cow;
use std::collections::HashMap;
use std::io::Cursor;
use std::ops::Deref;

use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Result};
use futures::{StreamExt, TryStreamExt};
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, LocalBoxStream};
use google_drive3::{
    api::{File, Scope},
    Delegate,
    oauth2::{InstalledFlowAuthenticator, InstalledFlowReturnMethod, storage::TokenStorage},
};
use google_drive3::client::{DefaultDelegate, Retry};
use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnector;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use mime::Mime;

mod args;

pub const SECRET: &str = r#"{"client_id":"747145616764-fjvhk211vuemcng9j3r6eu15jpo3o9kl.apps.googleusercontent.com","project_id":"drivesync-361310","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://oauth2.googleapis.com/token","auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs","client_secret":"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","redirect_uris":["http://localhost"]}"#;

pub type Hub = google_drive3::DriveHub<HttpsConnector<HttpConnector>>;

fn to_lexical_absolute(p: PathBuf) -> std::io::Result<PathBuf> {
    let mut absolute = if p.is_absolute() {
        PathBuf::new()
    } else {
        std::env::current_dir()?
    };
    for component in p.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                absolute.pop();
            }
            component => absolute.push(component.as_os_str()),
        }
    }
    Ok(absolute)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SyncEntry {
    name: String,
    size: u64,
    path: PathBuf,
    parent: String,
}

struct LocalStore;

#[async_trait::async_trait]
impl TokenStorage for LocalStore {
    async fn set(
        &self,
        _scopes: &[&str],
        token: google_drive3::oauth2::storage::TokenInfo,
    ) -> anyhow::Result<()> {
        let cfg = directories::BaseDirs::new().unwrap();
        let cfg = cfg.config_dir();
        let n = cfg.join("drivesync");
        std::fs::write(n, serde_json::to_string_pretty(&token).unwrap()).unwrap();
        Ok(())
    }

    async fn get(&self, _scopes: &[&str]) -> Option<google_drive3::oauth2::storage::TokenInfo> {
        let cfg = directories::BaseDirs::new().unwrap();
        let cfg = cfg.config_dir();
        let n = cfg.join("drivesync");
        let contents = std::fs::read_to_string(n).ok()?;
        Some(serde_json::from_str(&contents).unwrap())
    }
}

pub struct FileDelegate {
    cfg: Arc<Mutex<PathBuf>>,
    prog: ProgressBar,
    name: String,
    total: usize,
    backoff: Duration,
}

impl FileDelegate {
    pub fn new(cfg: Arc<Mutex<PathBuf>>, prog: ProgressBar, name: String, total: usize) -> Self {
        Self {
            cfg,
            prog,
            name,
            total,
            backoff: Duration::from_secs(1),
        }
    }
}

impl Delegate for FileDelegate {
    fn begin(&mut self, info: google_drive3::client::MethodInfo) {
        self.prog.set_position(0);
        DefaultDelegate.begin(info)
    }

    fn http_error(&mut self, _err: &hyper::Error) -> google_drive3::client::Retry {
        self.backoff *= 2;
        self.prog.set_prefix(Cow::Owned(format!(
            "Waiting for {} secs before retrying",
            self.backoff.as_secs()
        )));
        if self.backoff.as_secs() > 300 {
            Retry::Abort
        } else {
            Retry::After(self.backoff)
        }
    }

    fn api_key(&mut self) -> Option<String> {
        DefaultDelegate.api_key()
    }

    fn token(
        &mut self,
        err: &google_drive3::oauth2::Error,
    ) -> Option<google_drive3::oauth2::AccessToken> {
        DefaultDelegate.token(err)
    }

    fn upload_url(&mut self) -> Option<String> {
        let cfg = self.cfg.lock().unwrap();
        if cfg.exists() && cfg.is_file() {
            let cfg = std::fs::read_to_string(&*cfg).ok()?;
            let cfg = serde_json::from_str::<HashMap<String, String>>(&cfg).ok()?;
            cfg.get(&self.name).cloned()
        } else {
            None
        }
    }

    fn store_upload_url(&mut self, url: Option<&str>) {
        let cfg_path = self.cfg.lock().unwrap();

        let mut cfg = if cfg_path.exists() && cfg_path.is_file() {
            let cfg = std::fs::read_to_string(&*cfg_path).unwrap();
            serde_json::from_str::<HashMap<String, String>>(&cfg).unwrap()
        } else {
            Default::default()
        };

        if let Some(url) = url {
            cfg.insert(self.name.clone(), url.to_string());
        } else {
            let _ = cfg.remove(&self.name);
        }
        std::fs::write(cfg_path.deref(), serde_json::to_string(&cfg).unwrap()).unwrap();
    }

    fn response_json_decode_error(
        &mut self,
        json_encoded_value: &str,
        json_decode_error: &serde_json::Error,
    ) {
        DefaultDelegate.response_json_decode_error(json_encoded_value, json_decode_error)
    }

    fn http_failure(
        &mut self,
        body: &hyper::Response<hyper::body::Body>,
        err: Option<serde_json::Value>,
    ) -> google_drive3::client::Retry {
        DefaultDelegate.http_failure(body, err)
    }

    fn pre_request(&mut self) {
        DefaultDelegate.pre_request()
    }

    fn chunk_size(&mut self) -> u64 {
        self.prog.set_prefix(Cow::Borrowed(""));
        if self.total < (1 << 20) {
            1 << 20
        } else if self.total < (1 << 26) {
            1 << 22
        } else {
            1 << 24
        }
        // 1 << 20
    }

    fn cancel_chunk_upload(&mut self, chunk: &google_drive3::client::ContentRange) -> bool {
        self.prog.set_position(chunk.range.as_ref().unwrap().first);
        // println!("Uploading chun");
        DefaultDelegate.cancel_chunk_upload(chunk)
    }

    fn finished(&mut self, is_success: bool) {
        self.prog.set_position(self.total as _);
        DefaultDelegate.finished(is_success)
    }
}

fn tosync(
    hub: Hub,
    path: PathBuf,
    cursor: DriveCursor,
    recursive: bool,
) -> BoxFuture<'static, Result<LocalBoxStream<'static, Result<SyncEntry>>>> {
    Box::pin(async move {
        let parent = cursor.path.last().unwrap().clone();
        let (_, mut remote) = hub
            .files()
            .list()
            .order_by("name")
            .param("fields", "files(md5Checksum,trashed,size,name)")
            .q(&format!("('{parent}' in parents) and (trashed = false)"))
            .doit()
            .await?;

        let remote = remote
            .files
            .take().unwrap_or_default()
            .into_iter()
            .map(|v| {
                let name = v.name.as_ref().unwrap().clone();
                (name, v)
            })
            .collect::<BTreeMap<_, _>>();

        // println!("Stream from here: {path:?}");
        let stream = async_fn_stream::try_fn_stream(|sink| async move {
            for local in std::fs::read_dir(&path)? {
                let local = local?;
                let ftype = local.file_type()?;
                let metadata = local.metadata()?;
                let fname = local.file_name().to_string_lossy().to_string();

                if ftype.is_file() {
                    if fname == ".drivesync" {
                        continue;
                    }
                    match remote.get(&fname) {
                        Some(v) => {
                            // Skip if unchanged
                            if metadata.len() == v.size.as_ref().unwrap().parse::<u64>().unwrap() {
                                continue;
                            }
                        }
                        _ => {}
                    }

                    let entry = SyncEntry {
                        name: fname.clone(),
                        size: metadata.len() as _,
                        path: local.path().clone(),
                        parent: parent.clone(),
                    };
                    sink.emit(entry).await;
                } else if ftype.is_dir() && recursive {
                    let mut ncursor = cursor.clone();
                    ncursor.traverse(Path::new(&fname), true).await?;
                    let nested =
                        Box::pin(tosync(hub.clone(), local.path(), ncursor, recursive));
                    let mut nested = nested.await?;
                    while let Some(item) = nested.next().await {
                        sink.emit(item?).await;
                    }
                }
            }
            Ok::<_, anyhow::Error>(())
        });

        Ok(Box::pin(stream) as _)
    })
}

async fn upsync(hub: &Hub, from: String, to: String, recursive: bool) -> anyhow::Result<()> {
    let from = std::path::PathBuf::from(from).canonicalize()?;

    let mut root = if !to.starts_with('/') {
        PathBuf::from("/")
    } else {
        PathBuf::new()
    };
    root.push(&std::path::PathBuf::from(to));

    let to = to_lexical_absolute(root)?;
    println!("Upsyncing {from:?} to {to:?}");

    let mut cursor = DriveCursor::new(hub.clone());
    cursor.traverse(&to, true).await?;

    let mp = MultiProgress::new();
    let mp = &mp;

    let cfg = Arc::new(Mutex::new(from.join(Path::new(".drivesync"))));
    let cfg = &cfg;

    let items = tosync(hub.clone(), from.clone(), cursor, recursive).await?;

    let mut uploads = items.map_ok(move |local| {
        // println!("MAP");
        let file = File {
            name: Some(local.name.clone()),
            parents: Some(vec![local.parent.clone()]),
            ..Default::default()
        };

        let stream = std::fs::File::open(&local.path).unwrap();
        let sty = ProgressStyle::with_template(
            "{wide_msg} {bar:40.green/yellow} {bytes:>10}/{total_bytes:<10} {bytes_per_sec:>10} {prefix:>20}",
        )
            .unwrap();

        let prog = ProgressBar::new(local.size as _)
            .with_style(sty)
            .with_message(Cow::Owned(local.name.clone()));

        let upload = async move {
            let mut delegate = FileDelegate::new(cfg.clone(), mp.add(prog), local.name.clone(), local.size as _);

            let upload = hub
                .files()
                .create(file)
                .delegate(&mut delegate);

            let mime = mime::mime!(Application / OctetStream);

            if local.size > (1 << 22) {
                upload
                    .upload_resumable(stream, mime)
                    .await?;
            } else {
                upload
                    .upload(stream, mime)
                    .await?;
            }

            Ok::<_, anyhow::Error>(local.size)
        };
        (local.size, upload)
    });

    let mut finished = false;
    let mut tasks = FuturesUnordered::new();
    let mut running = 0;

    loop {
        tokio::select! {
            biased;

            it = uploads.next(), if (running <= (1 << 24)) && !finished => {
                match it {
                    None => {
                        finished = true;
                    }
                    Some(Ok((size, item))) => {

                        running += size;
                        tasks.push(item);
                    }
                    Some(Err(e)) => {
                        println!("Err");
                        return Err(e);
                    }
                }
            }

            done = tasks.next(), if !tasks.is_empty() => {
                match done {
                    Some(Ok(v)) => {
                        running -= v;
                    }
                    Some(Err(e)) => {
                        return Err(e);
                    }
                    None if finished => {
                        return Ok(());
                    }
                    None => {
                        // panic!("Item underflow")
                    }
                }
            }

            else => {
                return Ok(())
            }
        }
    }
}

#[derive(Clone)]
struct DriveCursor {
    hub: Hub,
    path: Vec<String>,
}

impl DriveCursor {
    pub fn new(hub: Hub) -> Self {
        Self { path: vec![], hub }
    }

    pub async fn traverse(&mut self, path: &Path, create: bool) -> Result<()> {
        for component in path.components() {
            match component {
                std::path::Component::Prefix(_prefix) => todo!(),
                std::path::Component::RootDir => self.path = vec!["root".to_string()],
                std::path::Component::CurDir => todo!(),
                std::path::Component::ParentDir => {
                    if !self.path.is_empty() {
                        let _ = self.path.pop();
                    }
                }
                std::path::Component::Normal(p) => {
                    let parent = self.path.last().unwrap();
                    let p = p.to_string_lossy();
                    let q =
                        format!("('{parent}' in parents) and (name = '{p}') and (trashed = false)");
                    let (_, file) = self.hub.files().list().q(&q).doit().await?;

                    let f = match file.files {
                        Some(mut f) if f.len() == 1 => f.remove(0),
                        Some(f) if f.is_empty() => {
                            if create {
                                self.create(p.to_string()).await?
                            } else {
                                bail!("{p} not found in {parent}");
                            }
                        }
                        None => bail!("Received empty response when looking for {p} in {parent}"),
                        Some(f) => {
                            bail!("Received multiple possible files named {p} in {parent}: {f:#?}")
                        }
                    };
                    self.path.push(f.id.unwrap());
                }
            }
        }
        Ok(())
    }
    pub async fn create(&mut self, name: String) -> Result<File> {
        let newdir = File {
            name: Some(name),
            parents: Some(vec![self.path.last().cloned().unwrap()]),
            mime_type: Some(
                "application/vnd.google-apps.folder".to_string(),
            ),
            ..Default::default()
        };
        let mime =
            Mime::from_str("application/vnd.google-apps.folder").unwrap();
        let (_, new) = self
            .hub
            .files()
            .create(newdir)
            .param("fields", "id,parents")
            .upload(Cursor::new(vec![]), mime)
            .await
            .unwrap();
        Ok(new)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let tls = hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .https_only()
        .enable_http1()
        .enable_http2()
        .build();

    let client = hyper::Client::builder().build(tls);

    let secret: google_drive3::oauth2::ApplicationSecret = serde_json::from_str(SECRET).unwrap();
    let auth = InstalledFlowAuthenticator::builder(secret, InstalledFlowReturnMethod::HTTPRedirect)
        .with_storage(Box::new(LocalStore))
        .build()
        .await
        .unwrap();

    let hub = google_drive3::DriveHub::new(client, auth);
    let (_, _files) = hub
        .files()
        .list()
        .add_scope(Scope::File)
        .doit()
        .await
        .unwrap();

    let args: args::Args = argh::from_env();

    match args.nested {
        args::Nested::Up(up) => {
            upsync(&hub, up.from, up.to, up.recursive).await?;
        }
    }
    Ok(())
}
