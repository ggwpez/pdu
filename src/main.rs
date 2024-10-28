//! Investigate storage size of Substrate chains.
//!
//! ## Example
//!
//! First acquire a state snapshot. We are going to use the People Rococo chain, since it is rather
//! small. You will need the
//! [try-runtime-cli](https://paritytech.github.io/try-runtime-cli/try_runtime/) for this and an
//! archive node to download the state from:
//!
//! ```sh
//! try-runtime create-snapshot --uri wss://rococo-people-rpc.polkadot.io:443 rococo-people.snap
//! ```
//!
//! Then run the analysis:
//!
//! ```sh
//! cargo run --release -- --network rococo-people
//! ```
//!
//! The results will be a bit boring for such a small network, but for a larger one - eg Kusama - it
//! could look like this. You can download [this snapshot](https://tasty.limo/kusama.snap) to try it.
//!
//! ![Kusama storage analysis](./.images/ksm-overview.png)
//!
//! You can also zoom in on a specific pallet:
//!
//! ```sh
//! cargo run --release -- --network rococo-people --pallet Balances
//! ```
//!
//! Again for Kusama:
//!
//! ![Kusama Balances pallet](./.images/ksm-zoom.png)
//!
//! ## License
//!
//! GPLv3 ONLY, see [LICENSE](./LICENSE) file for details.

use anyhow::{anyhow, Result};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use parity_scale_codec::{Compact, Decode, Encode};
use sp_crypto_hashing::twox_128;
use std::{collections::BTreeMap as Map, fs::File, io::prelude::*};
use subxt::Metadata;
use subxt_metadata::StorageEntryMetadata;
use termtree::Tree;
use tokio::sync::mpsc::{channel, Receiver};
use std::sync::Mutex;
use std::sync::Arc;
use tokio::task;
use std::time::Duration;
use tokio::task::JoinHandle;
use subxt_metadata::PalletMetadata;

#[cfg(feature = "chainspec")]
mod chainspec;

#[derive(Parser)]
struct Root {
    #[clap(subcommand)]
	cmd: SubCommand,
}

#[derive(Parser)]
enum SubCommand {
    Info(Info),
    #[cfg(feature = "chainspec")]
    Chainspec(chainspec::Chainspec),
}

/// PDU - Polkadot runtime storage analyzer.
#[derive(Parser)]
struct Info {
	/// Name of the network to analyze.
	#[clap(short, long)]
	network: String,

	/// URI of an Archive node endpoint.
	#[clap(long, alias = "url")]
	uri: Option<String>,

	/// Focus only on this pallet.
	#[clap(short, long)]
	pallet: Option<String>,

	/// Print verbose information.
	#[clap(long)]
	verbose: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let root = Root::parse();

    match root.cmd {
        SubCommand::Info(info) => info.run().await,
        #[cfg(feature = "chainspec")]
        SubCommand::Chainspec(chainspec) => chainspec.run().await,
    }
}

impl Info {
    pub async fn run(&self) -> Result<()> {
        env_logger::init();
        let url = self
            .uri
            .clone()
            .unwrap_or(format!("wss://{}-rpc.polkadot.io:443", self.network));
        let snap_path = format!("{}.snap", self.network);
        let meta_path = format!("{}.meta", self.network);
        let verbose = self.verbose || self.pallet.is_some();

        let (num_keys, rx) = load_snapshot(&snap_path)?;
        let bar = setup_bar(num_keys);

        let meta = get_metadata(&meta_path, &url).await?;
        let pallets = meta.pallets().sorted_by(|a, b| a.name().cmp(b.name())).collect::<Vec<_>>();

        let prefix_lookup = build_prefix_lookup(&pallets);

        log::info!("Indexed {} known prefixes", prefix_lookup.len());
        log::info!("Starting to categorize {} keys", num_keys);

        let rx = Arc::new(Mutex::new(rx));
        let prefix_lookup = Arc::new(prefix_lookup);

        let num_threads = num_cpus::get();
        let chunk_size = num_keys / num_threads + 1;

        let mut handles = vec![];

        for _ in 0..num_threads {
            let rx_clone = Arc::clone(&rx);
            let prefix_lookup_clone = Arc::clone(&prefix_lookup);
            let bar_clone = bar.clone();
            let handle = task::spawn(async move {
                process_snapshot_chunk(rx_clone, prefix_lookup_clone, chunk_size, bar_clone).await
            });
            handles.push(handle);
        }

        let found_by_pallet = merge_partial_results(handles).await?;

        bar.finish();
        println!();

        print_results(&found_by_pallet, verbose, &self);

        Ok(())
    }
}

fn setup_bar(num_keys: usize) -> ProgressBar {
	let bar = ProgressBar::new(num_keys as u64);
	bar.set_style(ProgressStyle::default_bar().template("[{elapsed}] {bar:60.cyan/blue} {percent}% {per_sec:1}").unwrap());
	bar.enable_steady_tick(Duration::from_millis(100));
	bar
}

fn build_prefix_lookup(pallets: &[PalletMetadata]) -> PrefixMap {
    let mut prefix_lookup = PrefixMap::new();

    for pallet in pallets {
        let pallet_hash = twox_128(pallet.name().as_bytes());
        prefix_lookup.insert(pallet_hash.into(), (pallet.name().into(), None));

        if let Some(storage) = pallet.storage() {
            for entry in storage.entries() {
                let entry_hash = twox_128(entry.name().as_bytes());
                let full_hash = [pallet_hash, entry_hash].concat();
                prefix_lookup.insert(full_hash.into(), (pallet.name().into(), Some(entry.clone())));
            }
        }
    }

    prefix_lookup
}

async fn process_snapshot_chunk(
    rx: Arc<Mutex<Receiver<(Vec<u8>, (Vec<u8>, i32))>>>,
    prefix_lookup: Arc<PrefixMap>,
    chunk_size: usize,
    bar: ProgressBar,
) -> Map<String, PalletInfo> {
    let mut found_by_pallet = Map::<String, PalletInfo>::new();
    let unknown = ansi_term::Color::Yellow.paint("Unknown").to_string();
    let mut processed = 0;
    
    while processed < chunk_size {
        let item = {
            let mut rx_guard = rx.lock().unwrap();
            rx_guard.try_recv()
        };

        match item {
            Ok((key, (value, _ref_count))) => {
                let cat = categorize_prefix(&key, &prefix_lookup);

                match cat {
                    CategorizedKey::Item(pallet, item) => {
                        let pallet_info = found_by_pallet.entry(pallet.clone()).or_insert(PalletInfo {
                            name: pallet.clone(),
                            size: 0,
                            items: Map::new(),
                        });

                        let item_info = pallet_info.items.entry(item.name().to_string()).or_insert(ItemInfo {
                            name: item.name().to_string(),
                            key_len: 0,
                            value_len: 0,
                            num_entries: 0,
                        });

                        item_info.key_len += key.len();
                        item_info.value_len += value.len();
                        item_info.num_entries += 1;

                        pallet_info.size += key.len() + value.len();
                    },
                    CategorizedKey::Pallet(pallet) => {
                        let pallet_info = found_by_pallet.entry(pallet.clone()).or_insert(PalletInfo {
                            name: pallet.clone(),
                            size: 0,
                            items: Map::new(),
                        });

                        let item_info = pallet_info.items.entry(unknown.to_string()).or_insert(ItemInfo {
                            name: unknown.to_string(),
                            key_len: 0,
                            value_len: 0,
                            num_entries: 0,
                        });

                        item_info.key_len += key.len();
                        item_info.value_len += value.len();
                        item_info.num_entries += 1;

                        pallet_info.size += key.len() + value.len();
                    },
                    CategorizedKey::Unknown => {
                        let pallet_info = found_by_pallet.entry(unknown.to_string()).or_insert(PalletInfo {
                            name: unknown.to_string(),
                            size: 0,
                            items: Map::new(),
                        });

                        let item_info = pallet_info.items.entry(unknown.to_string()).or_insert(ItemInfo {
                            name: unknown.to_string(),
                            key_len: 0,
                            value_len: 0,
                            num_entries: 0,
                        });

                        item_info.key_len += key.len();
                        item_info.value_len += value.len();
                        item_info.num_entries += 1;

                        pallet_info.size += key.len() + value.len();
                    },
                }
                processed += 1;
                bar.inc(1);
            },
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            },
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
        }
    }

    found_by_pallet
}


async fn merge_partial_results(handles: Vec<JoinHandle<Map<String, PalletInfo>>>) -> Result<Map<String, PalletInfo>> {
    let mut found_by_pallet = Map::<String, PalletInfo>::new();

    for handle in handles {
        let partial_result = handle.await?;
        for (pallet, mut pallet_info) in partial_result {
            found_by_pallet
                .entry(pallet)
                .and_modify(|existing| {
                    existing.size += pallet_info.size;
                    for (item_name, item_info) in pallet_info.items.iter_mut() {
                        existing.items
                            .entry(item_name.clone())
                            .and_modify(|existing_item| {
                                existing_item.key_len += item_info.key_len;
                                existing_item.value_len += item_info.value_len;
                                existing_item.num_entries += item_info.num_entries;
                            })
                            .or_insert_with(|| item_info.clone());
                    }
                })
                .or_insert(pallet_info);
        }
    }

    Ok(found_by_pallet)
}

type PrefixMap = Map<Vec<u8>, (String, Option<StorageEntryMetadata>)>;

#[derive(Default)]
struct NetworkInfo {
    size: usize,
    num_keys: usize,
    key_size: usize,
    num_values: usize,
    value_size: usize,
}

/// Storage size information of a pallet.
struct PalletInfo {
	/// Name of the pallet.
	name: String,
	size: usize,
	/// The storage items of the pallet.
	items: Map<String, ItemInfo>,
}

/// Storage size information of a storage item inside a pallet.
#[derive(Clone)]
struct ItemInfo {
	name: String,
	key_len: usize,
	value_len: usize,
	num_entries: usize,
}

enum CategorizedKey {
	/// A key that belongs to a storage item inside a pallet.
	Item(String, StorageEntryMetadata),
	/// A key that belongs to a pallet but an unknown storage item.
	Pallet(String),
	/// A key that does not belong to any known pallet.
	Unknown,
}

impl From<(String, Option<StorageEntryMetadata>)> for CategorizedKey {
	fn from((pallet, storage): (String, Option<StorageEntryMetadata>)) -> Self {
		if let Some(storage) = storage {
			CategorizedKey::Item(pallet, storage)
		} else {
			CategorizedKey::Pallet(pallet)
		}
	}
}

fn print_results(found_by_pallet: &Map<String, PalletInfo>, verbose: bool, args: &Info) {
	let pallet_infos = found_by_pallet
		.values()
		.sorted_by(|a, b| b.size.cmp(&a.size))
		.collect::<Vec<_>>();

        let network_info = pallet_infos.iter().fold(NetworkInfo::default(), |acc, p| {
            let key_size = p.items.values().map(|i| i.key_len).sum::<usize>();
            let value_size = p.items.values().map(|i| i.value_len).sum::<usize>();
            let num_keys = p.items.values().map(|i| i.num_entries).sum::<usize>();
            
            NetworkInfo {
                size: acc.size + p.size,
                num_keys: acc.num_keys + num_keys,
                key_size: acc.key_size + key_size,
                num_values: acc.num_values + num_keys,
                value_size: acc.value_size + value_size,
            }
        });

        let suffix = if verbose {
            format!(
                " ({} keys, key: {}, value: {})",
                network_info.num_keys,
                fmt_bytes(network_info.key_size, false),
                fmt_bytes(network_info.value_size, false)
            )
        } else {
            "".into()
        };
	let mut pretty_tree = Tree::new(format!("{} {}{suffix}", fmt_bytes(network_info.size, true), args.network));

	// Print stats about how many keys per pallet and item
	for (_p, pallet) in pallet_infos.iter().enumerate() {
		if args
			.pallet
			.as_ref()
			.map_or(false, |p| p.to_lowercase() != pallet.name.to_lowercase())
		{
			continue;
		}
		let suffix = if verbose {
			let total_keys = pallet.items.values().map(|i| i.num_entries).sum::<usize>();
			let key_size = pallet.items.values().map(|i| i.key_len).sum::<usize>();
			let value_size = pallet.items.values().map(|i| i.value_len).sum::<usize>();
			format!(
				" ({} keys, key: {}, value: {})",
				total_keys,
				fmt_bytes(key_size, false),
				fmt_bytes(value_size, false)
			)
		} else {
			"".into()
		};
		let mut pallet_node =
			Tree::new(format!("{} {}{}", fmt_bytes(pallet.size, true), pallet.name, suffix));

		for (_e, (_, item)) in pallet
			.items
			.iter()
			.sorted_by_key(|(_, i)| i.key_len + i.value_len)
			.rev()
			.enumerate()
		{
			let suffix = if verbose {
				format!(
					" ({} keys, key: {}, value: {})",
					item.num_entries,
					fmt_bytes(item.key_len, false),
					fmt_bytes(item.value_len, false)
				)
			} else {
				"".into()
			};
			let item_node = format!("{} {}{}", fmt_bytes(item.value_len + item.key_len, true), item.name, suffix);
			pallet_node.push(item_node);
		}

		pretty_tree.push(pallet_node);
	}

	println!("{}", pretty_tree);
}

fn fmt_bytes(number: usize, pad_left: bool) -> String {
    let (scaled, suffix) = match number {
        n if n >= 1_000_000_000 => (number as f64 / 1_000_000_000.0, "G"),
        n if n >= 1_000_000 => (number as f64 / 1_000_000.0, "M"),
        n if n >= 1_000 => (number as f64 / 1_000.0, "K"),
        _ => (number as f64, ""),
    };

    let formatted = if scaled < 10.0 {
        format!("{:.1} {}", scaled, suffix)
    } else {
        format!("{:.0} {}", scaled, suffix)
    };

    if pad_left {
        format!("{:>3}", formatted)
    } else {
        formatted
    }
}

fn categorize_prefix(key: &[u8], lookup: &PrefixMap) -> CategorizedKey {
	if key.len() >= 32 {
		let prefix = &key[0..32];

		if let Some((pallet, storage)) = lookup.get(prefix) {
			return (pallet.clone(), storage.clone()).into();
		}
	}
	if key.len() >= 16 {
		let prefix = &key[0..16];

		if let Some((pallet, storage)) = lookup.get(prefix) {
			return (pallet.clone(), storage.clone()).into();
		}
	}
	CategorizedKey::Unknown
}

async fn get_metadata(path: &str, url: &str) -> Result<Metadata> {
	// Check if metadata.json exists
	if let Ok(file) = File::open(path) {
		let bytes = file.bytes().map(|b| b.unwrap()).collect::<Vec<u8>>();
		let meta = Metadata::decode(&mut bytes.as_slice())?;
		log::info!("Metadata loaded from file");
		return Ok(meta);
	}

	let cl = subxt::OnlineClient::<subxt::SubstrateConfig>::from_url(url).await?;
	let meta = cl.metadata();

	// Write meta to file
	let mut file = File::create(path)?;
	file.write_all(&meta.encode())?;
	log::info!("Metadata written to file");

	Ok(meta)
}

/// Load a try-runtime-cli snapshot from a path.
///
/// Returns the total number of keys in the snapshot and a channel that can be used to read exactly
/// that many Key-Value pairs.
fn load_snapshot(path: &str) -> Result<(usize, Receiver<(Vec<u8>, (Vec<u8>, i32))>)> {
	log::info!("Loading snapshot from file");
	let file = File::open(path)
		.map_err(|e| anyhow!("Failed to load snapshot file from {}: {}", path, e))?;
	let mut input = parity_scale_codec::IoReader(file);

	let snapshot_version = Compact::<u16>::decode(&mut input)?;
	if snapshot_version.0 != 4 {
		log::warn!("Snapshot version is not 4 but {}", snapshot_version.0);
	}

	let state_version: u8 = u8::decode(&mut input)?;
	if state_version != 1 {
		log::warn!("State version is not 1 but {}", state_version);
	}

	let num_keys = Compact::<u32>::decode(&mut input).map(|l| l.0)?;

	let (tx, rx) = channel(1024*100);

	tokio::spawn(async move {
		for _ in 0..num_keys {
			let key = Vec::<u8>::decode(&mut input).unwrap();

			let value = Vec::<u8>::decode(&mut input).unwrap();
			let ref_count = i32::decode(&mut input).unwrap();

			tx.send((key, (value, ref_count))).await.unwrap();
		}
	});

	Ok((num_keys as usize, rx))
}
