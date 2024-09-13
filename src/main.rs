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
//! cargo run --release -- --runtime rococo-people
//! ```
//!
//! The results will be a bit bring for such a small runtime, but for a larger one - eg Kusama - it
//! could look like this:
//!
//! ![Kusama storage analysis](./.images/ksm-overview.png)
//!
//! You can also zoom in on a specific pallet:
//!
//! ```sh
//! cargo run --release -- --runtime rococo-people --pallet Balances
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
use indicatif::ProgressBar;
use itertools::Itertools;
use parity_scale_codec::{Compact, Decode, Encode};
use sp_crypto_hashing::twox_128;
use subxt::Metadata;
use subxt_metadata::StorageEntryMetadata;
use tokio::sync::mpsc::{channel, Receiver};

use std::{collections::BTreeMap, fs::File, io::prelude::*};

type PrefixMap = BTreeMap<Vec<u8>, (String, Option<StorageEntryMetadata>)>;

struct PalletInfo {
	name: String,
	size: usize,
	items: BTreeMap<String, ItemInfo>,
}

struct ItemInfo {
	name: String,
	key_len: usize,
	value_len: usize,
	num_entries: usize,
}

#[derive(Parser)]
struct Args {
	#[clap(short, long)]
	runtime: String,

	#[clap(long, alias = "url")]
	uri: Option<String>,

	#[clap(short, long)]
	pallet: Option<String>,

	#[clap(long)]
	verbose: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
	env_logger::init();
	let args = Args::parse();
	let url = args.uri.unwrap_or(format!("wss://{}-rpc.polkadot.io:443", args.runtime));
	let snap_path = format!("{}.snap", args.runtime);
	let meta_path = format!("{}.meta", args.runtime);
	let verbose = args.verbose || args.pallet.is_some();

	let (num_keys, mut snap) = load_snapshot(&snap_path).await?;
	let bar = ProgressBar::new(num_keys as u64);

	let meta = get_metadata(&meta_path, &url).await?;
	let pallets = meta.pallets().sorted_by(|a, b| a.name().cmp(b.name())).collect::<Vec<_>>();

	let mut prefix_lookup = PrefixMap::new();

	for pallet in &pallets {
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

	log::info!("Indexed {} known prefixes", prefix_lookup.len());
	log::info!("Starting to categorize {} keys", num_keys);

	let mut found_by_pallet = BTreeMap::<String, PalletInfo>::new();

	for _ in 0..num_keys {
		let (key, (value, _ref_count)) = snap.recv().await.unwrap();
		let cat = categorize_prefix(&key, &prefix_lookup);

		match cat {
			Some((pallet, Some(storage))) => {
				let pallet_info = found_by_pallet.entry(pallet.clone()).or_insert(PalletInfo {
					name: pallet.clone(),
					size: 0,
					items: BTreeMap::new(),
				});

				let item_info =
					pallet_info.items.entry(storage.name().to_string()).or_insert(ItemInfo {
						name: storage.name().to_string(),
						key_len: 0,
						value_len: 0,
						num_entries: 0,
					});

				item_info.key_len += key.len();
				item_info.value_len += value.len();
				item_info.num_entries += 1;

				pallet_info.size += key.len() + value.len();
			},
			Some((pallet, None)) => {
				let pallet_info = found_by_pallet.entry(pallet.clone()).or_insert(PalletInfo {
					name: pallet.clone(),
					size: 0,
					items: BTreeMap::new(),
				});

				let item_info =
					pallet_info.items.entry("Unknown".to_string()).or_insert(ItemInfo {
						name: "Unknown".to_string(),
						key_len: 0,
						value_len: 0,
						num_entries: 0,
					});

				item_info.key_len += key.len();
				item_info.value_len += value.len();
				item_info.num_entries += 1;

				pallet_info.size += key.len() + value.len();
			},
			_ => {
				let pallet_info =
					found_by_pallet.entry("Unknown".to_string()).or_insert(PalletInfo {
						name: "Unknown".to_string(),
						size: 0,
						items: BTreeMap::new(),
					});

				let item_info =
					pallet_info.items.entry("Unknown".to_string()).or_insert(ItemInfo {
						name: "Unknown".to_string(),
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

		bar.inc(1);
	}
	bar.finish();
	println!();

	let pallet_infos = found_by_pallet
		.values()
		.sorted_by(|a, b| b.size.cmp(&a.size))
		.collect::<Vec<_>>();

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
				" ({} keys, key_size: {}, value_size: {})",
				total_keys,
				fmt_bytes(key_size),
				fmt_bytes(value_size)
			)
		} else {
			"".into()
		};
		println!("{}  {}{suffix}", fmt_bytes(pallet.size), pallet.name);

		for (_e, (_, item)) in pallet
			.items
			.iter()
			.sorted_by_key(|(_, i)| i.key_len + i.value_len)
			.rev()
			.enumerate()
		{
			let suffix = if verbose {
				format!(
					" ({} keys, key_size: {}, value_size: {})",
					item.num_entries,
					fmt_bytes(item.key_len),
					fmt_bytes(item.value_len)
				)
			} else {
				"".into()
			};

			println!("{}   {}{}", fmt_bytes(item.key_len + item.value_len), item.name, suffix);
		}
	}

	Ok(())
}

fn fmt_bytes(bytes: usize) -> String {
	if bytes < 1000 {
		format!("{: >3} B", bytes)
	} else if bytes < 1_000_000 {
		format!("{: >3.0} K", bytes as f64 / 1000.0)
	} else if bytes < 1_000_000_000 {
		format!("{: >3.0} M", bytes as f64 / 1_000_000.0)
	} else {
		format!("{: >3.0} G", bytes as f64 / 1_000_000_000.0)
	}
}

fn categorize_prefix(
	key: &[u8],
	lookup: &PrefixMap,
) -> Option<(String, Option<StorageEntryMetadata>)> {
	if key.len() >= 32 {
		let prefix = &key[0..32];

		if let Some((pallet, storage)) = lookup.get(prefix) {
			return Some((pallet.clone(), storage.clone()));
		}
	}
	if key.len() >= 16 {
		let prefix = &key[0..16];

		if let Some((pallet, storage)) = lookup.get(prefix) {
			return Some((pallet.clone(), storage.clone()));
		}
	}
	None
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

async fn load_snapshot(path: &str) -> Result<(usize, Receiver<(Vec<u8>, (Vec<u8>, i32))>)> {
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

	let storage_len = Compact::<u32>::decode(&mut input).map(|l| l.0)?;

	let (tx, rx) = channel(1024);

	tokio::spawn(async move {
		for _ in 0..storage_len {
			let key = Vec::<u8>::decode(&mut input).unwrap();

			let value = Vec::<u8>::decode(&mut input).unwrap();
			let ref_count = i32::decode(&mut input).unwrap();

			tx.send((key, (value, ref_count))).await.unwrap();
		}
	});

	Ok((storage_len as usize, rx))
}
