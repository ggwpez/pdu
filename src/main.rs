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

pub mod grep;
pub mod info;

use anyhow::{anyhow, Result};
use clap::Parser;
use env_logger::Env;
use frame_remote_externalities::{
	Builder, Mode, OfflineConfig, RemoteExternalities, SnapshotConfig,
};
use itertools::Itertools;
use parity_scale_codec::{Decode, Encode};
use sp_crypto_hashing::twox_128;
use sp_runtime::{generic, traits::BlakeTwo256, OpaqueExtrinsic};
use std::{collections::BTreeMap as Map, fs::File, io::prelude::*};
use subxt::Metadata;
use subxt_metadata::{PalletMetadata, StorageEntryMetadata};
use tokio::sync::mpsc::{channel, Receiver};

use grep::Grep;
use info::Info;

/// Block number
type BlockNumber = u32;
/// Opaque block header type.
type Header = generic::Header<BlockNumber, BlakeTwo256>;
/// Opaque block type.
type Block = generic::Block<Header, OpaqueExtrinsic>;

#[derive(Parser)]
struct Root {
	#[clap(subcommand)]
	cmd: SubCommand,
}

#[derive(Parser)]
enum SubCommand {
	Info(Info),
	Grep(Grep),
}

#[tokio::main]
async fn main() -> Result<()> {
	env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
	let root = Root::parse();

	match root.cmd {
		SubCommand::Info(info) => info.run().await,
		SubCommand::Grep(grep) => grep.run().await,
	}
}

pub async fn load_snapshot_kvs(path: &str) -> Result<Receiver<Option<(Vec<u8>, Vec<u8>)>>> {
	let mut ext = externalities(path).await?;
	let (tx, rx) = channel(1024 * 100);

	tokio::spawn(async move {
		let mut key = Vec::new();

		// We have to ensure that the `execute_with` closure will not be re-scheduled to a different
		// thread, since it uses thread locals. Hence why we return the values here and re-enter it.
		loop {
			let kv_pairs = ext.execute_with(|| {
				let mut kv_pairs = Vec::new();

				for _ in 0..100 {
					let Some(next_key) = sp_io::storage::next_key(&key) else {
						key = Vec::new();
						break;
					};
					key = next_key;
					let value = sp_io::storage::get(&key).unwrap();

					kv_pairs.push((key.clone(), value.clone()));
				}

				kv_pairs
			});

			for (key, value) in kv_pairs {
				tx.send(Some((key, value.to_vec()))).await.unwrap();
			}

			if key.is_empty() {
				break;
			}
		}
	});

	Ok(rx)
}

async fn externalities(path: &str) -> Result<RemoteExternalities<Block>> {
	let config = SnapshotConfig::new(path);

	Builder::<Block>::default()
		.mode(Mode::Offline(OfflineConfig { state_snapshot: config.clone() }))
		.build()
		.await
		.map_err(|e| anyhow!("Failed to create externalities: {:?}", e))
}

type PrefixMap = Map<Vec<u8>, (String, Option<StorageEntryMetadata>)>;

pub fn build_prefix_lookup(pallets: &[PalletMetadata]) -> PrefixMap {
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

pub fn categorize_prefix(key: &[u8], lookup: &PrefixMap) -> CategorizedKey {
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

pub enum CategorizedKey {
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

impl CategorizedKey {
	pub fn name(&self) -> String {
		match self {
			CategorizedKey::Item(pallet, storage) => format!("{}::{}", pallet, storage.name()),
			CategorizedKey::Pallet(pallet) => pallet.clone(),
			CategorizedKey::Unknown => "Unknown".into(),
		}
	}
}

pub async fn get_metadata(path: &str, url: &str) -> Result<Metadata> {
	// Check if metadata file exists
	if let Ok(file) = File::open(path) {
		let bytes = file.bytes().map(|b| b.unwrap()).collect::<Vec<u8>>();
		let meta = Metadata::decode(&mut bytes.as_slice())?;
		log::info!("Metadata loaded from {}", path);
		return Ok(meta);
	}

	let cl = subxt::OnlineClient::<subxt::SubstrateConfig>::from_url(url).await?;
	let meta = cl.metadata();

	// Write meta to file
	let mut file = File::create(path)?;
	file.write_all(&meta.encode())?;
	log::info!("Metadata written to {}", path);

	Ok(meta)
}
