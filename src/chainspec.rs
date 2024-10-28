use frame_remote_externalities::{
	Builder, Mode, OfflineConfig, OnlineConfig, RemoteExternalities, SnapshotConfig, Transport,
};
use anyhow::{anyhow, Result};
use clap::Parser;
use sp_crypto_hashing::twox_128;

#[derive(Parser)]
pub struct Chainspec {
	#[clap(long, short)]
	snapshot_path: String,

	#[clap(long)]
	snapshot_out_path: String,

	#[clap(long, short)]
	chainspec_path: String,

	/// Export state of these pallets.
	#[clap(long, short)]
	pallets: Vec<String>,
}

use sp_runtime::{
	generic,
	traits::{BlakeTwo256, Block as BlockT},
	transaction_validity::{TransactionSource, TransactionValidity},
	ApplyExtrinsicResult, OpaqueExtrinsic,
};

/// Block number
type BlockNumber = u32;
/// Opaque block header type.
type Header = generic::Header<BlockNumber, BlakeTwo256>;
/// Opaque block type.
type Block = generic::Block<Header, OpaqueExtrinsic>;

impl Chainspec {
	pub async fn run(&self) -> Result<()> {
		let mut ext = self.externalities().await?;
		let mut kvs = Vec::<(Vec<u8>, Vec<u8>)>::new();
		let pallets = self.pallets.split(",").collect::<Vec<&str>>();

		ext.execute_with(|| {
			for pallet in &pallets {
				println!("Pallet: {}", pallet);
				let pallet_prefix = twox_128(pallet.as_bytes()).to_vec();

				let mut key = pallet_prefix.clone();
				loop {
					match sp_io::storage::next_key(&key) {
						Some(next_key) => {
							if next_key.starts_with(&pallet_prefix) {
								key = next_key;
								let value = frame_support::storage::unhashed::get_raw(&key).unwrap();
								kvs.push((key.clone(), value.clone()));
							} else {
								break;
							}
						}
						None => break,
					}
				}
			}
		});

		println!("Total KVs: {}", kvs.len());

		// Read from the chainspec JSON file
		let mut file = std::fs::File::open(&self.chainspec_path)?;
		let mut chainspec: serde_json::Value = serde_json::from_reader(&mut file)?;

		// Insert into "genesis"."raw"."top"
		let top = chainspec
			.get_mut("genesis")
			.unwrap()
			.get_mut("raw")
			.unwrap()
			.get_mut("top")
			.unwrap()
			.as_object_mut()
			.unwrap();
		
		for (key, value) in kvs {
			top.insert(hex::encode(key), serde_json::Value::String(hex::encode(value)));
		}

		// Write back to the chainspec JSON file
		let mut file = std::fs::File::create(&self.snapshot_out_path)?;
		serde_json::to_writer_pretty(&mut file, &chainspec)?;

		Ok(())
	}

	async fn externalities(&self) -> Result<RemoteExternalities<Block>> {
		let config = SnapshotConfig::new(self.snapshot_path.clone());

		Builder::<Block>::default()
			.mode(Mode::Offline(
					OfflineConfig { state_snapshot: config.clone() },
				))
			.build()
			.await
			.map_err(|e| anyhow!("Failed to create externalities: {:?}", e))
	}
}
