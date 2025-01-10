//! Module to parse a try-runtime snapshot and extract specific storage prefixes into a chainspec
//! file.
//!
//! This can be useful when you want to start a new network with a genesis state that contains
//! production data.

use anyhow::{anyhow, Result};
use clap::Parser;
use frame_remote_externalities::{
	Builder, Mode, OfflineConfig, OnlineConfig, RemoteExternalities, SnapshotConfig, Transport,
};
use frame_support::StorageHasher;
use parity_scale_codec::{Decode, Encode};
use sp_crypto_hashing::{blake2_256, twox_128};

#[derive(Parser)]
pub struct Chainspec {
	#[clap(long, short)]
	snapshot_path: String,

	#[clap(long)]
	chainspec_out_path: String,

	#[clap(long, short)]
	chainspec_path: String,

	/// Export state of these pallets.
	#[clap(long, short)]
	pallets: String,

	/// Insert some accounts into the genesis state.
	///
	/// This will affect prefix `xx(System)
	#[clap(long)]
	dev_accounts: Option<u32>,
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
						Some(next_key) =>
							if next_key.starts_with(&pallet_prefix) {
								key = next_key;
								let value =
									frame_support::storage::unhashed::get_raw(&key).unwrap();
								kvs.push((key.clone(), value.clone()));
							} else {
								break;
							},
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
			let v = "0x".to_string() + &hex::encode(value);
			let k = "0x".to_string() + &hex::encode(key);
			top.insert(k, serde_json::Value::String(v));
		}

		if self.dev_accounts.is_some() {
			let pallet_prefix = twox_128(b"System");
			let storage_prefix = twox_128(b"Account");
			let account_prefix =
				pallet_prefix.iter().chain(storage_prefix.iter()).cloned().collect::<Vec<_>>();
			let value = hex::decode("000000000000000001000000000000000c72b888e00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000080").unwrap();

			let n = self.dev_accounts.unwrap_or(0);
			for i in 0..n {
				let acc =
					sp_runtime::AccountId32::decode(&mut &blake2_256(&i.encode())[..]).unwrap();

				let key = frame_support::Blake2_128Concat::hash(&acc.encode());
				let final_key =
					account_prefix.iter().chain(key.iter()).cloned().collect::<Vec<_>>();

				let v = "0x".to_string() + &hex::encode(&value);
				let k = "0x".to_string() + &hex::encode(final_key);
				top.insert(k, serde_json::Value::String(v));
			}
			println!("Inserted {} dev accounts", n);
		}

		// Write back to the chainspec JSON file
		let mut file = std::fs::File::create(&self.chainspec_out_path)?;
		serde_json::to_writer_pretty(&mut file, &chainspec)?;

		Ok(())
	}

	async fn externalities(&self) -> Result<RemoteExternalities<Block>> {
		let config = SnapshotConfig::new(self.snapshot_path.clone());

		Builder::<Block>::default()
			.mode(Mode::Offline(OfflineConfig { state_snapshot: config.clone() }))
			.build()
			.await
			.map_err(|e| anyhow!("Failed to create externalities: {:?}", e))
	}
}
