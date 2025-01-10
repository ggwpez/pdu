use crate::*;

use sp_application_crypto::{ByteArray, Ss58Codec};
use sp_runtime::AccountId32;
use std::str::FromStr;

/// PDU - Polkadot runtime storage analyzer.
#[derive(Parser)]
pub struct Grep {
	/// Name of the network to analyze.
	#[clap(short, long, alias = "snap")]
	snapshot: String,

	/// URI of an Archive node endpoint.
	#[clap(long, alias = "url")]
	rpc: String,

	#[clap(subcommand)]
	search: GrepSearch,
}

#[derive(Parser)]
pub enum GrepSearch {
	Address(GrepSearchAddress),
}

#[derive(Parser)]
pub struct GrepSearchAddress {
	/// Address to search for.
	#[clap(long, index = 1)]
	ss58: String,
}

impl Grep {
	pub async fn run(self) -> Result<()> {
		let rx = load_snapshot_kvs(&self.snapshot).await?;
		let meta_path = self.meta_path();

		// TODO merge with info struct
		let url = self.url();
		let meta = get_metadata(&meta_path, &url).await?;
		let pallets = meta.pallets().sorted_by(|a, b| a.name().cmp(b.name())).collect::<Vec<_>>();
		let prefix_lookup = build_prefix_lookup(&pallets);

		log::info!("Indexed {} known prefixes", prefix_lookup.len());

		match &self.search {
			GrepSearch::Address(search) => search.search_address(rx, prefix_lookup).await,
		}
	}

	// TODO merge with info struct
	fn url(&self) -> String {
		match self.rpc.to_lowercase().as_str() {
			"kusama" => "wss://kusama-rpc.polkadot.io:443".into(),
			"polkadot" => "wss://rpc.polkadot.io:433".into(),
			v => v.into(),
		}
	}

	fn meta_path(&self) -> String {
		format!("{}.meta", self.network())
	}

	pub fn network(&self) -> String {
		let canon = std::fs::canonicalize(&self.snapshot).unwrap();
		let file_name = canon.file_name().unwrap().to_str().unwrap();

		if let Some(idx) = file_name.rfind('.') {
			file_name[..idx].into()
		} else {
			file_name.into()
		}
	}
}

impl GrepSearchAddress {
	pub async fn search_address(
		&self,
		mut rx: Receiver<Option<(Vec<u8>, Vec<u8>)>>,
		prefix_lookup: PrefixMap,
	) -> Result<()> {
		let subject = self.subject()?;

		let mut count = 0;
		let mut total = 0;

		while let Some(Some((key, value))) = rx.recv().await {
			total += 1;

			let (found_in_key, found_in_value) =
				(Self::is_sub(&key, subject.as_ref()), Self::is_sub(&value, subject.as_ref()));

			let (k, v) = if found_in_key && found_in_value {
				(Some(&key), Some(value))
			} else if found_in_key {
				(Some(&key), None)
			} else if found_in_value {
				(None, Some(value))
			} else {
				continue;
			};

			count += 1;
			let info = categorize_prefix(&key, &prefix_lookup);

			match (k, v) {
				(Some(k), Some(v)) => {
					println!(
						"KEY-VALUE MATCH '{}' 0x{} => 0x{}",
						info.name(),
						hex::encode(k),
						hex::encode(v)
					);
				},
				(Some(k), None) => {
					println!("KEY MATCH '{}' 0x{}", info.name(), hex::encode(k));
				},
				(None, Some(v)) => {
					println!("VALUE MATCH '{}' 0x{}", info.name(), hex::encode(v));
				},
				_ => unreachable!(),
			}
		}

		println!("Matched {} times in {} entries", count, total);

		Ok(())
	}

	fn subject(&self) -> Result<Vec<u8>> {
		let address = AccountId32::from_str(&self.ss58).map_err(|_| anyhow!("Invalid SS58"))?;
		log::info!("Searching for address: {}", address.to_ss58check());

		Ok(address.to_raw_vec())
	}

	fn is_sub<T: PartialEq>(mut haystack: &[T], needle: &[T]) -> bool {
		if needle.len() == 0 {
			return true;
		}
		while !haystack.is_empty() {
			if haystack.starts_with(needle) {
				return true;
			}
			haystack = &haystack[1..];
		}
		false
	}
}
