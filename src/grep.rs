use crate::*;

use sp_application_crypto::ByteArray;
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
	ParaAccount(GrepSearchParaAccount),
}

impl GrepSearch {
	pub fn subject(&self) -> Result<Subject> {
		match self {
			GrepSearch::Address(search) => search.subject(),
			GrepSearch::ParaAccount(search) => search.subject(),
		}
	}
}

#[derive(Parser)]	
pub struct GrepSearchAddress {
	/// Address to search for.
	#[clap(long, index = 1)]
	ss58: String,
}

impl GrepSearchAddress {
	pub fn subject(&self) -> Result<Subject> {
		let address = AccountId32::from_str(&self.ss58).map_err(|_| anyhow!("Invalid SS58"))?;
		Ok(Subject::Address(address.to_raw_vec()))
	}
}

#[derive(clap::ValueEnum, Clone, Copy)]
pub enum ParaLocation {
	Child,
	Sibling,
}

#[derive(Parser)]
pub struct GrepSearchParaAccount {
	/// Para ID to search for.
	#[clap(long, index = 1)]
	para_id: u16,

	/// Location of the para account.
	#[clap(long, value_enum, index = 2)]
	para_location: ParaLocation,
}

impl GrepSearchParaAccount {
	pub fn subject(&self) -> Result<Subject> {
		let address = para_id_to_address(self.para_location, self.para_id);
		Ok(Subject::Address(address))
	}
}

fn para_id_to_address(location: ParaLocation, para_id: u16) -> Vec<u8> {
	/*let address = AccountId32::from_str(&self.ss58).map_err(|_| anyhow!("Invalid SS58"))?;
	Ok(address.to_raw_vec())*/
	let prefix = match location {
		ParaLocation::Child => b"para",
		ParaLocation::Sibling => b"sibl"
	};

	let mut bytes = vec![0; 32];
	bytes[0..4].copy_from_slice(prefix);
	let encoded_id = para_id.encode();
	bytes[4..4 + encoded_id.len()].copy_from_slice(&encoded_id);
	bytes
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

		let subject = self.search.subject()?;
		log::info!("Searching for subject: {}", subject.to_string());

		search_subject(rx, prefix_lookup, subject).await
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

/// Something that we search for.
pub enum Subject {
	Address(Vec<u8>),
}

impl Subject {
	pub fn matches(&self, data: &[u8]) -> bool {
		match self {
			Subject::Address(address) => is_substr(data, address).is_some(),
		}
	}
}

impl ToString for Subject {
	fn to_string(&self) -> String {
		match self {
			Subject::Address(address) => format!("Address(0x{})", hex::encode(address)),
		}
	}
}

pub async fn search_subject(
	mut rx: Receiver<Option<(Vec<u8>, Vec<u8>)>>,
	prefix_lookup: PrefixMap,
	subject: Subject,
) -> Result<()> {
	let mut count = 0;
	let mut total = 0;

	while let Some(Some((key, value))) = rx.recv().await {
		total += 1;

		let (found_in_key, found_in_value) =
			(subject.matches(&key), subject.matches(&value));

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
				println!("VALUE MATCH '{}' 0x{} => 0x{}", info.name(), hex::encode(key), hex::encode(v));
			},
			_ => unreachable!(),
		}
	}

	println!("Matched {} times in {} entries", count, total);

	Ok(())
}

pub fn is_substr<T: PartialEq>(mut haystack: &[T], needle: &[T]) -> Option<usize> {
	if needle.len() == 0 {
		return Some(0);
	}
	let mut start = 0;
	while !haystack.is_empty() {
		if haystack.starts_with(needle) {
			return Some(start);
		}
		haystack = &haystack[1..];
		start += 1;
	}
	None
}
