use crate::*;

use sp_application_crypto::ByteArray;
use sp_runtime::AccountId32;
use std::str::FromStr;
use std::fmt::{self, Display};

/// PDU - Polkadot runtime storage analyzer.
#[derive(Parser)]
pub struct Grep {
	/// Name of the network to analyze.
	#[clap(short, long, alias = "snap")]
	snapshot: String,

	/// URI of an Archive node endpoint.
	#[clap(long, alias = "url")]
	rpc: String,

	#[clap(long, global = true)]
	ignore_pallet: Option<String>,

	#[clap(subcommand)]
	search: GrepSearch,
}

#[derive(Parser)]
pub enum GrepSearch {
	Address(GrepSearchAddress),
	ParaAccount(GrepSearchParaAccount),
}

impl GrepSearch {
	pub fn subjects(&self) -> Result<Vec<Subject>> {
		match self {
			GrepSearch::Address(search) => search.subjects(),
			GrepSearch::ParaAccount(search) => search.subjects(),
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
	pub fn subjects(&self) -> Result<Vec<Subject>> {
		let address = AccountId32::from_str(&self.ss58).map_err(|_| anyhow!("Invalid SS58"))?;
		Ok(vec![Subject::Address(address.to_raw_vec())])
	}
}

#[derive(clap::ValueEnum, Clone, Copy)]
pub enum ParaLocation {
	Child,
	Sibling,
}

impl Display for ParaLocation {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", match self {
			ParaLocation::Child => "Child",
			ParaLocation::Sibling => "Sibling",
		})
	}
}

#[derive(Parser)]
pub struct GrepSearchParaAccount {
	/// Location of the para account.
	#[clap(long, value_enum, index = 1)]
	para_location: ParaLocation,

	/// Para ID to search for.
	#[clap(long, index = 2, num_args = 1.., value_delimiter = ',')]
	para_ids: Vec<u16>,

	/// Path to a JSON file containing para IDs.
	#[clap(long, conflicts_with = "para_id")]
	para_ids_from_json: Option<String>,
}

impl GrepSearchParaAccount {
	pub fn subjects(&self) -> Result<Vec<Subject>> {
		let para_ids = self.para_ids()?;
		let mut subjects = Vec::new();

		for para_id in para_ids.iter() {
			let address = para_id_to_address(self.para_location, *para_id);
			subjects.push(Subject::ParaAccount(self.para_location, *para_id, address));
		}

		Ok(subjects)
	}

	fn para_ids(&self) -> Result<Vec<u16>> {
		if let Some(path) = &self.para_ids_from_json {
			self.para_ids_from_json(path)
		} else {
			Ok(self.para_ids.clone())
		}
	}

	fn para_ids_from_json(&self, path: &str) -> Result<Vec<u16>> {
		let file = std::fs::File::open(path)?;
		let reader = std::io::BufReader::new(file);
		let ids: Vec<u16> = serde_json::from_reader(reader)?;
		Ok(ids)
	}
}

fn para_id_to_address(location: ParaLocation, para_id: u16) -> Vec<u8> {
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

		let subjects = self.search.subjects()?;
		log::info!("Searching for {} subjects:\n{}", subjects.len(), subjects.iter().map(|s| s.to_string()).collect::<Vec<_>>().join("\n"));

		self.search_subjects(rx, prefix_lookup, subjects).await
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
	ParaAccount(ParaLocation, u16, Vec<u8>),
}

impl Display for Subject {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Subject::Address(address) => write!(f, "Address({})", to_ss58(address)),
			Subject::ParaAccount(location, id, address) => write!(f, "ParaAccount({}, {}, {})", location, id, to_ss58(address)),
		}
	}
}

impl Subject {
	pub fn matches(&self, data: &[u8]) -> bool {
		match self {
			Subject::Address(address) => is_substr(data, address).is_some(),
			Subject::ParaAccount(_, _, address) => is_substr(data, address).is_some(),
		}
	}
}

impl Grep {
	pub async fn search_subjects(
		&self,
		mut rx: Receiver<Option<(Vec<u8>, Vec<u8>)>>,
		prefix_lookup: PrefixMap,
		subjects: Vec<Subject>,
	) -> Result<()> {
		let mut count = 0;
		let mut total = 0;

		while let Some(Some((key, value))) = rx.recv().await {
			total += 1;

			let (mut found_in_key, mut found_in_value) = (None, None);

			for subject in subjects.iter() {
				if subject.matches(&key) {
					found_in_key = Some(subject);
				}
				if subject.matches(&value) {
					found_in_value = Some(subject);
				}
			}

			let (k, v) = if let (Some(key_subject), Some(value_subject)) = (found_in_key, found_in_value) {
				(Some((key_subject, &key)), Some((value_subject, &value)))
			} else if let Some(key_subject) = found_in_key {
				(Some((key_subject, &key)), None)
			} else if let Some(value_subject) = found_in_value {
				(None, Some((value_subject, &value)))
			} else {
				continue;
			};

			count += 1;
			let info = categorize_prefix(&key, &prefix_lookup);

			if self.is_ignored(&info) {
				continue;
			}

			if let Some((k_subject, k)) = k {
				println!("{}: KEY '{}' 0x{}", k_subject, info.name(), hex::encode(k));
			}

			if let Some((v_subject, v)) = v {
				println!("{}: VALUE '{}' 0x{} => 0x{}", v_subject, info.name(), hex::encode(key), hex::encode(v));
			}
		}

		println!("Matched {} times in {} entries", count, total);

		Ok(())
	}

	fn is_ignored(&self, key: &CategorizedKey) -> bool {
		match key {
			CategorizedKey::Item(pallet, _) => {
				self.ignore_pallet.as_ref().map(|p| p == pallet).unwrap_or(false)
			}
			_ => false,
		}
	}
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


fn to_ss58(address: &[u8]) -> String {
	use sp_application_crypto::Ss58Codec;
	let inner: [u8; 32] = address.try_into().unwrap();
	AccountId32::from(inner).to_ss58check()
}
