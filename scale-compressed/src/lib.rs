// SPDX-License-Identifier: GPL-3.0 OR Apache-2.0

//! Crate to compress [SCALE](https://crates.io/crates/parity-scale-codec) encoded data.
//!
//! This crate is useful for compressing data that is sent over the network.
//!
//! # Example
//!
//! ```rust
//! use scale_compressed::ScaleCompressed;
//! use parity_scale_codec::{Encode, Decode};
//!
//! let compressed = ScaleCompressed::new(vec![1u8, 2, 3, 4, 5]);
//! let encoded = compressed.encode();
//! let decoded = ScaleCompressed::<Vec<u8>>::decode(&mut &encoded[..]).unwrap();
//! assert_eq!(vec![1, 2, 3, 4, 5], decoded.0);
//! ```

use parity_scale_codec::{Decode, Encode, Error, Input, Output};

/// Wrap a struct to be compressed for encoding.
pub struct ScaleCompressed<T>(pub T);

impl<T> ScaleCompressed<T> {
	pub fn new(inner: T) -> Self {
		Self(inner)
	}
}

impl<T: Encode> Encode for ScaleCompressed<T> {
	fn encode_to<O: Output + ?Sized>(&self, output: &mut O) {
		let compressed: Vec<u8> =
			self.0.using_encoded(|buf| miniz_oxide::deflate::compress_to_vec(buf, 6));
		compressed.encode_to(output); // Double encode for the length prefix
	}
}

impl<T: Decode> Decode for ScaleCompressed<T> {
	fn decode<I: Input>(input: &mut I) -> Result<Self, Error> {
		let compressed = Vec::<u8>::decode(input)?;
		let decompressed =
			miniz_oxide::inflate::decompress_to_vec_with_limit(&compressed, 4 * 1024 * 1024)
				.map_err(|_| Error::from("Data corrupted"))?;
		drop(compressed);

		T::decode(&mut &decompressed[..]).map(Self)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::fmt::Debug;

	#[test]
	fn test_encode_decode() {
		test_works(vec![0; 0]);
		test_works(vec![1, 2, 3, 4, 5]);

		test_works(None::<u8>);
		test_works(Some(1));
		test_works(Some(vec![1, 2, 3, 4, 5]));
		test_works(Some(vec![0; 0]));

		test_works(1);
		test_works(1u8);
		test_works(1u16);
		test_works(1u32);
		test_works(1u64);
		test_works(1u128);

		test_works(true);
		test_works(false);

		test_works("hey".to_string());
	}

	fn test_works<T: Encode + Decode + Clone + PartialEq + Debug>(original: T) {
		let compressed = ScaleCompressed::new(original.clone());
		let encoded = compressed.encode();
		let decoded = ScaleCompressed::<T>::decode(&mut &encoded[..]).unwrap();
		assert_eq!(original, decoded.0);
	}
}
