# scale-compressed

Crate to compress [SCALE](https://crates.io/crates/parity-scale-codec) encoded data.

This crate is useful for compressing data that is sent over the network.

## Example

```rust
use scale_compressed::ScaleCompressed;
use parity_scale_codec::{Encode, Decode};

let compressed = ScaleCompressed::new(vec![1u8, 2, 3, 4, 5]);
let encoded = compressed.encode();
let decoded = ScaleCompressed::<Vec<u8>>::decode(&mut &encoded[..]).unwrap();
assert_eq!(vec![1, 2, 3, 4, 5], decoded.0);
```

License: GPL-3.0 OR Apache-2.0
