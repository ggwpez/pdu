# polkadot-du

Investigate storage size of Substrate chains.

Install with: 

```sh
cargo install polkadot-du
pdu --help
```

### Example

First acquire a state snapshot. We are going to use the People Rococo chain, since it is rather
small. You will need the
[try-runtime-cli](https://paritytech.github.io/try-runtime-cli/try_runtime/) for this and an
archive node to download the state from:

```sh
try-runtime create-snapshot --uri wss://rococo-people-rpc.polkadot.io:443 rococo-people.snap
```

Then run the analysis:

```sh
cargo run --release -- --network rococo-people
```

The results will be a bit boring for such a small network, but for a larger one - eg Kusama - it
could look like this. You can download [this snapshot](https://tasty.limo/kusama.snap) to try it.

![Kusama storage analysis](./.images/ksm-overview.png)

You can also zoom in on a specific pallet:

```sh
cargo run --release -- --network rococo-people --pallet Balances
```

Again for Kusama:

![Kusama Balances pallet](./.images/ksm-zoom.png)

### License

GPLv3 ONLY, see [LICENSE](./LICENSE) file for details.

License: GPL-3.0
