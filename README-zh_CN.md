<div align="center">

<!-- <img src="https://raw.githubusercontent.com/al8n/txn/main/art/logo.png" height = "200px"> -->

<h1>Txn</h1>

</div>
<div align="center">

Generic transaction manager implementations for developing MVCC and ACID databases.

[<img alt="github" src="https://img.shields.io/badge/github-al8n/txn-8da0cb?style=for-the-badge&logo=Github" height="22">][Github-url]
<img alt="LoC" src="https://img.shields.io/endpoint?url=https%3A%2F%2Fgist.githubusercontent.com%2Fal8n%2F4bac4a768dd5583e656aeecb44867864%2Fraw%2Ftxn" height="22">
[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/al8n/txn/coverage.yml?logo=Github-Actions&style=for-the-badge" height="22">][CI-url]
[<img alt="codecov" src="https://img.shields.io/codecov/c/gh/al8n/txn?style=for-the-badge&token=6R3QFWRWHL&logo=codecov" height="22">][codecov-url]

[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-txn-66c2a5?style=for-the-badge&labelColor=555555&logo=data:image/svg+xml;base64,PHN2ZyByb2xlPSJpbWciIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgdmlld0JveD0iMCAwIDUxMiA1MTIiPjxwYXRoIGZpbGw9IiNmNWY1ZjUiIGQ9Ik00ODguNiAyNTAuMkwzOTIgMjE0VjEwNS41YzAtMTUtOS4zLTI4LjQtMjMuNC0zMy43bC0xMDAtMzcuNWMtOC4xLTMuMS0xNy4xLTMuMS0yNS4zIDBsLTEwMCAzNy41Yy0xNC4xIDUuMy0yMy40IDE4LjctMjMuNCAzMy43VjIxNGwtOTYuNiAzNi4yQzkuMyAyNTUuNSAwIDI2OC45IDAgMjgzLjlWMzk0YzAgMTMuNiA3LjcgMjYuMSAxOS45IDMyLjJsMTAwIDUwYzEwLjEgNS4xIDIyLjEgNS4xIDMyLjIgMGwxMDMuOS01MiAxMDMuOSA1MmMxMC4xIDUuMSAyMi4xIDUuMSAzMi4yIDBsMTAwLTUwYzEyLjItNi4xIDE5LjktMTguNiAxOS45LTMyLjJWMjgzLjljMC0xNS05LjMtMjguNC0yMy40LTMzLjd6TTM1OCAyMTQuOGwtODUgMzEuOXYtNjguMmw4NS0zN3Y3My4zek0xNTQgMTA0LjFsMTAyLTM4LjIgMTAyIDM4LjJ2LjZsLTEwMiA0MS40LTEwMi00MS40di0uNnptODQgMjkxLjFsLTg1IDQyLjV2LTc5LjFsODUtMzguOHY3NS40em0wLTExMmwtMTAyIDQxLjQtMTAyLTQxLjR2LS42bDEwMi0zOC4yIDEwMiAzOC4ydi42em0yNDAgMTEybC04NSA0Mi41di03OS4xbDg1LTM4Ljh2NzUuNHptMC0xMTJsLTEwMiA0MS40LTEwMi00MS40di0uNmwxMDItMzguMiAxMDIgMzguMnYuNnoiPjwvcGF0aD48L3N2Zz4K" height="20">][doc-url]
[<img alt="crates.io" src="https://img.shields.io/crates/v/txn?style=for-the-badge&logo=data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iaXNvLTg4NTktMSI/Pg0KPCEtLSBHZW5lcmF0b3I6IEFkb2JlIElsbHVzdHJhdG9yIDE5LjAuMCwgU1ZHIEV4cG9ydCBQbHVnLUluIC4gU1ZHIFZlcnNpb246IDYuMDAgQnVpbGQgMCkgIC0tPg0KPHN2ZyB2ZXJzaW9uPSIxLjEiIGlkPSJMYXllcl8xIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB4PSIwcHgiIHk9IjBweCINCgkgdmlld0JveD0iMCAwIDUxMiA1MTIiIHhtbDpzcGFjZT0icHJlc2VydmUiPg0KPGc+DQoJPGc+DQoJCTxwYXRoIGQ9Ik0yNTYsMEwzMS41MjgsMTEyLjIzNnYyODcuNTI4TDI1Niw1MTJsMjI0LjQ3Mi0xMTIuMjM2VjExMi4yMzZMMjU2LDB6IE0yMzQuMjc3LDQ1Mi41NjRMNzQuOTc0LDM3Mi45MTNWMTYwLjgxDQoJCQlsMTU5LjMwMyw3OS42NTFWNDUyLjU2NHogTTEwMS44MjYsMTI1LjY2MkwyNTYsNDguNTc2bDE1NC4xNzQsNzcuMDg3TDI1NiwyMDIuNzQ5TDEwMS44MjYsMTI1LjY2MnogTTQzNy4wMjYsMzcyLjkxMw0KCQkJbC0xNTkuMzAzLDc5LjY1MVYyNDAuNDYxbDE1OS4zMDMtNzkuNjUxVjM3Mi45MTN6IiBmaWxsPSIjRkZGIi8+DQoJPC9nPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPC9zdmc+DQo=" height="22">][crates-url]
[<img alt="crates.io" src="https://img.shields.io/crates/d/txn?color=critical&logo=data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBzdGFuZGFsb25lPSJubyI/PjwhRE9DVFlQRSBzdmcgUFVCTElDICItLy9XM0MvL0RURCBTVkcgMS4xLy9FTiIgImh0dHA6Ly93d3cudzMub3JnL0dyYXBoaWNzL1NWRy8xLjEvRFREL3N2ZzExLmR0ZCI+PHN2ZyB0PSIxNjQ1MTE3MzMyOTU5IiBjbGFzcz0iaWNvbiIgdmlld0JveD0iMCAwIDEwMjQgMTAyNCIgdmVyc2lvbj0iMS4xIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHAtaWQ9IjM0MjEiIGRhdGEtc3BtLWFuY2hvci1pZD0iYTMxM3guNzc4MTA2OS4wLmkzIiB3aWR0aD0iNDgiIGhlaWdodD0iNDgiIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIj48ZGVmcz48c3R5bGUgdHlwZT0idGV4dC9jc3MiPjwvc3R5bGU+PC9kZWZzPjxwYXRoIGQ9Ik00NjkuMzEyIDU3MC4yNHYtMjU2aDg1LjM3NnYyNTZoMTI4TDUxMiA3NTYuMjg4IDM0MS4zMTIgNTcwLjI0aDEyOHpNMTAyNCA2NDAuMTI4QzEwMjQgNzgyLjkxMiA5MTkuODcyIDg5NiA3ODcuNjQ4IDg5NmgtNTEyQzEyMy45MDQgODk2IDAgNzYxLjYgMCA1OTcuNTA0IDAgNDUxLjk2OCA5NC42NTYgMzMxLjUyIDIyNi40MzIgMzAyLjk3NiAyODQuMTYgMTk1LjQ1NiAzOTEuODA4IDEyOCA1MTIgMTI4YzE1Mi4zMiAwIDI4Mi4xMTIgMTA4LjQxNiAzMjMuMzkyIDI2MS4xMkM5NDEuODg4IDQxMy40NCAxMDI0IDUxOS4wNCAxMDI0IDY0MC4xOTJ6IG0tMjU5LjItMjA1LjMxMmMtMjQuNDQ4LTEyOS4wMjQtMTI4Ljg5Ni0yMjIuNzItMjUyLjgtMjIyLjcyLTk3LjI4IDAtMTgzLjA0IDU3LjM0NC0yMjQuNjQgMTQ3LjQ1NmwtOS4yOCAyMC4yMjQtMjAuOTI4IDIuOTQ0Yy0xMDMuMzYgMTQuNC0xNzguMzY4IDEwNC4zMi0xNzguMzY4IDIxNC43MiAwIDExNy45NTIgODguODMyIDIxNC40IDE5Ni45MjggMjE0LjRoNTEyYzg4LjMyIDAgMTU3LjUwNC03NS4xMzYgMTU3LjUwNC0xNzEuNzEyIDAtODguMDY0LTY1LjkyLTE2NC45MjgtMTQ0Ljk2LTE3MS43NzZsLTI5LjUwNC0yLjU2LTUuODg4LTMwLjk3NnoiIGZpbGw9IiNmZmZmZmYiIHAtaWQ9IjM0MjIiIGRhdGEtc3BtLWFuY2hvci1pZD0iYTMxM3guNzc4MTA2OS4wLmkwIiBjbGFzcz0iIj48L3BhdGg+PC9zdmc+&style=for-the-badge" height="22">][crates-url]
<img alt="license" src="https://img.shields.io/badge/License-MPL%202.0-blue.svg?style=for-the-badge&fontColor=white&logoColor=ffffff&logo=data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz4KPHN2ZyBpZD0iX+WbvuWxgl8xIiBkYXRhLW5hbWU9IuWbvuWxgiAxIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCA0NzMuNDcgMjU1LjEyMiI+CiAgPGRlZnM+CiAgICA8c3R5bGU+CiAgICAgIC5jbHMtMSB7CiAgICAgICAgZmlsbDogI2ZmZjsKICAgICAgICBzdHJva2Utd2lkdGg6IDBweDsKICAgICAgfQogICAgPC9zdHlsZT4KICA8L2RlZnM+CiAgPHBvbHlnb24gY2xhc3M9ImNscy0xIiBwb2ludHM9IjM0MC4wNjUgLjQ4NCAzMzQuMzczIDEuMjA5IDMyOC45MjkgMi4xNzUgMzIzLjczMSAzLjYyOCAzMTguNzggNS4zMTggMzEzLjgzMiA3LjAxMiAzMDkuMTI3IDkuMTkgMzA0LjY3MiAxMS42MDYgMzAwLjQ2NiAxNC4yNjggMjk2LjI1OSAxNy4xNjkgMjkyLjI5NyAyMC4zMTIgMjg4LjU4NiAyMy42OTcgMjg1LjEyIDI3LjMyNiAyODEuOTAyIDMxLjE5NCAyNzguNjg0IDM1LjMwNyAyNzUuOTYyIDM5LjQxNiAyNzMuMjQgNDQuMDExIDI3MC43NjUgNDguNjA3IDI2OC41MzkgNTMuNjg1IDI2Ni41NTkgNDguMzYzIDI2NC4wODMgNDMuMjg1IDI2MS42MDggMzguNDUxIDI1OC42MzkgMzMuODU0IDI1NS40MjIgMjkuNzQ0IDI1MS45NTUgMjUuODc1IDI0OC4yNDQgMjIuMjQ3IDI0NC41MzEgMTguODYzIDI0MC4zMjIgMTUuNzE5IDIzNi4xMTYgMTMuMDU5IDIzMS40MTQgMTAuMzk3IDIyNi45NTkgOC4yMjIgMjIyLjAwOCA2LjI4OCAyMTcuMDU3IDQuNTk0IDIxMi4xMDkgMy4xNDMgMjA2LjkxMSAxLjkzNCAyMDEuNDY0IC45NjggMTkwLjU3NiAwIDE3OS45MzIgMCAxNzQuNzM1IC40ODQgMTY5Ljc4NiAuOTY4IDE2NC44MzUgMS42OTMgMTYwLjEzNCAyLjkwMyAxNTUuNjc4IDQuMTEyIDE1MS4yMjMgNS41NjIgMTQ2Ljc2NyA3LjI1MyAxNDIuODA3IDkuMTkgMTM4LjYwMiAxMS4xMjUgMTM0Ljg4OCAxMy41NDEgMTMxLjE3NSAxNS45NiAxMjcuNzExIDE4LjYxOSAxMjQuMjQ0IDIxLjUyMiAxMjEuMDI5IDI0LjY2NiAxMTguMDU4IDI3LjgwOSAxMTUuMDg3IDMxLjE5NCAxMTIuMzY1IDM0LjgyMiAxMDkuODg5IDM4LjY5MSAxMDcuNjYzIDQyLjU2IDEwNy42NjMgNS4wNzggMCA1LjA3OCAwIDU4Ljc2NCAzMy45MDcgNTguNzY0IDMzLjkwNyAyMDAuNDcgMCAyMDAuNDcgMCAyNTUuMTIyIDE1Ni42NjcgMjU1LjEyMiAxNTYuNjY3IDIwMC40NyAxMDcuNjYzIDIwMC40NyAxMDcuNjYzIDEwOC4zMzcgMTA3LjkwOSAxMDMuMjU5IDEwOC42NTIgOTguNDIxIDEwOS4zOTYgOTMuODI3IDExMC4zODUgODkuNDc0IDExMS42MjMgODUuMTIxIDExMy4xMDcgODEuMjUyIDExNC44NCA3Ny4zODMgMTE2LjgyIDczLjk5OCAxMTkuMDQ3IDcwLjYxMSAxMjEuNzcyIDY3LjcxMSAxMjQuNDk0IDY1LjA1MSAxMjcuNDYxIDYyLjYzMyAxMzAuOTI5IDYwLjQ1NSAxMzQuNjM5IDU4LjUyIDEzOC4zNTIgNTcuMDcgMTQyLjU2MSA1NS44NiAxNDcuMjYzIDU0Ljg5NSAxNTEuOTY1IDU0LjQxIDE1Ny4xNjIgNTQuMTY3IDE2MS4zNzEgNTQuMTY3IDE2NS4zMzEgNTQuNjUxIDE2OS4yOTEgNTUuMzc2IDE3Mi43NTUgNTYuMzQ1IDE3Ni4yMjEgNTcuNTU0IDE3OS40MzkgNTkuMDA1IDE4Mi40MDggNjAuNjk4IDE4NS4xMyA2Mi44NzMgMTg3LjYwNSA2NS4yOTIgMTkwLjA4MSA2Ny45NTIgMTkyLjA2MSA3MC44NTQgMTk0LjA0MSA3NC4yMzkgMTk1LjUyNCA3OC4xMDggMTk3LjAxMSA4MS45NzcgMTk4LjI0OSA4Ni41NzQgMTk5LjIzOCA5MS4xNjcgMTk5Ljk4IDk2LjI0NiAyMDAuNzIyIDEwMS44MDkgMjAwLjk3MSAxMDcuODUyIDIwMC45NzEgMjU1LjEyMiAzMDcuMzk3IDI1NS4xMjIgMzA3LjM5NyAyMDAuNDcgMjczLjQ4NyAyMDAuNDcgMjczLjQ4NyAxMTMuNDE1IDI3My43MzYgMTA4LjMzNyAyNzMuOTgzIDEwMy4yNTkgMjc0LjQ3OCA5OC40MjEgMjc1LjQ2NiA5My44MjcgMjc2LjQ1OCA4OS40NzQgMjc3LjY5NiA4NS4xMjEgMjc5LjE4IDgxLjI1MiAyODAuOTEzIDc3LjM4MyAyODIuODk0IDczLjk5OCAyODUuMTIgNzAuNjExIDI4Ny41OTUgNjcuNzExIDI5MC41NjcgNjUuMDUxIDI5My41MzQgNjIuNjMzIDI5Ny4wMDIgNjAuNDU1IDMwMC40NjYgNTguNTIgMzA0LjQyNSA1Ny4wNyAzMDguNjM1IDU1Ljg2IDMxMy4zMzYgNTQuODk1IDMxOC4wMzggNTQuNDEgMzIzLjIzNSA1NC4xNjcgMzI3LjQ0NCA1NC4xNjcgMzMxLjQwNCA1NC42NTEgMzM1LjM2NCA1NS4zNzYgMzM4LjgyOCA1Ni4zNDUgMzQyLjI5MiA1Ny41NTQgMzQ1LjUwOSA1OS4wMDUgMzQ4LjQ4MSA2MC42OTggMzUxLjIwMyA2Mi44NzMgMzUzLjY3OCA2NS4yOTIgMzU1LjkwNCA2Ny45NTIgMzU4LjEzMyA3MC44NTQgMzYwLjExNCA3NC4yMzkgMzYzLjA4MiA4MS45NzcgMzY0LjMyIDg2LjU3NCAzNjUuMzExIDkxLjE2NyAzNjYuMDUzIDk2LjI0NiAzNjYuNzk1IDEwMS44MDkgMzY3LjA0NCAxMDcuODUyIDM2Ny4wNDQgMjU1LjEyMiA0NzMuNDcgMjU1LjEyMiA0NzMuNDcgMjAwLjQ3IDQzOS41NiAyMDAuNDcgNDM5LjU2IDg2LjMzIDQzOS4zMTMgNzcuNjI0IDQzOC4zMjIgNjkuNjQ1IDQzNi44MzggNjEuOTA3IDQzNC44NTggNTQuNjUxIDQzMi4zODMgNDcuODgyIDQyOS40MTQgNDEuNTk1IDQyNS45NDggMzUuNzg4IDQyMS45ODggMzAuMjI5IDQxNy41MzIgMjUuMzkxIDQxMy4wNzcgMjAuNzk3IDQwNy44OCAxNi45MjggNDAyLjY4MiAxMy4zIDM5Ni45OTEgMTAuMTU3IDM5MS4wNTIgNy4yNTMgMzg0Ljg2MyA1LjA3OCAzNzguNjc0IDMuMTQzIDM3MS45OTMgMS42OTMgMzY1LjU1OCAuNzI1IDM1OC42MjkgMCAzNDUuNzU5IDAgMzQwLjA2NSAuNDg0Ii8+Cjwvc3ZnPg==" height="22">


[English][en-url] | 简体中文

</div>

## Installation

```toml
[dependencies]
crates = "0.1"
```

## Example

```rust
use crates::example;

fn main() {
  
}
```

## Tests

- `test`:

    ```sh
    cargo test --all-features
    ```

- `miri`:

    ```sh
    cargo miri test --all-features
    ```

## Support Platforms

| targets                       |   status  |
|:-----------------------------:|:---------:|
| aarch64-linux-android         |  &#9989;  |
| aarch64-unknown-linux-gnu     |  &#9989;  |
| aarch64-unknown-linux-musl    |  &#9989;  |
| i686-pc-windows-gnu           |  &#9989;  |
| i686-linux-android            |  &#9989;  |
| i686-unknown-linux-gnu        |  &#9989;  |
| mips64-unknown-linux-gnuabi64 |  &#9989;  |
| powerpc64-unknown-linux-gnu   |  &#9989;  |
| riscv64gc-unknown-linux-gnu   |  &#9989;  |
| wasm32-unknown-unknown        |  &#9989;  |
| wasm32-unknown-emscripten     |  &#9989;  |
| x86_64-unknown-linux-gnu      |  &#9989;  |
| x86_64-pc-windows-gnu         |  &#9989;  |
| x86_64-linux-android          |  &#9989;  |

## TODO (help wanted)

- [x] Done
- [ ] WIP

#### License

`txn` is under the terms of both the MIT license and the
Apache License (Version 2.0).

See [LICENSE-APACHE](LICENSE-APACHE), [LICENSE-MIT](LICENSE-MIT) for details.

Copyright (c) 2024 Al Liu.

[Github-url]: https://github.com/al8n/crates/
[CI-url]: https://github.com/al8n/crates/actions/workflows/ci.yml
[doc-url]: https://docs.rs/crates
[crates-url]: https://crates.io/crates/crates
[codecov-url]: https://app.codecov.io/gh/al8n/crates/
[rustc-url]: https://github.com/rust-lang/rust/blob/master/RELEASES.md
[license-apache-url]: https://opensource.org/licenses/Apache-2.0
[license-mit-url]: https://opensource.org/licenses/MIT
[en-url]: https://github.com/al8n/txn/tree/main/README.md
