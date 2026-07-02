Run `cargo fmt` after editing Rust files.

When publishing a release, after `cargo publish` succeeds, create and push a git tag matching the crate version: `git tag v<version> && git push origin v<version>` (e.g. `v0.1.7`).
