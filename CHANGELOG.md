# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.1] - 2024-02-25

- Fix: delete tmp directory when external sorted is used.

## [0.6.0] - 2024-02-19

- Potentially breaking: support for empty index instead of failing if the index is empty.

## [0.5.0] - 2022-08-02

- Breaking: renamed `Encodable` to `Serialize`
- Serde serialization wrapper

## [0.4.0] - 2020-12-23

### Changed

- Breaking: cleaner `Encodable` trait ([PR #6](https://github.com/appaquet/extindex-rs/pull/6/files#diff-3dcefa956e75e2171b83e5134b542405a2adb7909a16dc03fad7fd92e8e2d945L11))
- Moved to `memmap2` as `memmap` isn't supported anymore.
- Moved to `tempfile` as `tempdir` isn't supported anymore.
- Upgrade to `extsort` 0.4
