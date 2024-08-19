# Changelog

## [1.1.1](https://github.com/snakemake/snakemake-storage-plugin-gcs/compare/v1.1.0...v1.1.1) (2024-08-19)


### Bug Fixes

* allow both gs:// and gcs:// as query scheme; internally normalize to gs:// for compatibility with google cloud tools ([#52](https://github.com/snakemake/snakemake-storage-plugin-gcs/issues/52)) ([099e87f](https://github.com/snakemake/snakemake-storage-plugin-gcs/commit/099e87fcf47e59b5e911f72f50ca3c94aed7444f))

## [1.1.0](https://github.com/snakemake/snakemake-storage-plugin-gcs/compare/v1.0.0...v1.1.0) (2024-08-19)


### Features

* list storage candidates ([#30](https://github.com/snakemake/snakemake-storage-plugin-gcs/issues/30)) ([c7ba28e](https://github.com/snakemake/snakemake-storage-plugin-gcs/commit/c7ba28ec3a9cd1b6d2c39806ab785df57ec18f6d))


### Bug Fixes

* fixing the directory() issue due to uploading the local_prefix instead of GCS prefix.  ([#41](https://github.com/snakemake/snakemake-storage-plugin-gcs/issues/41)) ([27c80dc](https://github.com/snakemake/snakemake-storage-plugin-gcs/commit/27c80dce7ad9349e8ae8788984b1525c2478d575))

## [1.0.0](https://github.com/snakemake/snakemake-storage-plugin-gcs/compare/v0.1.4...v1.0.0) (2024-04-26)


### ⚠ BREAKING CHANGES

* expect correct google cloud storage abbreviation in query scheme (gcs://) ([#39](https://github.com/snakemake/snakemake-storage-plugin-gcs/issues/39))

### Bug Fixes

* expect correct google cloud storage abbreviation in query scheme (gcs://) ([#39](https://github.com/snakemake/snakemake-storage-plugin-gcs/issues/39)) ([0ebf52c](https://github.com/snakemake/snakemake-storage-plugin-gcs/commit/0ebf52cc6131fe092f638306f104e4c37a88aac4))
* fix directory support ([#38](https://github.com/snakemake/snakemake-storage-plugin-gcs/issues/38)) ([ce3d165](https://github.com/snakemake/snakemake-storage-plugin-gcs/commit/ce3d165f94e2d9d8f9469434d88edc0fe1b7f2a1))


### Documentation

* add intro, fix link ([6ec568a](https://github.com/snakemake/snakemake-storage-plugin-gcs/commit/6ec568a092aa6b636549a48fc09f0f1ba07b6f00))

## [0.1.4](https://github.com/snakemake/snakemake-storage-plugin-gcs/compare/v0.1.3...v0.1.4) (2024-03-08)


### Bug Fixes

* repair GCS query string ([#26](https://github.com/snakemake/snakemake-storage-plugin-gcs/issues/26)) ([f61e8d0](https://github.com/snakemake/snakemake-storage-plugin-gcs/commit/f61e8d0e3b83d3b03ad2eb41ceb0c5902345ef48))

## [0.1.3](https://github.com/snakemake/snakemake-storage-plugin-gcs/compare/v0.1.2...v0.1.3) (2023-12-20)


### Documentation

* update readme ([7d23319](https://github.com/snakemake/snakemake-storage-plugin-gcs/commit/7d233198eb911f7fb3f73176f2304681272dd080))

## [0.1.2](https://github.com/snakemake/snakemake-storage-plugin-gcs/compare/v0.1.1...v0.1.2) (2023-12-20)


### Bug Fixes

* relax towards older crc32c ([#7](https://github.com/snakemake/snakemake-storage-plugin-gcs/issues/7)) ([b99dfa0](https://github.com/snakemake/snakemake-storage-plugin-gcs/commit/b99dfa07cc4b9bebbc2126d8f725bcd544c91dcf))

## [0.1.1](https://github.com/snakemake/snakemake-storage-plugin-gcs/compare/v0.1.0...v0.1.1) (2023-12-08)


### Documentation

* update metadata ([cceaad1](https://github.com/snakemake/snakemake-storage-plugin-gcs/commit/cceaad1c9795cc95c4d420b2ee2ebe0c7fdd5b0d))

## 0.1.0 (2023-12-07)


### Miscellaneous Chores

* release 0.1.0 ([6709181](https://github.com/snakemake/snakemake-storage-plugin-gcs/commit/67091814a0b44107809162b6eb6d9178745d8afa))
