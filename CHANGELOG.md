# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-11-01

### Added

- `VelocityTracker` for tracking rolling transaction velocity across many entities.
- `BucketedRingBuffer`, a constant-time ring buffer with running totals and lazy bucket eviction.
- Out-of-order event handling.
- Optional idle-entity eviction via `idle_timeout`, keeping the entity registry bounded.
- `VelocityResult`, a frozen dataclass returned by `get_velocity`.
- Latency and memory benchmarks, enforced in CI on every push.
- A minimal Alpine `Dockerfile` for low-resource deployment.

### Removed

- The earlier monolithic prototype (rule engine and audit logger). The project is now
  deliberately scoped to a single, well-tested primitive — sliding-window velocity
  aggregation — rather than a broad framework.
