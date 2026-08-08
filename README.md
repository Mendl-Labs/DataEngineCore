# DataEngineCore

**Open-source market data ingestion core** — exchange WebSocket handling, order book replication, caching, and performance-hardened infrastructure for the TradingPlatform.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Workspace Crates](#workspace-crates)
- [What This Is Not](#what-this-is-not)
- [Extension Points](#extension-points)
- [Quick Start](#quick-start)
- [Sibling Repositories](#sibling-repositories)
- [Testing](#testing)
- [License](#license)

---

## Overview

DataEngineCore is the generic market-data infrastructure extracted from the platform's private data-ingestion SaaS. It:

1. **Connects** to exchange WebSocket feeds and normalizes market data (trades, orderbook deltas, tickers)
2. **Replicates order books** locally with low-latency update handling
3. **Publishes** normalized data onto the message bus (via `MessageBrokerEngine`'s `publisher`/`protocol` crates) for downstream consumers like `SignalEngine`
4. **Tracks per-tenant subscription entitlements** (symbol/exchange caps, data types, orderbook depth, retention) through a pluggable policy interface — no concrete pricing tiers ship in this crate
5. **Hardens performance** with CPU affinity pinning, SIMD-accelerated parsing, zero-copy buffer pools, and sub-microsecond metrics

### Key Characteristics

| Attribute | Value |
|-----------|-------|
| **Data sources** | Exchange WebSocket feeds (trades, orderbook, tickers) |
| **Output** | Published onto `MessageBrokerEngine` topics |
| **Multi-tenancy** | Generic bookkeeping only — concrete tier policy is caller-supplied |
| **Performance** | CPU affinity, SIMD parsing, zero-copy buffers, sub-µs metrics |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          DataEngineCore                                 │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │                     hostbuilder                                  │    │
│  │                                                                  │    │
│  │  ┌──────────┐  ┌──────────────┐  ┌────────────┐  ┌───────────┐  │    │
│  │  │  core     │  │    data       │  │  network    │  │ security   │  │    │
│  │  │           │  │               │  │             │  │            │  │    │
│  │  │ HostedObj │  │ caching       │  │ API         │  │ input      │  │    │
│  │  │ Subscrip- │  │ order_book    │  │ endpoints   │  │ validation │  │    │
│  │  │ tion mgmt │  │ pipeline      │  │ handlers    │  │ enterprise │  │    │
│  │  └──────────┘  └──────────────┘  └────────────┘  └───────────┘  │    │
│  │                                                                  │    │
│  │  ┌──────────────────┐          ┌────────────────────────────┐   │    │
│  │  │  infrastructure    │          │       optimization           │   │    │
│  │  │                    │          │                              │   │    │
│  │  │ monitoring          │          │ SIMD parsing                 │   │    │
│  │  │ resilience          │          │ kernel bypass                │   │    │
│  │  │ logging_facade      │          │ ultra_metrics                │   │    │
│  │  └──────────────────┘          └────────────────────────────┘   │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  ┌──────────────┐                              ┌─────────────────────┐ │
│  │    config     │                              │     performance       │ │
│  │               │                              │                       │ │
│  │ env-var driven│                              │ CPU affinity, SIMD,   │ │
│  │ validation    │                              │ zero-copy buffers,    │ │
│  │               │                              │ sub-µs metrics        │ │
│  └──────────────┘                              └─────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼  publishes normalized market data
                    ┌───────────────────────┐
                    │ MessageBrokerEngine    │
                    │ (publisher/protocol)   │
                    └───────────────────────┘
```

---

## Workspace Crates

| Crate | Purpose |
|-------|---------|
| `hostbuilder` | The engine itself — application structure, exchange data pipeline, order book replication, subscription/tenant bookkeeping, network API, security, and performance-optimization submodules (see below) |
| `config` | Environment-variable-driven configuration management and validation |
| `performance` | CPU affinity management, sub-microsecond metrics collection, zero-copy buffer pools, SIMD-accelerated operations |

### `hostbuilder` module organization

| Module | Purpose |
|--------|---------|
| `core` | `HostedObject`/`HostedObjectTrait` — the top-level application structure; `TenantSubscriptionLimiter` and the `TierLimits` policy trait |
| `data` | Exchange data pipeline, caching, order book maintenance, security/instrument metadata (`get_info`) |
| `network` | API endpoints and request handlers |
| `infrastructure` | Monitoring, resilience (retry/circuit-breaking), structured logging facade |
| `security` | Input validation, enterprise security manager, security-event monitoring |
| `optimization` | SIMD-accelerated message parsing, kernel-bypass networking hooks, ultra-low-latency metrics |

---

## What This Is Not

This crate ships **ingestion infrastructure, not pricing policy**. Notably absent, by design:

- **A concrete subscription tier table.** `core::TierLimits` is a trait with no default implementation — what "Free" vs "Enterprise" actually means (symbol/exchange caps, allowed data types, orderbook depth, priority routing, retention days) is a business decision you supply.
- **Multi-tenant billing or usage metering beyond raw counters.** `TenantMetrics`/`TenantSubscriptionStats` track counts for you to bill against; they don't implement billing itself.

If you're building a hosted product on top of this, construct your tier policy at your binary's entry point and pass it in — mirroring how the platform's own private SaaS binary consumes this crate.

---

## Extension Points

| Trait / Seam | Where | You supply |
|---|---|---|
| `hostbuilder::core::TierLimits` | `hostbuilder` crate | Concrete per-tier entitlement numbers — construct an implementation and pass it to `TenantSubscriptionLimiter::new()` / `HostedObject::new()` |

```rust
use hostbuilder::{HostedObject, TierLimits, SubscriptionTier};
use std::sync::Arc;

struct MyTierPolicy;
impl TierLimits for MyTierPolicy {
    fn max_symbols(&self, tier: SubscriptionTier) -> usize { /* ... */ 0 }
    fn max_exchanges(&self, tier: SubscriptionTier) -> usize { /* ... */ 0 }
    fn allowed_data_types(&self, tier: SubscriptionTier) -> Vec<&'static str> { vec![] }
    fn max_orderbook_depth(&self, tier: SubscriptionTier) -> i32 { 0 }
    fn has_priority_routing(&self, tier: SubscriptionTier) -> bool { false }
    fn data_retention_days(&self, tier: SubscriptionTier) -> u32 { 0 }
}

let engine = HostedObject::new(Arc::new(MyTierPolicy)).await;
```

---

## Quick Start

### Prerequisites

- Rust 1.82+
- `MessageBrokerEngine` running (data is published to it)
- Exchange API credentials for the feeds you want to ingest

### Build

```powershell
cd DataEngineCore
cargo build --workspace --release
```

### Run

DataEngineCore is a library workspace — there's no standalone binary here. A consuming binary constructs `HostedObject` with a `TierLimits` implementation and calls `.run()`:

```rust
use hostbuilder::{HostedObject, HostedObjectTrait};
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut engine = HostedObject::new(Arc::new(MyTierPolicy)).await;
    engine.run().await
}
```

---

## Sibling Repositories

Several crates have path dependencies on sibling repositories, expected to sit alongside `DataEngineCore` on disk:

```
TradingPlatform/
├── DataEngineCore/         (this repo)
├── BacktestingCore/         (derivatives — shared instrument types)
├── databaseschema/           (optional — persistence)
├── LoggingEngine/             (ultra-logger — structured logging)
└── MessageBrokerEngine/        (publisher/subscriber/protocol — message bus)
```

---

## Testing

```powershell
cargo test --workspace
```

---

## License

Functional Source License, Version 1.1, ALv2 Future License (FSL-1.1-ALv2) — see [LICENSE](LICENSE). Free for internal use, non-commercial research/education, and professional services; converts to Apache License 2.0 two years after each version's release. In short: use it, modify it, build a product on top of it — you just can't resell this engine itself (or a thin wrapper around it) as a directly competing hosted data-ingestion service.
