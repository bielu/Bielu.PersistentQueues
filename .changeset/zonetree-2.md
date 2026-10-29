---
"bielu-persistentqueues": major
---

Upgrade `ZoneTree` 1.9.8 → 2.0.5 (`Bielu.PersistentQueues.Storage.ZoneTree`) and `LightningDB` 0.23.0 → 0.23.1 (`Bielu.PersistentQueues.Storage.LMDB`).

ZoneTree 2.x is faster (about 1.48× across its reference benchmarks) and fixes crash-recovery, compressed-WAL repair and seek bugs. LightningDB 0.23.1 fixes a process crash on the finalizer thread when opening a cursor or transaction fails.

**Breaking (on-disk):** ZoneTree 2.0 switches WAL checksums from CRC32 to XXH3 and renames existing WAL files the first time a store is opened. Data written by 1.x is migrated automatically and reads back intact, but the migration is one-way: rolling back to a 1.x ZoneTree provider afterwards silently loses records that were still only in the WAL. Back up the data directory before upgrading if a rollback might be needed. The public API is unchanged.
