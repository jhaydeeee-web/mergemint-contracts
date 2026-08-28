// mergemint-backend/src/db.rs
//
// Database connection pool and shared state helpers.
//
// ## Lock-poison recovery (#473)
//
// Rust's lock APIs return `Err(PoisonError)` if a thread panicked while
// holding the lock. Calling `.unwrap()` would re-panic every subsequent
// caller, effectively taking the whole service down for what is often a
// transient edge case.
//
// We use `.unwrap_or_else(|e| e.into_inner())` instead: when the lock is
// poisoned we recover the inner value and continue under the assumption that
// the data is still in a consistent-enough state to serve requests.  If the
// data truly is corrupt the next business-logic validation will catch it and
// return an error to the client rather than crashing the process.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Lightweight in-memory store used during development / integration tests.
/// Production deployments replace this with a real database pool.
#[derive(Debug, Default)]
pub struct DbStore {
    pub records: HashMap<String, String>,
}

/// Shared, thread-safe handle to the database store.
///
/// The store uses an `RwLock` so API read paths can proceed concurrently while
/// writes still take exclusive access. This mirrors the production goal of
/// avoiding a single mutex-guarded connection that serializes every DB access.
pub type SharedDb = Arc<RwLock<DbStore>>;

/// Create a new, empty shared database handle.
pub fn new_shared_db() -> SharedDb {
    Arc::new(RwLock::new(DbStore::default()))
}

/// Acquire the database write lock, recovering gracefully from lock poison.
///
/// If a previous thread panicked while holding this lock, `.into_inner()`
/// extracts the guarded value so the service can keep running instead of
/// propagating the panic to every subsequent request.
#[allow(dead_code)]
pub fn acquire_db(db: &SharedDb) -> std::sync::RwLockWriteGuard<'_, DbStore> {
    db.write().unwrap_or_else(|e| e.into_inner())
}

/// Acquire the database read lock, recovering gracefully from lock poison.
pub fn read_db(db: &SharedDb) -> std::sync::RwLockReadGuard<'_, DbStore> {
    db.read().unwrap_or_else(|e| e.into_inner())
}

// ---------------------------------------------------------------------------
// Bounty listing (backs `GET /api/v1/bounties` and the assignee variant)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bounty {
    pub id: String,
    pub creator: String,
    pub assignee: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct BountyPage {
    pub bounties: Vec<Bounty>,
    pub next_cursor: Option<DateTime<Utc>>,
}

/// Shared paging logic: filter the in-memory store's JSON-encoded records by
/// `matches`, newest-first, applying the same cursor/limit contract used
/// elsewhere in the API (`created_at < cursor`, page size `limit`).
fn list_bounties_where(
    db: &SharedDb,
    limit: i64,
    cursor: Option<DateTime<Utc>>,
    matches: impl Fn(&Bounty) -> bool,
) -> anyhow::Result<BountyPage> {
    let guard = read_db(db);
    let mut bounties: Vec<Bounty> = guard
        .records
        .values()
        .filter_map(|raw| serde_json::from_str::<Bounty>(raw).ok())
        .filter(|b| matches(b))
        .filter(|b| cursor.is_none_or(|c| b.created_at < c))
        .collect();
    bounties.sort_by(|a, b| b.created_at.cmp(&a.created_at));

    let next_cursor = if bounties.len() > limit as usize {
        bounties.truncate(limit as usize);
        bounties.last().map(|b| b.created_at)
    } else {
        None
    };

    Ok(BountyPage {
        bounties,
        next_cursor,
    })
}

/// List bounties created by `creator`, newest first. Backs
/// `GET /api/v1/bounties`.
pub async fn list_bounties_by_creator(
    db: &SharedDb,
    creator: &str,
    limit: i64,
    cursor: Option<DateTime<Utc>>,
) -> anyhow::Result<BountyPage> {
    list_bounties_where(db, limit, cursor, |b| {
        creator.is_empty() || b.creator == creator
    })
}

/// List bounties where `assignee` appears as the bounty's assignee, newest
/// first. Backs `GET /api/v1/bounties/assignee/{address}`.
pub async fn list_bounties_by_assignee(
    db: &SharedDb,
    assignee: &str,
    limit: i64,
    cursor: Option<DateTime<Utc>>,
) -> anyhow::Result<BountyPage> {
    list_bounties_where(db, limit, cursor, |b| {
        b.assignee.as_deref() == Some(assignee)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Placeholder test — verifies that the test harness compiles and is wired
    /// correctly.  See issue #487.
    #[test]
    fn it_compiles() {}

    #[test]
    fn test_acquire_db_normal() {
        let db = new_shared_db();
        let mut guard = acquire_db(&db);
        guard.records.insert("key".to_string(), "value".to_string());
        assert_eq!(guard.records.get("key").map(|s| s.as_str()), Some("value"));
    }

    #[test]
    fn test_acquire_db_poison_recovery() {
        let db = new_shared_db();

        // Simulate a panic while holding the lock.
        let db_clone = Arc::clone(&db);
        let _ = std::panic::catch_unwind(move || {
            let _guard = db_clone.write().unwrap();
            panic!("simulated panic");
        });

        // The lock is now poisoned; acquire_db must not propagate the poison.
        let guard = acquire_db(&db);
        assert!(guard.records.is_empty(), "recovered store should be intact");
    }

    #[test]
    fn test_concurrent_read_guards_are_allowed() {
        let db = new_shared_db();
        let read_a = read_db(&db);
        let read_b = read_db(&db);

        assert!(read_a.records.is_empty());
        assert!(read_b.records.is_empty());
    }
}
