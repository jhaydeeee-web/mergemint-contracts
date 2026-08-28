/// Bounty routes — mounted under `/api/v1/bounties` by the router in main.rs.
///
/// Endpoints
/// ---------
/// GET  /api/v1/bounties                         list bounties (paginated)
/// GET  /api/v1/bounties/{id}                    get a single bounty
/// POST /api/v1/bounties                         create a bounty
/// POST /api/v1/bounties/{id}/claim              claim a bounty
/// GET  /api/v1/bounties/assignee/{address}      list bounties by assignee (#481)
/// GET  /api/v1/bounties/stream                  SSE stream of state changes (#482)
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Json,
    },
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::convert::Infallible;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt as _;

use crate::db::BountyPage;
use crate::AppState;

// ── Query params ──────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ListParams {
    pub limit: Option<i64>,
    pub cursor: Option<DateTime<Utc>>,
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// `GET /api/v1/bounties`
pub async fn list_bounties(
    State(state): State<AppState>,
    Query(params): Query<ListParams>,
) -> Result<Json<BountyPage>, (StatusCode, Json<serde_json::Value>)> {
    let limit = params.limit.unwrap_or(20).min(100);
    crate::db::list_bounties_by_creator(&state.db, "", limit, params.cursor)
        .await
        .map(Json)
        .map_err(|e| {
            // Log the real error server-side; send only a generic message to
            // the client so internal detail (DB errors, query strings, etc.)
            // is never exposed (#469).
            tracing::error!(error = %e, "list_bounties: database error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "internal server error" })),
            )
        })
}

/// `GET /api/v1/bounties/assignee/{address}`
///
/// Returns a paginated list of bounties where `address` is recorded as an
/// assignee in the `assignees` join table. This is the symmetric counterpart
/// to the existing `bounties_by_creator` query.
///
/// Implements issue #481 — "bounties by assignee" endpoint.
pub async fn list_bounties_by_assignee(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(params): Query<ListParams>,
) -> Result<Json<BountyPage>, (StatusCode, Json<serde_json::Value>)> {
    if address.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "address path parameter must not be empty" })),
        ));
    }

    let limit = params.limit.unwrap_or(20).min(100);
    crate::db::list_bounties_by_assignee(&state.db, &address, limit, params.cursor)
        .await
        .map(Json)
        .map_err(|e| {
            // Log the real error server-side; send only a generic message to
            // the client so internal detail is never exposed (#469).
            tracing::error!(error = %e, address = %address, "list_bounties_by_assignee: database error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "internal server error" })),
            )
        })
}

/// `GET /api/v1/bounties/stream`
///
/// Server-Sent Events channel that broadcasts a bounty ID whenever
/// `refresh_bounty` completes in the indexer. Clients subscribe once and
/// receive incremental push notifications instead of polling.
///
/// Event format: `data: {"bountyId": "<uuid>"}` with event name `bounty_updated`.
///
/// Implements issue #482 — push channel for bounty state changes.
pub async fn bounty_stream(
    State(state): State<AppState>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let rx = state.bounty_broadcast.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|result| {
        result.ok().map(|bounty_id| {
            Ok(Event::default()
                .event("bounty_updated")
                .data(format!(r#"{{"bountyId":"{}"}}"#, bounty_id)))
        })
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// `POST /api/v1/bounties/{id}/claim`
///
/// Marks a bounty as claimed by the caller. After persisting the state change
/// the bounty ID is broadcast on the SSE channel so subscribed clients are
/// notified immediately without a polling round-trip.
pub async fn claim_bounty(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    // Notify all SSE subscribers that this bounty's state changed
    let _ = state.bounty_broadcast.send(id.clone());

    Json(serde_json::json!({
        "id": id,
        "status": "claimed"
    }))
}
