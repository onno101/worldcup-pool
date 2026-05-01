"""Upsert matches from external API into Lakebase."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from worldcup_pool.backend.config import get_settings
from worldcup_pool.backend.db import (
    matches_table_has_winner_team_code_column,
    session_scope,
    try_add_match_predictions_advance_column,
    try_add_matches_goal_events_column,
    try_add_matches_group_key_column,
    try_add_matches_winner_team_column,
)
from worldcup_pool.football_data import FootballDataClient

logger = logging.getLogger(__name__)


def run_sync() -> int:
    """Pull matches from football-data.org and upsert. Returns number of rows processed."""
    settings = get_settings()
    client = FootballDataClient(settings.football_data_token)
    normalized = client.fetch_competition_matches(settings.football_data_competition)
    client.enrich_goal_events(normalized)
    now = datetime.now(timezone.utc)

    upsert_with_winner = text(
        """
        INSERT INTO matches (
            external_match_id, competition_code, stage, matchday, group_key,
            home_team_code, away_team_code, home_team_name, away_team_name,
            kickoff_utc, prediction_deadline_utc, status, home_score, away_score,
            winner_team_code, goal_events, last_synced_at
        ) VALUES (
            :external_match_id, :competition_code, :stage, :matchday, :group_key,
            :home_team_code, :away_team_code, :home_team_name, :away_team_name,
            :kickoff_utc, :prediction_deadline_utc, :status, :home_score, :away_score,
            :winner_team_code, CAST(:goal_events AS jsonb), :last_synced_at
        )
        ON CONFLICT (external_match_id) DO UPDATE SET
            stage = EXCLUDED.stage,
            matchday = EXCLUDED.matchday,
            group_key = EXCLUDED.group_key,
            home_team_code = EXCLUDED.home_team_code,
            away_team_code = EXCLUDED.away_team_code,
            home_team_name = EXCLUDED.home_team_name,
            away_team_name = EXCLUDED.away_team_name,
            kickoff_utc = EXCLUDED.kickoff_utc,
            prediction_deadline_utc = EXCLUDED.prediction_deadline_utc,
            status = EXCLUDED.status,
            home_score = EXCLUDED.home_score,
            away_score = EXCLUDED.away_score,
            winner_team_code = EXCLUDED.winner_team_code,
            goal_events = EXCLUDED.goal_events,
            last_synced_at = EXCLUDED.last_synced_at
        """
    )
    upsert_no_winner = text(
        """
        INSERT INTO matches (
            external_match_id, competition_code, stage, matchday, group_key,
            home_team_code, away_team_code, home_team_name, away_team_name,
            kickoff_utc, prediction_deadline_utc, status, home_score, away_score,
            goal_events, last_synced_at
        ) VALUES (
            :external_match_id, :competition_code, :stage, :matchday, :group_key,
            :home_team_code, :away_team_code, :home_team_name, :away_team_name,
            :kickoff_utc, :prediction_deadline_utc, :status, :home_score, :away_score,
            CAST(:goal_events AS jsonb), :last_synced_at
        )
        ON CONFLICT (external_match_id) DO UPDATE SET
            stage = EXCLUDED.stage,
            matchday = EXCLUDED.matchday,
            group_key = EXCLUDED.group_key,
            home_team_code = EXCLUDED.home_team_code,
            away_team_code = EXCLUDED.away_team_code,
            home_team_name = EXCLUDED.home_team_name,
            away_team_name = EXCLUDED.away_team_name,
            kickoff_utc = EXCLUDED.kickoff_utc,
            prediction_deadline_utc = EXCLUDED.prediction_deadline_utc,
            status = EXCLUDED.status,
            home_score = EXCLUDED.home_score,
            away_score = EXCLUDED.away_score,
            goal_events = EXCLUDED.goal_events,
            last_synced_at = EXCLUDED.last_synced_at
        """
    )

    with session_scope() as session:
        try_add_matches_group_key_column(session)
        try_add_matches_goal_events_column(session)
        try_add_matches_winner_team_column(session)
        try_add_match_predictions_advance_column(session)
        has_win = matches_table_has_winner_team_code_column(session)
        upsert_sql = upsert_with_winner if has_win else upsert_no_winner
        lock_h = settings.prediction_lock_before_kickoff_hours
        logger.info("run_sync: upserting %s matches into Lakebase", len(normalized))
        all_params: list[dict[str, object]] = []
        for m in normalized:
            deadline = m.kickoff_utc - timedelta(hours=lock_h)
            params: dict[str, object] = {
                "external_match_id": m.external_match_id,
                "competition_code": m.competition_code,
                "stage": m.stage,
                "matchday": m.matchday,
                "group_key": m.group_key,
                "home_team_code": m.home_team_code,
                "away_team_code": m.away_team_code,
                "home_team_name": m.home_team_name,
                "away_team_name": m.away_team_name,
                "kickoff_utc": m.kickoff_utc,
                "prediction_deadline_utc": deadline,
                "status": m.status,
                "home_score": m.home_score,
                "away_score": m.away_score,
                "goal_events": json.dumps(m.goal_events or []),
                "last_synced_at": now,
            }
            if has_win:
                params["winner_team_code"] = m.winner_team_code
            all_params.append(params)
        if all_params:
            session.execute(upsert_sql, all_params)
    return len(normalized)
