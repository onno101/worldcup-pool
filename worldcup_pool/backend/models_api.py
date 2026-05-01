from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, model_validator

from worldcup_pool.pool_limits import MAX_TOP_SCORER_PICKS


class MatchPredictionIn(BaseModel):
    """Both null clears the saved prediction; both set saves scoreline (0–30 each)."""

    match_id: UUID
    home_goals: int | None = Field(default=None, ge=0, le=30)
    away_goals: int | None = Field(default=None, ge=0, le=30)
    """Knockout only: when the predicted line is a draw after extra time, which team advances (penalties)."""
    advance_team_code: str | None = Field(default=None, max_length=16)

    @model_validator(mode="after")
    def both_or_neither(self) -> MatchPredictionIn:
        if (self.home_goals is None) != (self.away_goals is None):
            raise ValueError("home_goals and away_goals must both be set or both null")
        return self


class PutMatchPredictionsIn(BaseModel):
    predictions: list[MatchPredictionIn]


class MatchPredictionError(BaseModel):
    match_id: UUID
    detail: str


class PutMatchPredictionsOut(BaseModel):
    updated: int
    errors: list[MatchPredictionError] = []


class TopScorerPickIn(BaseModel):
    player_name: str = Field(min_length=1, max_length=160)
    country_code: str = Field(min_length=1, max_length=16)


class TopScorerPickOut(BaseModel):
    player_name: str
    country_code: str
    country_name: str
    points_awarded: int = Field(
        0,
        description="Scorer-goal points from finished matches (2× in knockout); populated on tournament predictions API.",
    )


class WorldcupPlayerOut(BaseModel):
    player_name: str
    country_code: str
    country_name: str


class TournamentPredictionsIn(BaseModel):
    tournament_winner_team_code: str | None = None
    """Deprecated single field; prefer `top_scorers`. Kept for backward compatibility."""
    top_scorer_player_name: str | None = None
    top_scorers: list[TopScorerPickIn] = Field(default_factory=list)
    notes_json: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _cap_scorers(self) -> TournamentPredictionsIn:
        if len(self.top_scorers) > MAX_TOP_SCORER_PICKS:
            raise ValueError(f"At most {MAX_TOP_SCORER_PICKS} top scorer picks")
        return self


class MatchOut(BaseModel):
    id: UUID
    external_match_id: str
    competition_code: str
    stage: str | None
    matchday: int | None
    group_key: str | None = None
    home_team_code: str
    away_team_code: str
    home_team_name: str
    away_team_name: str
    kickoff_utc: datetime
    prediction_deadline_utc: datetime
    status: str
    home_score: int | None
    away_score: int | None
    winner_team_code: str | None = Field(
        default=None,
        description="After sync: team that advances from a knockout (incl. penalties when AET was a draw).",
    )
    prediction_open: bool
    pred_home_goals: int | None = None
    pred_away_goals: int | None = None
    pred_advance_team_code: str | None = Field(
        default=None,
        description="Knockout draw prediction: user-selected team that advances.",
    )
    points_outcome: int = Field(0, description="Points this match: correct outcome (not exact), if finished and saved.")
    points_exact: int = Field(0, description="Points this match: exact scoreline, if finished and saved.")
    points_scorer_goals: int = Field(0, description="Points this match: top-scorer picks who scored, if finished and saved.")
    points_advancer: int = Field(
        0,
        description="Knockout only: points for predicting the team that advances (2× in knockout).",
    )


class TournamentPredictionsOut(BaseModel):
    tournament_winner_team_code: str | None
    top_scorer_player_name: str | None
    top_scorers: list[TopScorerPickOut] = Field(default_factory=list)
    notes_json: dict[str, Any]
    tournament_open: bool
    tournament_picks_lock_at_utc: datetime = Field(
        ...,
        description="Champion / top-scorer picks become read-only at this instant (UTC).",
    )
    tournament_lock_hours_before_first_kickoff: int = Field(
        ...,
        description="Hours before each match kickoff when that fixture's score pick locks (match tab only).",
    )
    points_tournament_winner: int = Field(
        0,
        description="+25 when the final is finished and your pick matches the champion (not doubled).",
    )


class MeOut(BaseModel):
    user_id: str
    email: str | None = None
    is_admin: bool = False


class PoolSummaryOut(BaseModel):
    total_matches: int
    predicted_matches: int
    next_deadline_utc: datetime | None
    next_deadline_label: str | None = None


class TeamOptionOut(BaseModel):
    code: str
    name: str


class GroupRosterTeam(BaseModel):
    team_code: str
    team_name: str


class GroupRosterOut(BaseModel):
    """Teams in one World Cup group bucket from the schedule; is_complete when four teams are known."""

    group_key: str
    teams: list[GroupRosterTeam]
    is_complete: bool = False


class GroupRostersResponse(BaseModel):
    groups: list[GroupRosterOut]


class SyncOut(BaseModel):
    matches_synced: int


class PoolRankingEntryOut(BaseModel):
    rank: int
    user_id: str
    display_name: str | None = None
    email: str | None = None
    profile_picture: str | None = None
    """From pool profile when set."""
    match_predictions_filled: int = 0
    """Count of matches with a saved scoreline (both home and away goals set)."""
    total_points: int
    points_outcome: int
    points_exact: int
    points_scorer_goals: int
    points_advancer: int = Field(
        0,
        description="Sum of knockout advancer points (correct team to next round after ET).",
    )
    points_tournament_winner: int


class PoolRankingOut(BaseModel):
    entries: list[PoolRankingEntryOut]


class PoolDashboardOut(BaseModel):
    """Leaderboard payload for GET /api/dashboard (Leaderboard tab in the UI)."""

    predictors_count: int
    leaderboard: PoolRankingOut


class PublicParticipantProfileOut(BaseModel):
    """Read-only profile for anyone who has saved pool predictions (match or tournament)."""

    user_id: str
    display_name: str | None = None
    nationality: str | None = None
    profile_picture: str | None = None
    updated_at: datetime | None = None
    tournament_winner_team_code: str | None = None
    top_scorers: list[TopScorerPickOut] = Field(default_factory=list)
    match_predictions_saved: int = 0


class UserProfileIn(BaseModel):
    display_name: str | None = Field(default=None, max_length=120)
    nationality: str | None = Field(default=None, max_length=80)
    # Data URL generated from the uploaded file input (e.g. data:image/png;base64,...).
    profile_picture: str | None = Field(default=None, max_length=1_500_000)


class UserProfileOut(BaseModel):
    user_id: str
    display_name: str | None = None
    nationality: str | None = None
    profile_picture: str | None = None
    updated_at: datetime | None = None
