export type MatchOut = {
  id: string;
  external_match_id: string;
  competition_code: string;
  stage: string | null;
  matchday: number | null;
  group_key: string | null;
  home_team_code: string;
  away_team_code: string;
  home_team_name: string;
  away_team_name: string;
  kickoff_utc: string;
  prediction_deadline_utc: string;
  status: string;
  home_score: number | null;
  away_score: number | null;
  /** Team that advanced (knockouts; includes penalty winner when AET was a draw). */
  winner_team_code?: string | null;
  prediction_open: boolean;
  pred_home_goals: number | null;
  pred_away_goals: number | null;
  /** Knockout draw: saved team that advances (penalties). */
  pred_advance_team_code?: string | null;
  /** Awarded points this match (finished + saved line); from GET /api/matches. */
  points_outcome?: number;
  points_exact?: number;
  points_scorer_goals?: number;
  points_advancer?: number;
};

export type PoolSummaryOut = {
  total_matches: number;
  predicted_matches: number;
  next_deadline_utc: string | null;
  next_deadline_label: string | null;
};

export type TopScorerPickOut = {
  player_name: string;
  country_code: string;
  country_name: string;
  /** Scorer-goal points from finished matches (from tournament API). */
  points_awarded?: number;
};

export type PoolRankingEntryOut = {
  rank: number;
  user_id: string;
  display_name: string | null;
  email: string | null;
  profile_picture: string | null;
  match_predictions_filled: number;
  total_points: number;
  points_outcome: number;
  points_exact: number;
  points_scorer_goals: number;
  points_advancer: number;
  points_tournament_winner: number;
};

export type PoolRankingOut = { entries: PoolRankingEntryOut[] };

export type PoolDashboardOut = {
  predictors_count: number;
  leaderboard: PoolRankingOut;
};

export type PublicParticipantProfileOut = {
  user_id: string;
  display_name: string | null;
  nationality: string | null;
  profile_picture: string | null;
  updated_at: string | null;
  tournament_winner_team_code: string | null;
  top_scorers: TopScorerPickOut[];
  match_predictions_saved: number;
};

export type WorldcupPlayerOut = {
  player_name: string;
  country_code: string;
  country_name: string;
};

export type TournamentPredictionsOut = {
  tournament_winner_team_code: string | null;
  top_scorer_player_name: string | null;
  top_scorers: TopScorerPickOut[];
  notes_json: Record<string, unknown>;
  tournament_open: boolean;
  /** ISO8601 UTC — champion / top-scorer picks are read-only at or after this instant. */
  tournament_picks_lock_at_utc?: string;
  /** Hours before each match kickoff when that fixture locks (same env as match tab). */
  tournament_lock_hours_before_first_kickoff?: number;
  /** +25 when final is finished and pick matches champion. */
  points_tournament_winner?: number;
};

export type TeamOptionOut = { code: string; name: string };

export type MeOut = {
  user_id: string;
  email: string | null;
  /** Pool admin (ADMIN_EMAILS); can trigger match sync, etc. */
  is_admin?: boolean;
};

export type PutMatchPredictionsOut = {
  updated: number;
  errors: { match_id: string; detail: string }[];
};

export type UserProfileOut = {
  user_id: string;
  display_name: string | null;
  nationality: string | null;
  profile_picture: string | null;
  updated_at: string | null;
};

export type GroupRosterTeam = { team_code: string; team_name: string };

export type GroupRosterOut = {
  group_key: string;
  teams: GroupRosterTeam[];
  is_complete: boolean;
};

export type GroupRostersResponse = { groups: GroupRosterOut[] };
