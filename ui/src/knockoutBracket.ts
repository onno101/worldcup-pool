/**
 * FIFA World Cup 2026 knockout bracket projection.
 *
 * Derives projected Round-of-32 matchups from the user's group-stage predictions.
 * The bracket definition follows the official FIFA draw structure (December 2025).
 *
 * 48 teams · 12 groups (A–L) · top 2 per group + best 8 third-place → 32 in knockout.
 */

import type { StandingRow } from "./groupStandings";
import type { MatchOut } from "./types";

/* ------------------------------------------------------------------ */
/*  Types                                                              */
/* ------------------------------------------------------------------ */

export type ProjectedTeam = {
  teamCode: string;
  teamName: string;
  group?: string;        // e.g. "A"  (absent for teams from confirmed matches)
  position?: 1 | 2 | 3;  // 1st, 2nd, 3rd in group
};

export type ProjectedR32Match = {
  matchKey: string;          // e.g. "M73"
  home: ProjectedTeam | null;
  away: ProjectedTeam | null;
  homeLabel: string;         // e.g. "1st A" or "3rd C"
  awayLabel: string;
  /** True if the actual DB match has confirmed teams that match this slot. */
  confirmed: boolean;
};

/* ------------------------------------------------------------------ */
/*  R32 bracket slot definitions                                       */
/* ------------------------------------------------------------------ */

type FixedSide = { pos: 1 | 2; group: string };
type ThirdSide = { pos: 3; pool: string[] };
type SlotSide = FixedSide | ThirdSide;

function isThirdSide(s: SlotSide): s is ThirdSide {
  return s.pos === 3;
}

type R32SlotDef = {
  matchKey: string;
  home: SlotSide;
  away: SlotSide;
};

/**
 * Official FIFA 2026 Round-of-32 bracket (match numbers M73–M88).
 *
 * Fixed matchups: winner/runner-up slots are deterministic.
 * Third-place slots list the eligible group pools; the actual assignment
 * depends on which 8 of the 12 groups produce a qualifying third-place team.
 */
const R32_SLOTS: R32SlotDef[] = [
  /* --- Quarter-bracket 1 (→ QF M97) --- */
  { matchKey: "M74", home: { pos: 1, group: "E" }, away: { pos: 3, pool: ["A","B","C","D","F"] } },
  { matchKey: "M77", home: { pos: 1, group: "I" }, away: { pos: 3, pool: ["C","D","F","G","H"] } },
  { matchKey: "M73", home: { pos: 2, group: "A" }, away: { pos: 2, group: "B" } },
  { matchKey: "M75", home: { pos: 1, group: "F" }, away: { pos: 2, group: "C" } },

  /* --- Quarter-bracket 2 (→ QF M99) --- */
  { matchKey: "M76", home: { pos: 1, group: "C" }, away: { pos: 2, group: "F" } },
  { matchKey: "M78", home: { pos: 2, group: "E" }, away: { pos: 2, group: "I" } },
  { matchKey: "M79", home: { pos: 1, group: "A" }, away: { pos: 3, pool: ["C","E","F","H","I"] } },
  { matchKey: "M80", home: { pos: 1, group: "L" }, away: { pos: 3, pool: ["E","H","I","J","K"] } },

  /* --- Quarter-bracket 3 (→ QF M98) --- */
  { matchKey: "M83", home: { pos: 2, group: "K" }, away: { pos: 2, group: "L" } },
  { matchKey: "M84", home: { pos: 1, group: "H" }, away: { pos: 2, group: "J" } },
  { matchKey: "M81", home: { pos: 1, group: "D" }, away: { pos: 3, pool: ["B","E","F","I","J"] } },
  { matchKey: "M82", home: { pos: 1, group: "G" }, away: { pos: 3, pool: ["A","E","H","I","J"] } },

  /* --- Quarter-bracket 4 (→ QF M100) --- */
  { matchKey: "M86", home: { pos: 1, group: "J" }, away: { pos: 2, group: "H" } },
  { matchKey: "M88", home: { pos: 2, group: "D" }, away: { pos: 2, group: "G" } },
  { matchKey: "M85", home: { pos: 1, group: "B" }, away: { pos: 3, pool: ["E","F","G","I","J"] } },
  { matchKey: "M87", home: { pos: 1, group: "K" }, away: { pos: 3, pool: ["D","E","I","J","L"] } },
];

/** Display order groups the 16 R32 matches into 4 quarter-brackets of 4. */
export const R32_QUARTER_LABELS = [
  "Upper left bracket",
  "Upper right bracket",
  "Lower left bracket",
  "Lower right bracket",
];

/* ------------------------------------------------------------------ */
/*  Bracket tree: R32 → R16 → QF → SF → Final                        */
/* ------------------------------------------------------------------ */

export const BRACKET_TREE = {
  roundOf16: [
    { matchKey: "M89", homeFrom: "M74", awayFrom: "M77" },
    { matchKey: "M90", homeFrom: "M73", awayFrom: "M75" },
    { matchKey: "M91", homeFrom: "M76", awayFrom: "M78" },
    { matchKey: "M92", homeFrom: "M79", awayFrom: "M80" },
    { matchKey: "M93", homeFrom: "M83", awayFrom: "M84" },
    { matchKey: "M94", homeFrom: "M81", awayFrom: "M82" },
    { matchKey: "M95", homeFrom: "M86", awayFrom: "M88" },
    { matchKey: "M96", homeFrom: "M85", awayFrom: "M87" },
  ],
  quarterFinals: [
    { matchKey: "M97", homeFrom: "M89", awayFrom: "M90" },
    { matchKey: "M98", homeFrom: "M93", awayFrom: "M94" },
    { matchKey: "M99", homeFrom: "M91", awayFrom: "M92" },
    { matchKey: "M100", homeFrom: "M95", awayFrom: "M96" },
  ],
  semiFinals: [
    { matchKey: "M101", homeFrom: "M97", awayFrom: "M98" },
    { matchKey: "M102", homeFrom: "M99", awayFrom: "M100" },
  ],
  final: [
    { matchKey: "M103", homeFrom: "M101", awayFrom: "M102" },
  ],
  thirdPlace: [
    { matchKey: "M104", homeFrom: "M101", awayFrom: "M102" },
  ],
} as const;

/* ------------------------------------------------------------------ */
/*  Third-place ranking & allocation                                   */
/* ------------------------------------------------------------------ */

type ThirdPlaceEntry = {
  group: string; // "A" … "L"
  row: StandingRow;
};

/**
 * Rank third-place teams across all 12 groups.  Returns all 12 sorted
 * (best first); the caller picks the top 8.
 *
 * FIFA tiebreakers: points → goal-difference → goals-for → alphabetical.
 */
function rankThirdPlaceTeams(
  allStandings: Map<string, StandingRow[]>,
): ThirdPlaceEntry[] {
  const thirds: ThirdPlaceEntry[] = [];
  for (const [gk, rows] of allStandings) {
    const letter = gk.replace(/^GROUP_/i, "");
    if (rows.length >= 3) {
      thirds.push({ group: letter, row: rows[2] }); // rank 3 (0-indexed)
    }
  }
  thirds.sort((a, b) => {
    if (b.row.points !== a.row.points) return b.row.points - a.row.points;
    if (b.row.goalDiff !== a.row.goalDiff) return b.row.goalDiff - a.row.goalDiff;
    if (b.row.goalsFor !== a.row.goalsFor) return b.row.goalsFor - a.row.goalsFor;
    return a.group.localeCompare(b.group);
  });
  return thirds;
}

/**
 * Allocate 8 qualifying third-place teams to R32 match slots via
 * constraint-satisfaction backtracking.
 *
 * Each slot has a pool of eligible groups.  Returns a mapping
 * matchKey → group letter, or null if no valid assignment exists.
 */
function allocateThirdPlace(
  qualifyingGroups: string[],
): Record<string, string> | null {
  const thirdSlots = R32_SLOTS.filter((s) => isThirdSide(s.away));
  const qSet = new Set(qualifyingGroups);

  // Sort slots by number of eligible candidates (most constrained first)
  const sortedSlots = [...thirdSlots].sort((a, b) => {
    const aPool = (a.away as ThirdSide).pool;
    const bPool = (b.away as ThirdSide).pool;
    const aElig = aPool.filter((g) => qSet.has(g)).length;
    const bElig = bPool.filter((g) => qSet.has(g)).length;
    return aElig - bElig;
  });

  const assigned: Record<string, string> = {};
  const used = new Set<string>();

  function solve(idx: number): boolean {
    if (idx >= sortedSlots.length) return true;
    const slot = sortedSlots[idx];
    const pool = (slot.away as ThirdSide).pool;
    const eligible = pool.filter((g) => qSet.has(g) && !used.has(g));
    for (const g of eligible) {
      assigned[slot.matchKey] = g;
      used.add(g);
      if (solve(idx + 1)) return true;
      used.delete(g);
    }
    delete assigned[slot.matchKey];
    return false;
  }

  return solve(0) ? { ...assigned } : null;
}

/* ------------------------------------------------------------------ */
/*  Main projection function                                           */
/* ------------------------------------------------------------------ */

function posLabel(pos: 1 | 2 | 3, group: string): string {
  const ordinal = pos === 1 ? "1st" : pos === 2 ? "2nd" : "3rd";
  return `${ordinal} ${group}`;
}

function lookupTeam(
  allStandings: Map<string, StandingRow[]>,
  group: string,
  position: 1 | 2 | 3,
): ProjectedTeam | null {
  const gk = `GROUP_${group}`;
  const rows = allStandings.get(gk);
  if (!rows || rows.length < position) return null;
  const row = rows[position - 1]; // standings are sorted, 0-indexed
  if (!row || row.played === 0) return null; // no predictions entered for this group
  return {
    teamCode: row.teamCode,
    teamName: row.teamName,
    group,
    position,
  };
}

/**
 * Compute projected Round-of-32 matchups from the user's group-stage standings.
 *
 * @param allStandings  Map of "GROUP_X" → sorted StandingRow[]. Must include
 *                      all 12 groups for a complete projection; partial data yields
 *                      partial results (null teams for missing groups).
 * @param actualMatches All matches from the API (used to detect confirmed fixtures).
 */
export function computeProjectedR32(
  allStandings: Map<string, StandingRow[]>,
  actualMatches: MatchOut[],
): ProjectedR32Match[] {
  // ---- 1. Determine qualifying third-place teams ----
  const thirdRanked = rankThirdPlaceTeams(allStandings);
  const qualifying8 = thirdRanked.slice(0, 8).map((t) => t.group);
  const allocation = qualifying8.length === 8 ? allocateThirdPlace(qualifying8) : null;

  // ---- 2. Collect actual knockout matches for confirmation check ----
  const actualR32 = actualMatches.filter(
    (m) => normaliseStage(m.stage || "") === "LAST_32",
  );
  // Index by home+away codes for quick lookup
  const confirmedPairs = new Set(
    actualR32
      .filter((m) => m.home_team_code && m.away_team_code)
      .map((m) => `${m.home_team_code.toUpperCase()}|${m.away_team_code.toUpperCase()}`),
  );
  const hasAnyR32 = actualR32.length > 0;

  // ---- 3. Build projected matches ----
  const result: ProjectedR32Match[] = [];

  for (const slot of R32_SLOTS) {
    let home: ProjectedTeam | null = null;
    let homeLabel: string;
    let away: ProjectedTeam | null = null;
    let awayLabel: string;

    // Home side
    if (isThirdSide(slot.home)) {
      // Shouldn't happen in current bracket (3rd is always away), but handle it
      homeLabel = `3rd ?`;
      home = null;
    } else {
      const fs = slot.home as FixedSide;
      homeLabel = posLabel(fs.pos, fs.group);
      home = lookupTeam(allStandings, fs.group, fs.pos);
    }

    // Away side
    if (isThirdSide(slot.away)) {
      const pool = slot.away.pool;
      const allocatedGroup = allocation?.[slot.matchKey];
      if (allocatedGroup) {
        awayLabel = posLabel(3, allocatedGroup);
        away = lookupTeam(allStandings, allocatedGroup, 3);
      } else {
        awayLabel = `3rd ${pool.join("/")}`;
        away = null;
      }
    } else {
      const fs = slot.away as FixedSide;
      awayLabel = posLabel(fs.pos, fs.group);
      away = lookupTeam(allStandings, fs.group, fs.pos);
    }

    // Check confirmation: if actual R32 matches exist and one has these teams
    let confirmed = false;
    if (hasAnyR32 && home && away) {
      const key = `${home.teamCode.toUpperCase()}|${away.teamCode.toUpperCase()}`;
      confirmed = confirmedPairs.has(key);
    }

    result.push({ matchKey: slot.matchKey, home, away, homeLabel, awayLabel, confirmed });
  }

  return result;
}

/**
 * Check whether any group has at least one prediction filled in,
 * meaning we can show a (possibly partial) projected bracket.
 */
export function hasAnyGroupPredictions(
  allStandings: Map<string, StandingRow[]>,
): boolean {
  for (const rows of allStandings.values()) {
    if (rows.some((r) => r.played > 0)) return true;
  }
  return false;
}

/**
 * Count how many of the 12 groups are fully predicted (all 6 matches).
 */
export function countFullyPredictedGroups(
  allStandings: Map<string, StandingRow[]>,
): number {
  let full = 0;
  for (const rows of allStandings.values()) {
    // 4 teams × 3 matches each = 3 played per team in a full group
    if (rows.length >= 4 && rows.every((r) => r.played >= 3)) full++;
  }
  return full;
}

/**
 * Stage display order for knockout matches.
 * football-data.org uses LAST_32/LAST_16; we accept both variants.
 */
export const KNOCKOUT_STAGE_ORDER = [
  "LAST_32",
  "LAST_16",
  "QUARTER_FINALS",
  "SEMI_FINALS",
  "THIRD_PLACE",
  "FINAL",
];

export function stageDisplayName(stage: string): string {
  const map: Record<string, string> = {
    LAST_32: "Round of 32",
    ROUND_OF_32: "Round of 32",
    LAST_16: "Round of 16",
    ROUND_OF_16: "Round of 16",
    QUARTER_FINALS: "Quarter-finals",
    SEMI_FINALS: "Semi-finals",
    THIRD_PLACE: "Third-place play-off",
    FINAL: "Final",
  };
  return map[stage.toUpperCase()] ?? stage.replace(/_/g, " ");
}

/** Normalise both football-data.org and legacy stage names to canonical form. */
function normaliseStage(s: string): string {
  const u = s.toUpperCase();
  if (u === "ROUND_OF_32") return "LAST_32";
  if (u === "ROUND_OF_16") return "LAST_16";
  return u;
}

/* ------------------------------------------------------------------ */
/*  Bracket-slot ↔ DB-match mapping                                    */
/* ------------------------------------------------------------------ */

/** Match-key number ranges per round. FIFA numbers are chronological. */
const STAGE_KEY_RANGES: Record<string, { first: number; count: number }> = {
  LAST_32: { first: 73, count: 16 },
  LAST_16: { first: 89, count: 8 },
  QUARTER_FINALS: { first: 97, count: 4 },
  SEMI_FINALS: { first: 101, count: 2 },
  THIRD_PLACE: { first: 104, count: 1 },
  FINAL: { first: 103, count: 1 },
};

/**
 * Map every knockout DB match to its FIFA bracket slot (e.g. "M73").
 *
 * Within each stage, matches are sorted by kickoff time — FIFA assigns
 * match numbers chronologically, so sort order = match-key order.
 *
 * Returns matchId → matchKey.
 */
export function mapMatchesToBracketSlots(
  matches: MatchOut[],
): Map<string, string> {
  const result = new Map<string, string>();
  for (const [stage, range] of Object.entries(STAGE_KEY_RANGES)) {
    const staged = matches
      .filter((m) => normaliseStage(m.stage || "") === stage)
      .sort((a, b) => new Date(a.kickoff_utc).getTime() - new Date(b.kickoff_utc).getTime());
    for (let i = 0; i < staged.length && i < range.count; i++) {
      result.set(staged[i].id, `M${range.first + i}`);
    }
  }
  return result;
}

/* ------------------------------------------------------------------ */
/*  Full knockout projection (cascading)                               */
/* ------------------------------------------------------------------ */

export type KnockoutProjection = {
  projectedHome: ProjectedTeam | null;
  projectedAway: ProjectedTeam | null;
  homeLabel: string;
  awayLabel: string;
  /** True when the home team is derived from actual match data (not user predictions). */
  homeConfirmed: boolean;
  /** True when the away team is derived from actual match data (not user predictions). */
  awayConfirmed: boolean;
};

type DraftCell = { home: number | null; away: number | null; advance?: string | null };

/** Create a ProjectedTeam from an actual (confirmed) match side. */
function teamFromMatch(match: MatchOut, side: "home" | "away"): ProjectedTeam | null {
  const code = side === "home" ? match.home_team_code : match.away_team_code;
  const name = side === "home" ? match.home_team_name : match.away_team_name;
  if (!code || code === "?" || name === "TBD") return null;
  return { teamCode: code, teamName: name };
}

function matchIsTBD(m: MatchOut): boolean {
  return m.home_team_code === "?" || m.home_team_name === "TBD";
}

/**
 * Compute projected teams for ALL knockout matches (R32 through Final).
 *
 * - R32 matchups derive from group-stage standings **unless** the actual
 *   fixture is already confirmed, in which case the real teams are used.
 * - Later rounds cascade: each slot's team is the predicted (or actual)
 *   winner of its feeder match.
 * - Confirmed + finished matches always use the real winner, not the
 *   user's prediction — this keeps later-round projections accurate.
 * - Each side tracks `confirmed` so the UI can style teams that come from
 *   actual results differently from user-predicted ones.
 */
export function computeFullKnockoutProjections(
  allStandings: Map<string, StandingRow[]>,
  matches: MatchOut[],
  draft: Record<string, DraftCell>,
): Map<string, KnockoutProjection> {
  const result = new Map<string, KnockoutProjection>();
  const matchToBracket = mapMatchesToBracketSlots(matches);

  // Reverse map: matchKey → matchId
  const bracketToMatch = new Map<string, string>();
  for (const [mid, mk] of matchToBracket) bracketToMatch.set(mk, mid);

  // matchId lookup by id
  const matchById = new Map<string, MatchOut>();
  for (const m of matches) matchById.set(m.id, m);

  // ---- R32 projections ----
  const projectedR32 = computeProjectedR32(allStandings, matches);
  const r32ByKey = new Map<string, ProjectedR32Match>();
  for (const p of projectedR32) r32ByKey.set(p.matchKey, p);

  for (const [matchId, matchKey] of matchToBracket) {
    const r32 = r32ByKey.get(matchKey);
    if (!r32) continue; // not an R32 slot — handled in later rounds

    const match = matchById.get(matchId);
    if (match && !matchIsTBD(match)) {
      // Confirmed fixture → actual teams, both confirmed
      result.set(matchId, {
        projectedHome: teamFromMatch(match, "home"),
        projectedAway: teamFromMatch(match, "away"),
        homeLabel: r32.homeLabel,
        awayLabel: r32.awayLabel,
        homeConfirmed: true,
        awayConfirmed: true,
      });
    } else {
      // TBD → group-prediction-derived teams, not confirmed
      result.set(matchId, {
        projectedHome: r32.home,
        projectedAway: r32.away,
        homeLabel: r32.homeLabel,
        awayLabel: r32.awayLabel,
        homeConfirmed: false,
        awayConfirmed: false,
      });
    }
  }

  // ---- Helper: winner of a bracket-slot match ----
  type WinnerInfo = { team: ProjectedTeam; confirmed: boolean };

  function getWinner(matchKey: string): WinnerInfo | null {
    const matchId = bracketToMatch.get(matchKey);
    if (!matchId) return null;
    const proj = result.get(matchId);
    if (!proj) return null;
    const match = matchById.get(matchId);
    if (!match) return null;

    // Confirmed + finished → actual winner (always confirmed)
    if (!matchIsTBD(match) && match.status === "FINISHED" && match.winner_team_code) {
      const wc = match.winner_team_code.toUpperCase();
      const home = teamFromMatch(match, "home");
      const away = teamFromMatch(match, "away");
      if (home && match.home_team_code.toUpperCase() === wc) return { team: home, confirmed: true };
      if (away && match.away_team_code.toUpperCase() === wc) return { team: away, confirmed: true };
    }

    // Use user's draft prediction to determine winner (not confirmed)
    const cell = draft[matchId];
    if (!cell || cell.home == null || cell.away == null) return null;

    if (cell.home > cell.away && proj.projectedHome) {
      return { team: proj.projectedHome, confirmed: false };
    }
    if (cell.away > cell.home && proj.projectedAway) {
      return { team: proj.projectedAway, confirmed: false };
    }

    // Draw → penalty advance pick
    const adv = (cell.advance || "").trim().toUpperCase();
    if (!adv) return null;
    if (proj.projectedHome && adv === proj.projectedHome.teamCode.toUpperCase()) {
      return { team: proj.projectedHome, confirmed: false };
    }
    if (proj.projectedAway && adv === proj.projectedAway.teamCode.toUpperCase()) {
      return { team: proj.projectedAway, confirmed: false };
    }
    return null;
  }

  /** Loser of a bracket-slot match (opposite of getWinner). Used for third-place play-off. */
  function getLoser(matchKey: string): WinnerInfo | null {
    const matchId = bracketToMatch.get(matchKey);
    if (!matchId) return null;
    const proj = result.get(matchId);
    if (!proj) return null;
    const match = matchById.get(matchId);
    if (!match) return null;

    // Confirmed + finished → actual loser
    if (!matchIsTBD(match) && match.status === "FINISHED" && match.winner_team_code) {
      const wc = match.winner_team_code.toUpperCase();
      const home = teamFromMatch(match, "home");
      const away = teamFromMatch(match, "away");
      if (home && match.home_team_code.toUpperCase() === wc && away) return { team: away, confirmed: true };
      if (away && match.away_team_code.toUpperCase() === wc && home) return { team: home, confirmed: true };
    }

    // Use user's draft prediction to determine loser
    const cell = draft[matchId];
    if (!cell || cell.home == null || cell.away == null) return null;

    if (cell.home > cell.away && proj.projectedAway) {
      return { team: proj.projectedAway, confirmed: false };
    }
    if (cell.away > cell.home && proj.projectedHome) {
      return { team: proj.projectedHome, confirmed: false };
    }

    // Draw → penalty advance pick → loser is the other side
    const adv = (cell.advance || "").trim().toUpperCase();
    if (!adv) return null;
    if (proj.projectedHome && adv === proj.projectedHome.teamCode.toUpperCase() && proj.projectedAway) {
      return { team: proj.projectedAway, confirmed: false };
    }
    if (proj.projectedAway && adv === proj.projectedAway.teamCode.toUpperCase() && proj.projectedHome) {
      return { team: proj.projectedHome, confirmed: false };
    }
    return null;
  }

  // ---- R16 → QF → SF → Final (cascading) ----
  const laterRounds = [
    BRACKET_TREE.roundOf16,
    BRACKET_TREE.quarterFinals,
    BRACKET_TREE.semiFinals,
    BRACKET_TREE.final,
  ] as const;

  for (const round of laterRounds) {
    for (const slot of round) {
      const matchId = bracketToMatch.get(slot.matchKey);
      if (!matchId) continue;
      const match = matchById.get(matchId);

      if (match && !matchIsTBD(match)) {
        // Confirmed fixture → actual teams, both confirmed
        result.set(matchId, {
          projectedHome: teamFromMatch(match, "home"),
          projectedAway: teamFromMatch(match, "away"),
          homeLabel: `W ${slot.homeFrom}`,
          awayLabel: `W ${slot.awayFrom}`,
          homeConfirmed: true,
          awayConfirmed: true,
        });
      } else {
        // TBD → derive from bracket tree winners
        const homeInfo = getWinner(slot.homeFrom);
        const awayInfo = getWinner(slot.awayFrom);
        result.set(matchId, {
          projectedHome: homeInfo?.team ?? null,
          projectedAway: awayInfo?.team ?? null,
          homeLabel: `W ${slot.homeFrom}`,
          awayLabel: `W ${slot.awayFrom}`,
          homeConfirmed: homeInfo?.confirmed ?? false,
          awayConfirmed: awayInfo?.confirmed ?? false,
        });
      }
    }
  }

  // ---- Third-place play-off (losers of semi-finals) ----
  for (const slot of BRACKET_TREE.thirdPlace) {
    const matchId = bracketToMatch.get(slot.matchKey);
    if (!matchId) continue;
    const match = matchById.get(matchId);

    if (match && !matchIsTBD(match)) {
      result.set(matchId, {
        projectedHome: teamFromMatch(match, "home"),
        projectedAway: teamFromMatch(match, "away"),
        homeLabel: `L ${slot.homeFrom}`,
        awayLabel: `L ${slot.awayFrom}`,
        homeConfirmed: true,
        awayConfirmed: true,
      });
    } else {
      const homeInfo = getLoser(slot.homeFrom);
      const awayInfo = getLoser(slot.awayFrom);
      result.set(matchId, {
        projectedHome: homeInfo?.team ?? null,
        projectedAway: awayInfo?.team ?? null,
        homeLabel: `L ${slot.homeFrom}`,
        awayLabel: `L ${slot.awayFrom}`,
        homeConfirmed: homeInfo?.confirmed ?? false,
        awayConfirmed: awayInfo?.confirmed ?? false,
      });
    }
  }

  return result;
}
