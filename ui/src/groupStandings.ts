import type { MatchOut } from "./types";

/** Draft scores keyed by match id (same shape as App draft state). Null = empty input — not a prediction yet. */
export type DraftScores = Record<string, { home: number | null; away: number | null }>;

function draftLine(draft: DraftScores, matchId: string): { home: number; away: number } | null {
  const s = draft[matchId];
  if (!s) return null;
  const { home: h, away: a } = s;
  if (h == null || a == null) return null;
  return { home: h, away: a };
}

/** API row shape for {@link buildGroupStageMatchBuckets}. */
export type GroupRosterForBuckets = {
  group_key: string;
  teams: { team_code: string; team_name: string }[];
};

function normalizeGroupKey(g: string | null | undefined): string | null {
  const t = (g || "").trim();
  if (!t) return null;
  return t.toUpperCase();
}

const GROUP_LETTER_RE = /^GROUP_([A-Z])$/i;

/** Same mapping as `worldcup_pool/team_tla.py` — fold feed TLAs to official draw codes. */
const CANONICAL_TEAM_TLA: Record<string, string> = {
  CUR: "CUW",
  DEU: "GER",
  HOL: "NED",
  SCT: "SCO",
};

function canonicalTeamTla(code: string): string {
  const u = code.trim().toUpperCase();
  return CANONICAL_TEAM_TLA[u] ?? u;
}

/**
 * When the API returns official GROUP_A … rosters (World Cup), bucket GROUP_STAGE matches by
 * roster membership so the UI still splits tables even if synced fixtures omit `group_key`.
 * Returns null if there are not enough roster-defined groups to treat as authoritative.
 */
export function buildGroupStageMatchBuckets(
  matches: MatchOut[],
  apiGroups: GroupRosterForBuckets[],
): Map<string, MatchOut[]> | null {
  const official: { gk: string; codes: Set<string> }[] = [];
  for (const g of apiGroups) {
    const gk = normalizeGroupKey(g.group_key);
    if (!gk || !GROUP_LETTER_RE.test(gk)) continue;
    const codes = new Set(g.teams.map((t) => t.team_code));
    if (codes.size === 0) continue;
    official.push({ gk, codes });
  }
  if (official.length < 2) return null;

  const result = new Map<string, MatchOut[]>();
  for (const { gk } of official) result.set(gk, []);
  result.set("_OTHER", []);

  const byKick = (a: MatchOut, b: MatchOut) =>
    new Date(a.kickoff_utc).getTime() - new Date(b.kickoff_utc).getTime();

  const groupMatches = matches.filter((m) => (m.stage || "").toUpperCase() === "GROUP_STAGE");

  for (const m of groupMatches) {
    const hc = canonicalTeamTla(m.home_team_code);
    const ac = canonicalTeamTla(m.away_team_code);
    const dbGk = normalizeGroupKey(m.group_key);

    const rosterHits = official.filter((o) => o.codes.has(hc) && o.codes.has(ac));
    let target: string | null = null;

    if (rosterHits.length === 1) {
      target = rosterHits[0].gk;
    } else if (rosterHits.length > 1) {
      const fromDb = rosterHits.find((o) => o.gk === dbGk);
      target = fromDb?.gk ?? rosterHits[0].gk;
    } else if (dbGk && result.has(dbGk) && dbGk !== "_OTHER") {
      target = dbGk;
    }

    if (target && result.has(target)) result.get(target)!.push(m);
    else result.get("_OTHER")!.push(m);
  }

  const otherArr = result.get("_OTHER")!;
  if (otherArr.length === 0) result.delete("_OTHER");

  for (const rows of result.values()) rows.sort(byKick);

  return result;
}

/** Same shape as GET /api/group-rosters teams[]. */
export type RosterTeam = { team_code: string; team_name: string };

export type StandingRow = {
  rank: number;
  teamCode: string;
  teamName: string;
  played: number;
  wins: number;
  draws: number;
  losses: number;
  goalsFor: number;
  goalsAgainst: number;
  goalDiff: number;
  points: number;
};

function h2hOrder(a: string, b: string, matches: MatchOut[], draft: DraftScores): number {
  const m = matches.find(
    (x) =>
      (x.home_team_code === a && x.away_team_code === b) ||
      (x.home_team_code === b && x.away_team_code === a),
  );
  if (!m) return 0;
  const line = draftLine(draft, m.id);
  if (!line) return 0;
  const dh = line.home;
  const da = line.away;
  if (m.home_team_code === a) {
    if (dh > da) return -1;
    if (dh < da) return 1;
    return 0;
  }
  if (da > dh) return -1;
  if (da < dh) return 1;
  return 0;
}

/** Unique teams from fixtures in this bucket (client fallback when API roster incomplete). */
export function deriveRosterFromGroupMatches(matches: MatchOut[]): RosterTeam[] {
  const seen = new Map<string, string>();
  for (const m of matches) {
    seen.set(m.home_team_code, m.home_team_name);
    seen.set(m.away_team_code, m.away_team_name);
  }
  return [...seen.entries()]
    .map(([team_code, team_name]) => ({ team_code, team_name }))
    .sort((a, b) => a.team_name.localeCompare(b.team_name));
}

function filterIntraRosterMatches(matches: MatchOut[], rosterCodes: Set<string>): MatchOut[] {
  return matches.filter(
    (m) => rosterCodes.has(m.home_team_code) && rosterCodes.has(m.away_team_code),
  );
}

function sortStandingRows(rows: StandingRow[], h2hMatches: MatchOut[], draft: DraftScores): StandingRow[] {
  rows.sort((a, b) => {
    if (b.points !== a.points) return b.points - a.points;
    if (b.goalDiff !== a.goalDiff) return b.goalDiff - a.goalDiff;
    if (b.goalsFor !== a.goalsFor) return b.goalsFor - a.goalsFor;
    const h = h2hOrder(a.teamCode, b.teamCode, h2hMatches, draft);
    if (h !== 0) return h;
    return a.teamCode.localeCompare(b.teamCode);
  });
  rows.forEach((r, i) => {
    r.rank = i + 1;
  });
  return rows;
}

/**
 * FIFA-style table from predicted scorelines.
 * When `roster` lists the official teams for a World Cup group (typically four), only matches between
 * those teams affect points — so the table matches real 4-team groups once the schedule is synced.
 */
export function computeStandingsFromPredictions(
  groupMatches: MatchOut[],
  draft: DraftScores,
  roster?: RosterTeam[],
): StandingRow[] {
  type Acc = { name: string; p: number; w: number; d: number; l: number; gf: number; ga: number };
  const teams = new Map<string, Acc>();

  const bump = (code: string, name: string, gf: number, ga: number) => {
    let t = teams.get(code);
    if (!t) {
      t = { name, p: 0, w: 0, d: 0, l: 0, gf: 0, ga: 0 };
      teams.set(code, t);
    }
    t.p += 1;
    t.gf += gf;
    t.ga += ga;
    if (gf > ga) t.w += 1;
    else if (gf < ga) t.l += 1;
    else t.d += 1;
  };

  if (roster && roster.length > 0) {
    const codes = new Set(roster.map((r) => r.team_code));
    const intra = filterIntraRosterMatches(groupMatches, codes);
    for (const r of roster) {
      teams.set(r.team_code, { name: r.team_name, p: 0, w: 0, d: 0, l: 0, gf: 0, ga: 0 });
    }
    for (const m of intra) {
      const line = draftLine(draft, m.id);
      if (!line) continue;
      bump(m.home_team_code, m.home_team_name, line.home, line.away);
      bump(m.away_team_code, m.away_team_name, line.away, line.home);
    }
    const rows: StandingRow[] = roster.map((r) => {
      const t = teams.get(r.team_code)!;
      return {
        rank: 0,
        teamCode: r.team_code,
        teamName: t.name,
        played: t.p,
        wins: t.w,
        draws: t.d,
        losses: t.l,
        goalsFor: t.gf,
        goalsAgainst: t.ga,
        goalDiff: t.gf - t.ga,
        points: t.w * 3 + t.d,
      };
    });
    return sortStandingRows(rows, intra, draft);
  }

  for (const m of groupMatches) {
    const line = draftLine(draft, m.id);
    if (!line) continue;
    bump(m.home_team_code, m.home_team_name, line.home, line.away);
    bump(m.away_team_code, m.away_team_name, line.away, line.home);
  }

  const rows: StandingRow[] = [...teams.entries()].map(([teamCode, t]) => ({
    rank: 0,
    teamCode,
    teamName: t.name,
    played: t.p,
    wins: t.w,
    draws: t.d,
    losses: t.l,
    goalsFor: t.gf,
    goalsAgainst: t.ga,
    goalDiff: t.gf - t.ga,
    points: t.w * 3 + t.d,
  }));
  return sortStandingRows(rows, groupMatches, draft);
}

export function formatGroupLabel(groupKey: string): string {
  const u = groupKey.toUpperCase();
  if (u.startsWith("GROUP_") && u.length > 6) return `Group ${u.slice(6)}`;
  return groupKey.replace(/_/g, " ");
}

export function sortGroupKeys(keys: string[]): string[] {
  return [...keys].sort((a, b) => {
    if (a === "_OTHER") return 1;
    if (b === "_OTHER") return -1;
    const ma = a.match(/^GROUP_([A-Z]+)$/i);
    const mb = b.match(/^GROUP_([A-Z]+)$/i);
    if (ma && mb) return ma[1].localeCompare(mb[1]);
    return a.localeCompare(b);
  });
}
