import type { Dispatch, SetStateAction } from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { apiGet, apiPost, apiPut } from "./api";
import { DefaultProfileIcon } from "./DefaultProfileIcon";
import {
  buildGroupStageMatchBuckets,
  computeStandingsFromPredictions,
  deriveRosterFromGroupMatches,
  formatGroupLabel,
  sortGroupKeys,
  type RosterTeam,
  type StandingRow,
} from "./groupStandings";
import {
  computeFullKnockoutProjections,
  hasAnyGroupPredictions,
  stageDisplayName,
  type KnockoutProjection,
} from "./knockoutBracket";
import { canonicalTeamTla, teamTlaEquals, winnerCodeForTeamSelect } from "./teamTla";
import { teamFlag } from "./teamFlags";
import type {
  GroupRostersResponse,
  MatchOut,
  MeOut,
  PoolDashboardOut,
  PoolSummaryOut,
  PublicParticipantProfileOut,
  PutMatchPredictionsOut,
  TeamOptionOut,
  TopScorerPickOut,
  TournamentPredictionsOut,
  UserProfileOut,
  WorldcupPlayerOut,
} from "./types";

type DraftCell = { home: number | null; away: number | null; advance?: string | null };
type DraftPred = Record<string, DraftCell>;

type TabId = "matches" | "tournament" | "leaderboard" | "profile";

function formatTeamPick(code: string | null, teamList: TeamOptionOut[]) {
  if (!code?.trim()) return "—";
  const t = teamList.find((x) => x.code === code);
  return t ? `${t.name} (${t.code})` : code;
}

const MAX_TOP_SCORERS = 3;

function sleep(ms: number) {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

/* ---- Toast notification system ---- */
type ToastType = "ok" | "err";
type ToastItem = { id: number; type: ToastType; text: string };
let toastIdSeq = 0;

function useToast() {
  const [toasts, setToasts] = useState<ToastItem[]>([]);
  const push = useCallback((type: ToastType, text: string) => {
    const id = ++toastIdSeq;
    setToasts((prev) => [...prev, { id, type, text }]);
    setTimeout(() => setToasts((prev) => prev.filter((t) => t.id !== id)), 4500);
  }, []);
  const dismiss = useCallback((id: number) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  }, []);
  return { toasts, push, dismiss };
}

function ToastContainer({ toasts, onDismiss }: { toasts: ToastItem[]; onDismiss: (id: number) => void }) {
  if (!toasts.length) return null;
  return (
    <div className="toast-container" aria-live="polite">
      {toasts.map((t) => (
        <div key={t.id} className={`toast toast-${t.type}`}>
          <span>{t.text}</span>
          <button type="button" className="toast-close" onClick={() => onDismiss(t.id)} aria-label="Dismiss">&times;</button>
        </div>
      ))}
    </div>
  );
}

/* ---- Countdown hook ---- */
function useCountdown(targetIso: string | null) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    if (!targetIso) return;
    const iv = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(iv);
  }, [targetIso]);
  if (!targetIso) return null;
  const diff = new Date(targetIso).getTime() - now;
  if (diff <= 0) return null;
  const d = Math.floor(diff / 86400000);
  const h = Math.floor((diff % 86400000) / 3600000);
  const m = Math.floor((diff % 3600000) / 60000);
  const s = Math.floor((diff % 60000) / 1000);
  if (d > 0) return `${d}d ${h}h ${m}m`;
  if (h > 0) return `${h}h ${m}m ${s}s`;
  return `${m}m ${s}s`;
}

/* ---- Medal icons for top 3 ---- */
function MedalIcon({ rank }: { rank: number }) {
  if (rank === 1) return <span className="medal gold" title="1st place">🥇</span>;
  if (rank === 2) return <span className="medal silver" title="2nd place">🥈</span>;
  if (rank === 3) return <span className="medal bronze" title="3rd place">🥉</span>;
  return <span className="mono">{rank}</span>;
}

/* ---- Dark mode ---- */
function useDarkMode() {
  const [dark, setDark] = useState(() => {
    try { return localStorage.getItem("wc-dark-mode") === "true"; } catch { return false; }
  });
  useEffect(() => {
    document.documentElement.setAttribute("data-theme", dark ? "dark" : "light");
    try { localStorage.setItem("wc-dark-mode", String(dark)); } catch { /* noop */ }
  }, [dark]);
  return [dark, () => setDark((d) => !d)] as const;
}

/* ---- Flag-enhanced team display ---- */
function TeamName({ code, name, className }: { code: string; name: string; className?: string }) {
  const flag = teamFlag(code);
  return (
    <>
      {flag && <span className="team-flag" aria-hidden="true">{flag}</span>}
      <span className={className}>{name}</span>
    </>
  );
}

function fmtTournamentLockUtc(iso: string | undefined | null): string {
  const s = (iso || "").trim();
  if (!s) return "2026-06-11 18:00 UTC";
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return s;
  const y = d.getUTCFullYear();
  const mo = String(d.getUTCMonth() + 1).padStart(2, "0");
  const da = String(d.getUTCDate()).padStart(2, "0");
  const h = String(d.getUTCHours()).padStart(2, "0");
  const mi = String(d.getUTCMinutes()).padStart(2, "0");
  return `${y}-${mo}-${da} ${h}:${mi} UTC`;
}

/** DB / schema may lag a few seconds behind app HTTP startup (background init). */
function isTransientStartupLoadError(e: unknown): boolean {
  const msg = e instanceof Error ? e.message : String(e);
  if (/^401 /.test(msg) || /^403 /.test(msg)) return false;
  if (/^5\d\d /.test(msg)) return true;
  if (/does not exist/i.test(msg)) return true;
  return false;
}

function scorerKey(p: { player_name: string; country_code: string }) {
  return `${p.player_name}\t${p.country_code}`;
}

/** Empty field → null; clamp 0–30. */
function parseGoalInput(raw: string): number | null {
  const t = raw.trim();
  if (t === "") return null;
  const n = parseInt(t, 10);
  if (Number.isNaN(n)) return null;
  return Math.min(30, Math.max(0, n));
}

function LockIcon() {
  return (
    <svg
      className="lock-icon"
      viewBox="0 0 20 20"
      fill="currentColor"
      aria-hidden="true"
      focusable="false"
    >
      <path
        fillRule="evenodd"
        d="M5 9V7a5 5 0 0110 0v2h2a2 2 0 012 2v6a2 2 0 01-2 2H3a2 2 0 01-2-2v-6a2 2 0 012-2h2zm8-2v2H7V7a3 3 0 016 0z"
        clipRule="evenodd"
      />
    </svg>
  );
}

function matchAwardedTotal(m: MatchOut): number {
  return (
    (m.points_outcome ?? 0) +
    (m.points_exact ?? 0) +
    (m.points_scorer_goals ?? 0) +
    (m.points_advancer ?? 0)
  );
}

function matchPointsTooltip(m: MatchOut): string {
  const po = m.points_outcome ?? 0;
  const pe = m.points_exact ?? 0;
  const ps = m.points_scorer_goals ?? 0;
  const pa = m.points_advancer ?? 0;
  const bits: string[] = [];
  if (po) bits.push(`Outcome ${po}`);
  if (pe) bits.push(`Exact ${pe}`);
  if (ps) bits.push(`Top scorers ${ps}`);
  if (pa) bits.push(`Advancer ${pa}`);
  if (!bits.length) {
    return "No points this match. Multipliers: group ×1, R32 ×1.5, R16 ×2, QF ×2.5, SF/Final ×3.";
  }
  return `${bits.join(" · ")}. Multipliers: group ×1, R32 ×1.5, R16 ×2, QF ×2.5, SF/Final ×3.`;
}

function isKnockoutStage(stage: string | null | undefined): boolean {
  const s = (stage || "").trim().toUpperCase();
  return !!s && s !== "GROUP_STAGE";
}

function predictedWinnerSide(
  m: MatchOut,
  dh: number | null,
  da: number | null,
  advanceCode: string | null | undefined,
): "home" | "away" | null {
  if (dh == null || da == null) return null;
  if (dh > da) return "home";
  if (da > dh) return "away";
  if (!isKnockoutStage(m.stage)) return null;
  if (!canonicalTeamTla(advanceCode)) return null;
  if (teamTlaEquals(advanceCode, m.home_team_code)) return "home";
  if (teamTlaEquals(advanceCode, m.away_team_code)) return "away";
  return null;
}

function actualWinnerSide(m: MatchOut): "home" | "away" | null {
  if (m.status !== "FINISHED" || m.home_score == null || m.away_score == null) return null;
  const w = (m.winner_team_code || "").trim().toUpperCase();
  if (w) {
    if (w === m.home_team_code.toUpperCase()) return "home";
    if (w === m.away_team_code.toUpperCase()) return "away";
  }
  const h = m.home_score;
  const a = m.away_score;
  if (h > a) return "home";
  if (a > h) return "away";
  return null;
}

function formatResultParts(m: MatchOut): {
  home: number | null;
  away: number | null;
  suffix: string | null;
  title?: string;
} {
  if (m.home_score == null || m.away_score == null) return { home: null, away: null, suffix: null };
  const title = isKnockoutStage(m.stage)
    ? "Knockout: synced scoreline is after extra time (120 minutes) when extra time was played; if level after ET, the listed winner is the penalty shoot-out winner."
    : undefined;
  return { home: m.home_score, away: m.away_score, suffix: null, title };
}

function MatchTable({
  rows,
  draft,
  setDraft,
  projections,
  saved,
  recentlyFinished,
}: {
  rows: MatchOut[];
  draft: DraftPred;
  setDraft: Dispatch<SetStateAction<DraftPred>>;
  projections?: Map<string, KnockoutProjection>;
  saved?: DraftPred;
  recentlyFinished?: Set<string>;
}) {
  return (
    <table className="grid">
      <thead>
        <tr>
          <th>Kickoff (local)</th>
          <th>Home</th>
          <th className="center">Pred</th>
          <th>Away</th>
          <th>Result</th>
          <th className="narrow right">Pts</th>
          <th>Status</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((x) => {
          const open = x.prediction_open;
          const cell = draft[x.id] ?? { home: null, away: null, advance: null };
          const dh = cell.home ?? null;
          const da = cell.away ?? null;
          const adv = cell.advance ?? null;

          // Projected teams for TBD knockout matches
          const isTBD = x.home_team_code === "?" || x.home_team_name === "TBD";
          const proj = projections?.get(x.id);
          const homeName = isTBD && proj?.projectedHome ? proj.projectedHome.teamName : x.home_team_name;
          const homeCode = isTBD && proj?.projectedHome ? proj.projectedHome.teamCode : x.home_team_code;
          const awayName = isTBD && proj?.projectedAway ? proj.projectedAway.teamName : x.away_team_name;
          const awayCode = isTBD && proj?.projectedAway ? proj.projectedAway.teamCode : x.away_team_code;
          // Per-team projection flags: only amber when projected AND not confirmed
          const homeProjected = isTBD && !!proj?.projectedHome && !proj.homeConfirmed;
          const awayProjected = isTBD && !!proj?.projectedAway && !proj.awayConfirmed;
          const projected = homeProjected || awayProjected;

          const predSide = predictedWinnerSide(
            { ...x, home_team_code: homeCode, away_team_code: awayCode },
            dh, da, adv,
          );
          const actSide = actualWinnerSide(x);
          const finished =
            x.status === "FINISHED" && x.home_score != null && x.away_score != null;
          const savedLine = x.pred_home_goals != null && x.pred_away_goals != null;
          const ptsTotal = matchAwardedTotal(x);
          const resParts = formatResultParts(x);
          const sv = saved?.[x.id];
          const homeChanged = open && sv !== undefined && dh !== (sv.home ?? null);
          const awayChanged = open && sv !== undefined && da !== (sv.away ?? null);
          const justFinished = recentlyFinished?.has(x.id);
          return (
            <tr key={x.id} className={`${open ? "" : "locked"}${projected ? " ko-projected" : ""}${justFinished ? " score-reveal" : ""}`}>
              <td className="mono small">{fmtLocal(x.kickoff_utc)}</td>
              <td>
                <TeamName code={homeCode} name={homeName} className={`team${predSide === "home" ? " pred-winner" : ""}${homeProjected ? " projected-team" : ""}`} />
                {isTBD && proj?.homeLabel && (
                  <span className="bracket-pos-label">{proj.homeLabel}</span>
                )}
              </td>
              <td className="center inputs">
                <div className="pred-score-inputs">
                  <input
                    type="number"
                    min={0}
                    max={30}
                    className={homeChanged ? "unsaved" : undefined}
                    disabled={!open}
                    value={dh === null ? "" : dh}
                    onChange={(e) => {
                      if (!open) return;
                      setDraft((d) => {
                        const cur = d[x.id] ?? { home: null, away: null, advance: null };
                        const nh = parseGoalInput(e.target.value);
                        const na = cur.away;
                        const nextAdv =
                          nh !== null && na !== null && nh === na ? cur.advance ?? null : null;
                        return { ...d, [x.id]: { home: nh, away: na, advance: nextAdv } };
                      });
                    }}
                  />
                  <span className="dash">—</span>
                  <input
                    type="number"
                    min={0}
                    max={30}
                    className={awayChanged ? "unsaved" : undefined}
                    disabled={!open}
                    value={da === null ? "" : da}
                    onChange={(e) => {
                      if (!open) return;
                      setDraft((d) => {
                        const cur = d[x.id] ?? { home: null, away: null, advance: null };
                        const na = parseGoalInput(e.target.value);
                        const nh = cur.home;
                        const nextAdv =
                          nh !== null && na !== null && nh === na ? cur.advance ?? null : null;
                        return { ...d, [x.id]: { home: nh, away: na, advance: nextAdv } };
                      });
                    }}
                  />
                </div>
                {isKnockoutStage(x.stage) && dh !== null && da !== null && dh === da
                  && homeCode !== "?" && awayCode !== "?" ? (
                  <div className="ko-advance-picks" role="group" aria-label="Penalty advancing team">
                    <span className="ko-advance-label">Advances (penalties)</span>
                    <label className="ko-advance-check">
                      <input
                        type="checkbox"
                        disabled={!open}
                        checked={adv !== null && teamTlaEquals(adv, homeCode)}
                        onChange={(e) => {
                          if (!open) return;
                          setDraft((d) => {
                            const cur = d[x.id] ?? { home: dh, away: da, advance: null };
                            const next = e.target.checked ? homeCode : null;
                            return { ...d, [x.id]: { ...cur, advance: next } };
                          });
                        }}
                      />
                      <span className={`team${teamTlaEquals(adv, homeCode) ? " pred-winner" : ""}${homeProjected ? " projected-team" : ""}`}>
                        {homeName}
                      </span>
                      <span className="code">({homeCode})</span>
                    </label>
                    <label className="ko-advance-check">
                      <input
                        type="checkbox"
                        disabled={!open}
                        checked={adv !== null && teamTlaEquals(adv, awayCode)}
                        onChange={(e) => {
                          if (!open) return;
                          setDraft((d) => {
                            const cur = d[x.id] ?? { home: dh, away: da, advance: null };
                            const next = e.target.checked ? awayCode : null;
                            return { ...d, [x.id]: { ...cur, advance: next } };
                          });
                        }}
                      />
                      <span className={`team${teamTlaEquals(adv, awayCode) ? " pred-winner" : ""}${awayProjected ? " projected-team" : ""}`}>
                        {awayName}
                      </span>
                      <span className="code">({awayCode})</span>
                    </label>
                  </div>
                ) : null}
              </td>
              <td>
                <TeamName code={awayCode} name={awayName} className={`team${predSide === "away" ? " pred-winner" : ""}${awayProjected ? " projected-team" : ""}`} />
                {isTBD && proj?.awayLabel && (
                  <span className="bracket-pos-label">{proj.awayLabel}</span>
                )}
              </td>
              <td className="mono small" title={resParts.title}>
                {resParts.home == null || resParts.away == null ? (
                  "—"
                ) : (
                  <>
                    <span className={actSide === "home" ? "actual-winner" : undefined}>{resParts.home}</span>
                    {" — "}
                    <span className={actSide === "away" ? "actual-winner" : undefined}>{resParts.away}</span>
                    {resParts.suffix ? <> {resParts.suffix}</> : null}
                  </>
                )}
              </td>
              <td className="narrow right match-pts-cell">
                {finished && savedLine ? (
                  <span
                    className={`match-pts${ptsTotal === 0 ? " zero" : ""}`}
                    title={`${matchPointsTooltip(x)} Points reflect your saved prediction on the server (not unsaved draft edits).${resParts.title ? ` ${resParts.title}` : ""}`}
                  >
                    +{ptsTotal} pts
                  </span>
                ) : (
                  <span className="muted small" title={finished ? "No saved prediction for this match" : undefined}>
                    —
                  </span>
                )}
              </td>
              <td>
                <span className="match-status-cell">
                  {open ? (
                    <span className="lock-icon-slot" aria-hidden="true" />
                  ) : (
                    <span
                      className="lock-icon-wrap"
                      role="img"
                      aria-label="Predictions locked for this match"
                      title="Predictions locked — deadline passed for this match"
                    >
                      <LockIcon />
                    </span>
                  )}
                  <span className={`pill ${open ? "open" : x.status === "LIVE" || x.status === "IN_PLAY" ? "live" : "closed"}`}>
                    {open ? "Open" : x.status === "LIVE" || x.status === "IN_PLAY" ? "Live" : x.status}
                  </span>
                </span>
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function fmtLocal(iso: string) {
  const d = new Date(iso);
  return d.toLocaleString(undefined, {
    weekday: "short",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function fmtUpdated(iso: string | null) {
  if (!iso) return "Never";
  return new Date(iso).toLocaleString();
}

export default function App() {
  /** React 18 Strict Mode runs effects twice; overlapping loads must not let an abandoned run clear `loading`. */
  const loadSeq = useRef(0);

  const [darkMode, toggleDarkMode] = useDarkMode();
  const toast = useToast();

  const [tab, setTab] = useState<TabId>("matches");
  const [me, setMe] = useState<MeOut | null>(null);
  const [summary, setSummary] = useState<PoolSummaryOut | null>(null);
  const [matches, setMatches] = useState<MatchOut[]>([]);
  const [teams, setTeams] = useState<TeamOptionOut[]>([]);
  const [tournament, setTournament] = useState<TournamentPredictionsOut | null>(null);
  const [profile, setProfile] = useState<UserProfileOut | null>(null);
  const [groupRosters, setGroupRosters] = useState<GroupRostersResponse>({ groups: [] });
  const [dashboard, setDashboard] = useState<PoolDashboardOut | null>(null);
  const [dashboardErr, setDashboardErr] = useState<string | null>(null);
  const [profileModalUserId, setProfileModalUserId] = useState<string | null>(null);
  const [profileModal, setProfileModal] = useState<PublicParticipantProfileOut | null>(null);
  const [profileModalErr, setProfileModalErr] = useState<string | null>(null);
  const [profileModalLoading, setProfileModalLoading] = useState(false);

  const [draft, setDraft] = useState<DraftPred>({});
  const [savedDraft, setSavedDraft] = useState<DraftPred>({});
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set());
  const [recentlyFinished, setRecentlyFinished] = useState<Set<string>>(new Set());
  const prevMatchStatuses = useRef<Map<string, string>>(new Map());

  const [winner, setWinner] = useState("");
  const [topScorerPicks, setTopScorerPicks] = useState<TopScorerPickOut[]>([]);

  const [playerCatalog, setPlayerCatalog] = useState<WorldcupPlayerOut[]>([]);
  const [playerSearch, setPlayerSearch] = useState("");
  const [playerCatalogLoading, setPlayerCatalogLoading] = useState(false);

  const [profileName, setProfileName] = useState("");
  const [profileNationality, setProfileNationality] = useState("");
  const [profilePicture, setProfilePicture] = useState("");
  const [mustCompleteProfile, setMustCompleteProfile] = useState(false);

  const [loading, setLoading] = useState(true);
  // Convenience wrappers — these used to set banner state, now they push toasts
  const setMsg = useCallback((t: string | null) => { if (t) toast.push("ok", t); }, [toast]);
  const setErr = useCallback((t: string | null) => { if (t) toast.push("err", t); }, [toast]);
  const load = useCallback(async (opts?: { showSpinner?: boolean }) => {
    const showSpinner = opts?.showSpinner !== false;
    const seq = ++loadSeq.current;
    setErr(null);
    setDashboardErr(null);
    if (showSpinner) {
      setLoading(true);
    }
    const maxAttempts = 6;
    try {
      for (let attempt = 0; attempt < maxAttempts; attempt++) {
        try {
          const [m, s, ma, tm, tr, pr, gr] = await Promise.all([
            apiGet<MeOut>("/api/me"),
            apiGet<PoolSummaryOut>("/api/pool-summary"),
            apiGet<MatchOut[]>("/api/matches"),
            apiGet<TeamOptionOut[]>("/api/teams").catch(() => [] as TeamOptionOut[]),
            apiGet<TournamentPredictionsOut>("/api/predictions/tournament"),
            apiGet<UserProfileOut>("/api/profile"),
            apiGet<GroupRostersResponse>("/api/group-rosters").catch(() => ({ groups: [] })),
          ]);
          if (seq !== loadSeq.current) {
            return;
          }
          setMe(m);
          setSummary(s);
          setMatches(Array.isArray(ma) ? ma : []);
          setTeams(tm);
          setTournament(tr);
          setProfile(pr);
          setGroupRosters(gr);

          // Sync draft / tournament / profile form fields with this response **before** awaiting
          // `/api/dashboard`. A concurrent `load()` can bump `loadSeq` during that await; if we then
          // abort at the seq check, we must not leave `tournament` updated while `winner`/`topScorerPicks`
          // still hold stale values (empty champion + chips after "Tournament picks saved.").
          const d: DraftPred = {};
          for (const x of Array.isArray(ma) ? ma : []) {
            const rawAdv = x.pred_advance_team_code;
            const advTrim =
              rawAdv == null || typeof rawAdv !== "string" ? "" : rawAdv.trim();
            d[x.id] = {
              home: x.pred_home_goals ?? null,
              away: x.pred_away_goals ?? null,
              advance: advTrim ? advTrim : null,
            };
          }
          setDraft(d);
          setSavedDraft({ ...d });

          // Detect newly finished matches for score-reveal animation
          const newFinished = new Set<string>();
          for (const mx of Array.isArray(ma) ? ma : []) {
            const prev = prevMatchStatuses.current.get(mx.id);
            if (mx.status === "FINISHED" && prev && prev !== "FINISHED") {
              newFinished.add(mx.id);
            }
            prevMatchStatuses.current.set(mx.id, mx.status);
          }
          if (newFinished.size > 0) {
            setRecentlyFinished(newFinished);
            setTimeout(() => setRecentlyFinished(new Set()), 3000);
          }

          // Auto-collapse groups where all matches are finished
          const autoCollapsed = new Set<string>();
          for (const [gk, gRows] of buildGroupStageMatchBuckets(Array.isArray(ma) ? ma : [], gr.groups) ?? []) {
            if (gRows.every((r) => r.status === "FINISHED")) autoCollapsed.add(gk);
          }
          setCollapsedGroups((prev) => {
            const next = new Set(prev);
            for (const gk of autoCollapsed) next.add(gk);
            return next;
          });

          setWinner(winnerCodeForTeamSelect(tr.tournament_winner_team_code, tm));
          setTopScorerPicks(
            (Array.isArray(tr.top_scorers) ? tr.top_scorers : []).slice(0, MAX_TOP_SCORERS),
          );

          setProfileName(pr.display_name ?? "");
          setProfileNationality(pr.nationality ?? "");
          setProfilePicture(pr.profile_picture ?? "");
          const firstTimeUser = !pr.updated_at;
          setMustCompleteProfile(firstTimeUser);
          if (firstTimeUser) {
            setTab("profile");
            setMsg(
              "Welcome! Please complete your profile first so everyone can recognize you in the pool.",
            );
          }

          try {
            const dash = await apiGet<PoolDashboardOut>("/api/dashboard");
            if (seq !== loadSeq.current) {
              return;
            }
            setDashboard(dash);
          } catch (e) {
            if (seq !== loadSeq.current) {
              return;
            }
            setDashboard({ predictors_count: 0, leaderboard: { entries: [] } });
            setDashboardErr(e instanceof Error ? e.message : String(e));
          }

          return;
        } catch (e) {
          if (seq !== loadSeq.current) {
            return;
          }
          if (attempt < maxAttempts - 1 && isTransientStartupLoadError(e)) {
            await sleep(900 * (attempt + 1));
            continue;
          }
          setErr(e instanceof Error ? e.message : String(e));
          return;
        }
      }
    } finally {
      if (seq === loadSeq.current) {
        setLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    if (profileModalUserId === null) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") closeParticipantProfileModal();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [profileModalUserId]);

  function closeParticipantProfileModal() {
    setProfileModalUserId(null);
    setProfileModal(null);
    setProfileModalErr(null);
    setProfileModalLoading(false);
  }

  async function openParticipantProfile(userId: string) {
    setProfileModalUserId(userId);
    setProfileModal(null);
    setProfileModalErr(null);
    setProfileModalLoading(true);
    try {
      const p = await apiGet<PublicParticipantProfileOut>(
        `/api/profiles/${encodeURIComponent(userId)}`,
      );
      setProfileModal(p);
    } catch (e) {
      setProfileModalErr(e instanceof Error ? e.message : String(e));
    } finally {
      setProfileModalLoading(false);
    }
  }

  useEffect(() => {
    if (tab !== "tournament" || loading) return;
    let cancelled = false;
    setPlayerCatalogLoading(true);
    void (async () => {
      try {
        const players = await apiGet<WorldcupPlayerOut[]>("/api/worldcup-players");
        if (!cancelled) setPlayerCatalog(players);
      } catch {
        if (!cancelled) setPlayerCatalog([]);
      } finally {
        if (!cancelled) setPlayerCatalogLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [tab, loading]);

  const filteredWorldcupPlayers = useMemo(() => {
    const picked = new Set(topScorerPicks.map(scorerKey));
    const q = playerSearch.trim().toLowerCase();
    let list = playerCatalog.filter((p) => !picked.has(scorerKey(p)));
    if (q) {
      list = list.filter(
        (p) =>
          p.player_name.toLowerCase().includes(q) ||
          p.country_name.toLowerCase().includes(q) ||
          p.country_code.toLowerCase().includes(q),
      );
    }
    return list.slice(0, 150);
  }, [playerCatalog, playerSearch, topScorerPicks]);

  const { groupStageByGroup, otherByStage } = useMemo(() => {
    const other = new Map<string, MatchOut[]>();
    const kick = (a: MatchOut, b: MatchOut) =>
      new Date(a.kickoff_utc).getTime() - new Date(b.kickoff_utc).getTime();

    for (const x of matches) {
      const st = (x.stage || "").toUpperCase();
      if (st !== "GROUP_STAGE") {
        const label = x.stage || "Matches";
        if (!other.has(label)) other.set(label, []);
        other.get(label)!.push(x);
      }
    }
    for (const rows of other.values()) rows.sort(kick);

    const fromRosters = buildGroupStageMatchBuckets(matches, groupRosters.groups);
    if (fromRosters) {
      return { groupStageByGroup: fromRosters, otherByStage: other };
    }

    const groups = new Map<string, MatchOut[]>();
    for (const x of matches) {
      const st = (x.stage || "").toUpperCase();
      if (st === "GROUP_STAGE") {
        const gk = (x.group_key && x.group_key.trim()) || "_OTHER";
        if (!groups.has(gk)) groups.set(gk, []);
        groups.get(gk)!.push(x);
      }
    }
    for (const rows of groups.values()) rows.sort(kick);
    return { groupStageByGroup: groups, otherByStage: other };
  }, [matches, groupRosters.groups]);

  /** Standings for every group, computed from user's draft predictions. */
  const allGroupStandings = useMemo(() => {
    const standings = new Map<string, StandingRow[]>();
    for (const [gk, rows] of groupStageByGroup) {
      if (gk === "_OTHER") continue;
      const apiEntry = groupRosters.groups.find((g) => g.group_key === gk);
      const derived = deriveRosterFromGroupMatches(rows);
      let roster: RosterTeam[] | undefined;
      if (apiEntry && apiEntry.teams.length >= 2 && apiEntry.teams.length <= 4) {
        roster = apiEntry.teams;
      } else if (derived.length === 4) {
        roster = derived;
      }
      standings.set(gk, computeStandingsFromPredictions(rows, draft, roster));
    }
    return standings;
  }, [groupStageByGroup, draft, groupRosters.groups]);

  /** Full knockout projections (R32 → R16 → QF → SF → Final), cascading from group predictions + draft scores. */
  const knockoutProjections = useMemo(
    () => computeFullKnockoutProjections(allGroupStandings, matches, draft),
    [allGroupStandings, matches, draft],
  );

  const hasProjections = hasAnyGroupPredictions(allGroupStandings);

  // Countdown: find the next upcoming match kickoff
  const nextMatchKickoff = useMemo(() => {
    const now = Date.now();
    let earliest: string | null = null;
    let earliestMs = Infinity;
    for (const m of matches) {
      const ms = new Date(m.kickoff_utc).getTime();
      if (ms > now && ms < earliestMs) {
        earliestMs = ms;
        earliest = m.kickoff_utc;
      }
    }
    return earliest;
  }, [matches]);
  const countdown = useCountdown(nextMatchKickoff);

  // Progress: how many matches have predictions filled
  const predProgress = useMemo(() => {
    const open = matches.filter((m) => m.prediction_open);
    const filled = matches.filter((m) => {
      const c = draft[m.id];
      return c && c.home !== null && c.away !== null;
    });
    return { filled: filled.length, total: matches.length, openRemaining: open.filter((m) => { const c = draft[m.id]; return !c || c.home === null || c.away === null; }).length };
  }, [matches, draft]);

  async function saveMatches() {
    setMsg(null);
    setErr(null);
    if (mustCompleteProfile) {
      setErr("Please complete and save your profile first before entering predictions.");
      setTab("profile");
      return;
    }
    try {
      const openMatches = matches.filter((x) => x.prediction_open);
      for (const x of openMatches) {
        const cell = draft[x.id] ?? { home: null, away: null, advance: null };
        const h = cell.home;
        const a = cell.away;
        if ((h == null) !== (a == null)) {
          setErr("Enter both home and away scores for each open match, or leave both empty.");
          return;
        }
        const advTrim = (cell.advance || "").trim();
        // For TBD matches, advance code is a projected team code — check against projected teams
        const isTBD = x.home_team_code === "?" || x.home_team_name === "TBD";
        const proj = knockoutProjections.get(x.id);
        const effHomeCode = isTBD && proj?.projectedHome ? proj.projectedHome.teamCode : x.home_team_code;
        const effAwayCode = isTBD && proj?.projectedAway ? proj.projectedAway.teamCode : x.away_team_code;
        const advInvalid =
          !advTrim ||
          (!teamTlaEquals(advTrim, effHomeCode) && !teamTlaEquals(advTrim, effAwayCode));
        if (h != null && a != null && h === a && isKnockoutStage(x.stage) && effHomeCode !== "?" && advInvalid) {
          setErr(
            "Knockout: for a predicted draw after extra time, tick exactly one team as advancing on penalties before saving.",
          );
          return;
        }
      }
      if (openMatches.length === 0) {
        setMsg(matches.length ? "No open matches — all are locked." : "No matches to save.");
        return;
      }
      const predictions = openMatches.map((x) => {
        const cell = draft[x.id] ?? { home: null, away: null, advance: null };
        const h = cell.home ?? null;
        const a = cell.away ?? null;
        const koDraw = h != null && a != null && h === a && isKnockoutStage(x.stage);
        return {
          match_id: x.id,
          home_goals: h,
          away_goals: a,
          advance_team_code: koDraw ? (cell.advance || "").trim() || null : null,
        };
      });
      const res = await apiPut<PutMatchPredictionsOut>("/api/predictions/matches", {
        predictions,
      });
      const lockedCount = matches.length - openMatches.length;
      setMsg(
        lockedCount > 0
          ? `Saved ${res.updated} prediction(s). ${lockedCount} locked match${lockedCount === 1 ? "" : "es"} skipped (unchanged).`
          : `Saved ${res.updated} prediction(s).`,
      );
      if (res.errors?.length) {
        setErr(res.errors.map((e) => `${e.match_id}: ${e.detail}`).join("\n"));
      }
      await load({ showSpinner: false });
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    }
  }

  async function saveTournament() {
    setMsg(null);
    setErr(null);
    if (mustCompleteProfile) {
      setErr("Please complete and save your profile first before entering predictions.");
      setTab("profile");
      return;
    }
    try {
      const prevNotes = tournament?.notes_json;
      const notes: Record<string, unknown> =
        prevNotes && typeof prevNotes === "object" && !Array.isArray(prevNotes)
          ? { ...prevNotes }
          : {};
      delete notes.top_scorers;
      const res = await apiPut<TournamentPredictionsOut>("/api/predictions/tournament", {
        tournament_winner_team_code: winner.trim() ? winner.trim() : null,
        top_scorer_player_name: null,
        top_scorers: topScorerPicks.map((p) => ({
          player_name: p.player_name,
          country_code: p.country_code,
        })),
        notes_json: notes,
      });
      setTournament(res);
      setWinner(winnerCodeForTeamSelect(res.tournament_winner_team_code, teams));
      setTopScorerPicks((Array.isArray(res.top_scorers) ? res.top_scorers : []).slice(0, MAX_TOP_SCORERS));
      setMsg("Tournament picks saved.");
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    }
  }

  async function saveProfile() {
    setMsg(null);
    setErr(null);
    try {
      const sanitizedPicture =
        profilePicture && profilePicture.startsWith("data:image/") ? profilePicture : null;
      await apiPut<UserProfileOut>("/api/profile", {
        display_name: profileName || null,
        nationality: profileNationality || null,
        profile_picture: sanitizedPicture,
      });
      setMsg("Profile updated successfully.");
      await load({ showSpinner: false });
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    }
  }

  async function seedDemo() {
    setErr(null);
    try {
      await apiPost("/api/dev/seed-demo-matches");
      setMsg("Demo matches seeded.");
      await load({ showSpinner: false });
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    }
  }

  function onPictureFileSelected(file: File | null) {
    if (!file) return;
    if (!file.type.startsWith("image/")) {
      setErr("Please choose an image file for profile picture.");
      return;
    }
    const reader = new FileReader();
    reader.onload = () => {
      const result = String(reader.result || "");
      setProfilePicture(result);
    };
    reader.onerror = () => setErr("Could not read image file.");
    reader.readAsDataURL(file);
  }

  return (
    <div className="app-shell">
      <header className="hero">
        <div className="hero-inner">
          <div className="powered-by-bar">
            <span className="powered-by-label">Powered by</span>
            <span className="powered-by-product">
              <img className="powered-by-icon" src="/logos/lakebase.svg" alt="" />
              Lakebase
            </span>
            <span className="powered-by-sep">&</span>
            <span className="powered-by-product">
              <img className="powered-by-icon" src="/logos/databricks-apps.svg" alt="" />
              Databricks Apps
            </span>
            <img className="powered-by-dbx-logo" src="/logos/databricks-logo.png" alt="Databricks" />
          </div>
          <div className="hero-top">
            <div>
              <div className="host-flags" aria-label="Host countries">
                <span className="host-flag-chip" title="United States">
                  🇺🇸 USA
                </span>
                <span className="host-flag-chip" title="Canada">
                  🇨🇦 Canada
                </span>
                <span className="host-flag-chip" title="Mexico">
                  🇲🇽 Mexico
                </span>
              </div>
              <p className="eyebrow">World Cup 2026</p>
              <h1>Prediction pool</h1>
              <p className="lede">
                Predict the scores for every World Cup match and compete against your colleagues!
                Each match locks <strong>1 hour before kickoff</strong>. Pick your tournament winner and top scorers
                before the opening match on the <strong>Tournament</strong> tab. Check the{" "}
                <strong>Leaderboard</strong> to see where you stand.
              </p>
              {countdown && (
                <div className="countdown-row">
                  <span className="countdown-label">Next match in</span>
                  <span className="countdown-value">{countdown}</span>
                </div>
              )}
            </div>
            <div className="hero-brand-logos" aria-label="World Cup 2026 branding">
              <img
                className="hero-logo wc-logo"
                src="/logos/wc2026.svg"
                alt="FIFA World Cup 2026 logo"
              />
            </div>
          </div>
          {me && (
            <div className="meta-row">
              {profile?.profile_picture ? (
                <img src={profile.profile_picture} alt="Profile" className="profile-avatar-small" />
              ) : (
                <div className="profile-avatar-small avatar-placeholder" aria-hidden>
                  <DefaultProfileIcon />
                </div>
              )}
              <p className="meta">
                Signed in as <span className="mono">{profile?.display_name || me.email || me.user_id}</span>
              </p>
            </div>
          )}
        </div>
      </header>

      {import.meta.env.DEV && !import.meta.env.VITE_DEV_ACCESS_TOKEN && (
        <div className="banner warn">
          Local dev: set <code>VITE_DEV_ACCESS_TOKEN</code> in <code>ui/.env</code> (JWT with{" "}
          <code>email</code> claim). Vite proxy injects it into <code>/api</code> requests.
        </div>
      )}

      {summary && (
        <section className="stats">
          <div className="stat-card">
            <span className="stat-label">Matches predicted</span>
            <span className="stat-value">
              {summary.predicted_matches}/{summary.total_matches}
            </span>
            {summary.total_matches > 0 && (
              <div className="progress-bar-wrap" title={`${predProgress.filled} of ${predProgress.total} filled`}>
                <div className="progress-bar" style={{ width: `${Math.round((predProgress.filled / predProgress.total) * 100)}%` }} />
              </div>
            )}
          </div>
          <div className="stat-card">
            <span className="stat-label">Next lock</span>
            <span className="stat-value small">{summary.next_deadline_label || "—"}</span>
          </div>
          {predProgress.openRemaining > 0 && (
            <div className="stat-card">
              <span className="stat-label">Still open</span>
              <span className="stat-value">{predProgress.openRemaining}</span>
              <span className="muted small">Matches need your prediction</span>
            </div>
          )}
        </section>
      )}

      <nav className="tabs sticky-tabs">
        <button
          type="button"
          className={tab === "matches" ? "active" : ""}
          onClick={() => setTab("matches")}
          disabled={mustCompleteProfile}
          title={mustCompleteProfile ? "Save your profile first" : undefined}
        >
          Match forecasts
        </button>
        <button
          type="button"
          className={tab === "tournament" ? "active" : ""}
          onClick={() => setTab("tournament")}
          disabled={mustCompleteProfile}
          title={mustCompleteProfile ? "Save your profile first" : undefined}
        >
          Tournament picks
        </button>
        <button
          type="button"
          className={tab === "leaderboard" ? "active" : ""}
          onClick={() => setTab("leaderboard")}
          disabled={mustCompleteProfile}
          title={mustCompleteProfile ? "Save your profile first" : undefined}
        >
          Leaderboard
        </button>
        <button type="button" className={tab === "profile" ? "active" : ""} onClick={() => setTab("profile")}>
          My profile
        </button>
        <button type="button" className="ghost" onClick={() => void load()} disabled={loading}>
          Refresh
        </button>
        <button type="button" className="ghost dark-toggle" onClick={toggleDarkMode} title={darkMode ? "Switch to light mode" : "Switch to dark mode"}>
          {darkMode ? "☀️" : "🌙"}
        </button>
      </nav>

      <ToastContainer toasts={toast.toasts} onDismiss={toast.dismiss} />

      {loading && <p className="muted center">Loading…</p>}

      {!loading && tab === "matches" && (
        <section className="panel">
          <div className="toolbar">
          <h2>Match forecasts</h2>
            <div className="toolbar-actions">
              {import.meta.env.DEV && (
                <button type="button" className="secondary" onClick={() => void seedDemo()}>
                  Seed demo matches
                </button>
              )}
              <button type="button" className="primary" onClick={() => void saveMatches()}>
                Save match forecasts
              </button>
            </div>
          </div>
          {matches.length === 0 && (
            <div className="muted empty-matches-hint">
              <p>
                <strong>No matches available yet.</strong> The schedule will appear here once the tournament fixtures are loaded.
              </p>
            </div>
          )}
          {groupStageByGroup.size > 0 && (
            <p className="muted small group-standings-intro">
              Fill in your predicted scores and hit <strong>Save match forecasts</strong>.
              {hasProjections && (
                <> Knockout matchups update automatically based on your predictions &mdash;{" "}
                <span className="projected-team">amber</span> means the teams are not yet confirmed.
                You can predict the entire tournament at once!</>
              )}
            </p>
          )}
          {sortGroupKeys([...groupStageByGroup.keys()]).map((gk) => {
            const rows = groupStageByGroup.get(gk)!;
            const apiEntry = groupRosters.groups.find((g) => g.group_key === gk);
            const derived = deriveRosterFromGroupMatches(rows);
            let roster: RosterTeam[] | undefined;
            if (apiEntry && apiEntry.teams.length >= 2 && apiEntry.teams.length <= 4) {
              roster = apiEntry.teams;
            } else if (derived.length === 4) {
              roster = derived;
            }
            const standings = computeStandingsFromPredictions(rows, draft, roster);
            const title =
              gk === "_OTHER" ? "Group stage (no API group)" : formatGroupLabel(gk);
            const rankingHint =
              roster?.length === 4
                ? "Based on your predicted scores — not official results."
                : roster && roster.length >= 2
                  ? "Partial group — more teams will appear once the full schedule is loaded."
                  : "Based on your predicted scores — not official results.";
            const isCollapsed = collapsedGroups.has(gk);
            const toggleCollapse = () => setCollapsedGroups((prev) => {
              const next = new Set(prev);
              if (next.has(gk)) next.delete(gk); else next.add(gk);
              return next;
            });
            const allFinished = rows.every((r) => r.status === "FINISHED");
            return (
              <div key={gk} className={`group-stage-card${isCollapsed ? " collapsed" : ""}`}>
                <button type="button" className="group-collapse-toggle" onClick={toggleCollapse}>
                  <h3 className="group-stage-heading">
                    <span className={`collapse-chevron${isCollapsed ? " collapsed" : ""}`}>▸</span>
                    {title}
                    {allFinished && <span className="group-done-badge">Done</span>}
                  </h3>
                </button>
                {!isCollapsed && (
                  <>
                <p className="group-stage-sub muted small">
                  {rows.length} match{rows.length === 1 ? "" : "es"} in this group
                  {apiEntry?.is_complete ? " · 4-team group" : ""}
                </p>
                <div className="table-wrap group-matches-wrap">
                  <MatchTable rows={rows} draft={draft} setDraft={setDraft} saved={savedDraft} recentlyFinished={recentlyFinished} />
                </div>
                <h4 className="ranking-subtitle">Predicted ranking (live)</h4>
                <p className="muted small ranking-hint">{rankingHint}</p>
                <div className="table-wrap standings-wrap">
                  <table className="grid standings">
                    <thead>
                      <tr>
                        <th className="narrow">#</th>
                        <th>Team</th>
                        <th className="stat">P</th>
                        <th className="stat">W</th>
                        <th className="stat">D</th>
                        <th className="stat">L</th>
                        <th className="stat">GF</th>
                        <th className="stat">GA</th>
                        <th className="stat">GD</th>
                        <th className="stat">Pts</th>
                      </tr>
                    </thead>
                    <tbody>
                      {standings.map((s) => (
                        <tr key={s.teamCode}>
                          <td className="mono narrow">{s.rank}</td>
                          <td>
                            <TeamName code={s.teamCode} name={s.teamName} className="team" />
                            <span className="code">({s.teamCode})</span>
                          </td>
                          <td className="stat mono">{s.played}</td>
                          <td className="stat mono">{s.wins}</td>
                          <td className="stat mono">{s.draws}</td>
                          <td className="stat mono">{s.losses}</td>
                          <td className="stat mono">{s.goalsFor}</td>
                          <td className="stat mono">{s.goalsAgainst}</td>
                          <td className="stat mono">{s.goalDiff >= 0 ? `+${s.goalDiff}` : s.goalDiff}</td>
                          <td className="stat mono points">{s.points}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                  </>
                )}
              </div>
            );
          })}
          {/* ---- Knockout matches (with projected team names for TBD fixtures) ---- */}
          {otherByStage.size > 0 && (
            <p className="muted small">
              <strong>Knockout stages:</strong> predict the score <strong>after extra time</strong> (120 min).
              If you predict a draw, pick who wins on penalties.
              All points count <strong>double</strong> in knockouts!
              {hasProjections && (
                <> Teams in <span className="projected-team">amber</span> are projected from your predictions
                and update as you fill in scores.</>
              )}
            </p>
          )}
          {[...otherByStage.entries()].map(([stage, rows]) => (
            <div key={stage} className="stage-block">
              <h3 className="stage-title">{stageDisplayName(stage)}</h3>
              <div className="table-wrap">
                <MatchTable rows={rows} draft={draft} setDraft={setDraft} projections={knockoutProjections} saved={savedDraft} recentlyFinished={recentlyFinished} />
              </div>
            </div>
          ))}
          <div className="toolbar-bottom">
            <button type="button" className="primary" onClick={() => void saveMatches()}>
              Save match forecasts
            </button>
          </div>
        </section>
      )}

      {!loading && tab === "tournament" && tournament && (
        <section className="panel">
          <h2>Tournament picks</h2>
          <p className="muted small">
            Pick your champion and top scorers before{" "}
            <strong>{fmtTournamentLockUtc(tournament.tournament_picks_lock_at_utc)}</strong> — after that, these picks are locked.
          </p>
          {!tournament.tournament_open && (
            <p className="muted">
              Tournament picks are locked — the deadline has passed.
            </p>
          )}
          <div className="form-grid">
            <label>
              <span className="field-label-row">
                <span>Tournament winner</span>
                <span
                  className="awarded-badge muted small"
                  title="Awarded points for your saved champion pick (+25 after the final if correct). Save to refresh if you change the selection."
                >
                  +{tournament.points_tournament_winner ?? 0} pts
                </span>
              </span>
              {teams.length > 0 ? (
                <select
                  key={teams.map((t) => t.code).join("|")}
                  disabled={!tournament.tournament_open}
                  value={winner}
                  onChange={(e) => setWinner(e.target.value)}
                >
                  <option value="">— Select —</option>
                  {teams.map((t) => (
                    <option key={t.code} value={t.code}>
                      {t.name} ({t.code})
                    </option>
                  ))}
                </select>
              ) : (
                <input
                  type="text"
                  disabled={!tournament.tournament_open}
                  value={winner}
                  onChange={(e) => setWinner(e.target.value.toUpperCase())}
                  placeholder="e.g. GER (sync matches to load team list)"
                />
              )}
            </label>
            <div className="player-pick-field">
              <span className="player-pick-label">Top scorers (World Cup squads)</span>
              <p className="muted small player-pick-hint">
                Search and pick up to {MAX_TOP_SCORERS} players you think will score the most goals.
              </p>
              {topScorerPicks.length > 0 && (
                <ul className="chip-list" aria-label="Selected top scorers">
                  {topScorerPicks.map((p) => (
                    <li key={scorerKey(p)} className="chip">
                      <span>
                        {p.player_name}{" "}
                        <span className="code">({p.country_name})</span>{" "}
                        <span
                          className="chip-points muted small"
                          title="Points when this player scores in a match you predicted (2× in knockout)"
                        >
                          +{p.points_awarded ?? 0} pts
                        </span>
                      </span>
                      {tournament.tournament_open && (
                        <button
                          type="button"
                          className="chip-remove"
                          aria-label={`Remove ${p.player_name}`}
                          onClick={() =>
                            setTopScorerPicks((prev) => prev.filter((x) => scorerKey(x) !== scorerKey(p)))
                          }
                        >
                          ×
                        </button>
                      )}
                    </li>
                  ))}
                </ul>
              )}
              {tournament.tournament_open && (
                <>
                  <input
                    type="search"
                    className="player-pick-search"
                    disabled={!tournament.tournament_open || topScorerPicks.length >= MAX_TOP_SCORERS}
                    value={playerSearch}
                    onChange={(e) => setPlayerSearch(e.target.value)}
                    placeholder={
                      topScorerPicks.length >= MAX_TOP_SCORERS
                        ? `Maximum ${MAX_TOP_SCORERS} picks`
                        : "Search by player or country…"
                    }
                    autoComplete="off"
                    spellCheck={false}
                  />
                  {playerCatalogLoading && <p className="muted small">Loading player list…</p>}
                  {!playerCatalogLoading && playerCatalog.length === 0 && (
                    <p className="muted small">Player list not available yet — check back closer to the tournament.</p>
                  )}
                  {!playerCatalogLoading && playerCatalog.length > 0 && (
                    <ul className="player-pick-results" role="listbox" aria-label="Matching players">
                      {filteredWorldcupPlayers.length === 0 ? (
                        <li className="player-pick-empty muted small">No matches — try another search.</li>
                      ) : (
                        filteredWorldcupPlayers.map((p) => (
                          <li key={scorerKey(p)}>
                            <button
                              type="button"
                              className="player-pick-row"
                              disabled={topScorerPicks.length >= MAX_TOP_SCORERS}
                              onClick={() => {
                                if (topScorerPicks.length >= MAX_TOP_SCORERS) return;
                                setTopScorerPicks((prev) => {
                                  if (prev.some((x) => scorerKey(x) === scorerKey(p))) return prev;
                                  return [
                                    ...prev,
                                    {
                                      player_name: p.player_name,
                                      country_code: p.country_code,
                                      country_name: p.country_name,
                                      points_awarded: 0,
                                    },
                                  ];
                                });
                                setPlayerSearch("");
                              }}
                            >
                              <span className="player-pick-name">{p.player_name}</span>
                              <span className="player-pick-meta muted small">
                                {p.country_name} <span className="code">({p.country_code})</span>
                              </span>
                            </button>
                          </li>
                        ))
                      )}
                    </ul>
                  )}
                </>
              )}
            </div>
          </div>
          <button
            type="button"
            className="primary"
            disabled={!tournament.tournament_open}
            onClick={() => void saveTournament()}
          >
            Save tournament picks
          </button>
        </section>
      )}

      {!loading && tab === "leaderboard" && (
        <section className="panel">
          <h2>Leaderboard</h2>
          {dashboardErr ? (
            <p className="banner err" role="alert">
              Could not load leaderboard: {dashboardErr}
            </p>
          ) : null}
          <div className="scoring-rules">
            <p className="muted small">
              Points are awarded per match based on your prediction. Knockout rounds are worth more!
              Advancer points: predict a team reaches the next round — you get credit regardless of which match they came from.
              Tournament winner = <strong>+25 pts</strong>. Click any name to view their picks.
            </p>
            <div className="table-wrap scoring-table-wrap">
              <table className="grid scoring-table">
                <thead>
                  <tr>
                    <th>Round</th>
                    <th className="stat">×</th>
                    <th className="stat">Outcome</th>
                    <th className="stat">Exact score</th>
                    <th className="stat">Scorer</th>
                    <th className="stat">Advancer</th>
                  </tr>
                </thead>
                <tbody>
                  <tr><td>Group</td><td className="stat mono">×1</td><td className="stat mono">2</td><td className="stat mono">5</td><td className="stat mono">2</td><td className="stat mono">—</td></tr>
                  <tr><td>R32</td><td className="stat mono">×1.5</td><td className="stat mono">3</td><td className="stat mono">8</td><td className="stat mono">3</td><td className="stat mono">5</td></tr>
                  <tr><td>R16</td><td className="stat mono">×2</td><td className="stat mono">4</td><td className="stat mono">10</td><td className="stat mono">4</td><td className="stat mono">6</td></tr>
                  <tr><td>QF</td><td className="stat mono">×2.5</td><td className="stat mono">5</td><td className="stat mono">13</td><td className="stat mono">5</td><td className="stat mono">8</td></tr>
                  <tr><td>SF / 3rd / Final</td><td className="stat mono">×3</td><td className="stat mono">6</td><td className="stat mono">15</td><td className="stat mono">6</td><td className="stat mono">9</td></tr>
                </tbody>
              </table>
            </div>
          </div>
          <div className="dashboard-summary">
            <div className="stat-card dashboard-stat">
              <span className="stat-label">Participants</span>
              <span className="stat-value">{dashboard?.predictors_count ?? 0}</span>
              <span className="muted small">Colleagues in the pool</span>
            </div>
            <div className="stat-card dashboard-stat">
              <span className="stat-label">Your rank</span>
              <span className="stat-value">
                {me && (dashboard?.leaderboard.entries ?? []).some((e) => e.user_id === me.user_id)
                  ? (dashboard?.leaderboard.entries ?? []).find((e) => e.user_id === me.user_id)?.rank ?? "—"
                  : "—"}
              </span>
              <span className="muted small">Out of all participants</span>
            </div>
            <div className="stat-card dashboard-stat">
              <span className="stat-label">Your points</span>
              <span className="stat-value">
                {me
                  ? (dashboard?.leaderboard.entries ?? []).find((e) => e.user_id === me.user_id)?.total_points ?? 0
                  : "—"}
              </span>
              <span className="muted small">Live scoring from finished matches</span>
            </div>
          </div>
          {(dashboard?.leaderboard.entries ?? []).length === 0 ? (
            <p className="muted">
              No participants yet — be the first to join by saving your profile!
            </p>
          ) : (
            <div className="table-wrap">
              <table className="grid ranking-table">
                <thead>
                  <tr>
                    <th className="narrow">#</th>
                    <th>Participant</th>
                    <th className="stat" title="Match lines saved (both scores set)">
                      #-Predictions
                    </th>
                    <th className="stat">Points</th>
                    <th className="stat" title="Winner or draw (non-exact)">
                      W/D
                    </th>
                    <th className="stat" title="Exact scoreline">
                      Exact
                    </th>
                    <th className="stat" title="Top scorer picks who scored">
                      TS
                    </th>
                    <th className="stat" title="Knockout: correct team to next round">
                      Adv
                    </th>
                    <th className="stat" title="Tournament champion pick">
                      TW
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {(dashboard?.leaderboard.entries ?? []).map((e) => (
                    <tr key={e.user_id} className={me?.user_id === e.user_id ? "ranking-you" : undefined}>
                      <td className="narrow rank-cell"><MedalIcon rank={e.rank} /></td>
                      <td>
                        <button
                          type="button"
                          className="leaderboard-user-hit"
                          onClick={() => void openParticipantProfile(e.user_id)}
                          title="View pool profile"
                        >
                          {e.profile_picture ? (
                            <img src={e.profile_picture} alt="" className="leaderboard-avatar" />
                          ) : (
                            <span className="leaderboard-avatar avatar-placeholder" aria-hidden>
                              <DefaultProfileIcon />
                            </span>
                          )}
                          <span className="leaderboard-user-name team">
                            {e.display_name || e.email || e.user_id}
                          </span>
                        </button>
                      </td>
                      <td className="stat mono">{e.match_predictions_filled ?? 0}</td>
                      <td className="stat mono points">{e.total_points}</td>
                      <td className="stat mono">{e.points_outcome}</td>
                      <td className="stat mono">{e.points_exact}</td>
                      <td className="stat mono">{e.points_scorer_goals}</td>
                      <td className="stat mono">{e.points_advancer ?? 0}</td>
                      <td className="stat mono">{e.points_tournament_winner}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      )}

      {!loading && tab === "profile" && profile && (
        <section className="panel">
          <h2>My profile</h2>
          {mustCompleteProfile && (
            <p className="banner warn">
              Welcome! Set up your profile to get started with the prediction pool.
            </p>
          )}
          <p className="muted">
            This is how you appear on the leaderboard.
          </p>
          <div className="profile-layout">
            <div>
              {profilePicture ? (
                <img src={profilePicture} alt="Profile preview" className="profile-avatar-large" />
              ) : (
                <div className="profile-avatar-large avatar-placeholder" aria-hidden>
                  <DefaultProfileIcon />
                </div>
              )}
            </div>
            <div className="form-grid">
              <label>
                Display name
                <input
                  type="text"
                  value={profileName}
                  onChange={(e) => setProfileName(e.target.value)}
                  placeholder="How others will see you"
                />
              </label>
              <label>
                Nationality
                <input
                  type="text"
                  value={profileNationality}
                  onChange={(e) => setProfileNationality(e.target.value)}
                  placeholder="e.g. Dutch"
                />
              </label>
              <label>
                Profile picture
                <input type="file" accept="image/*" onChange={(e) => onPictureFileSelected(e.target.files?.[0] ?? null)} />
              </label>
              <p className="muted small">Last updated: {fmtUpdated(profile.updated_at)}</p>
              <button type="button" className="primary" onClick={() => void saveProfile()}>
                Save profile
              </button>
            </div>
          </div>
        </section>
      )}

      {profileModalUserId !== null && (
        <div
          className="modal-backdrop"
          role="presentation"
          onClick={() => closeParticipantProfileModal()}
        >
          <div
            className="modal-card"
            role="dialog"
            aria-modal="true"
            aria-labelledby="participant-profile-title"
            onClick={(ev) => ev.stopPropagation()}
          >
            <div className="modal-head">
              <h2 id="participant-profile-title">
                {profileModal?.display_name?.trim()
                  ? `Pool profile — ${profileModal.display_name}`
                  : "Pool profile"}
              </h2>
              <button
                type="button"
                className="modal-close"
                aria-label="Close"
                onClick={() => closeParticipantProfileModal()}
              >
                ×
              </button>
            </div>
            <div className="modal-body">
              {profileModalLoading && <p className="muted">Loading…</p>}
              {profileModalErr && <p className="banner err modal-inline-err">{profileModalErr}</p>}
              {profileModal && (
                <>
                  <p className="muted small modal-user-id mono">User id: {profileModal.user_id}</p>
                  <div className="participant-modal-layout">
                    <div>
                      {profileModal.profile_picture ? (
                        <img src={profileModal.profile_picture} alt="" className="profile-avatar-large" />
                      ) : (
                        <div className="profile-avatar-large avatar-placeholder" aria-hidden>
                          <DefaultProfileIcon />
                        </div>
                      )}
                    </div>
                    <dl className="participant-facts">
                      <dt>Display name</dt>
                      <dd>{profileModal.display_name || "—"}</dd>
                      <dt>Nationality</dt>
                      <dd>{profileModal.nationality || "—"}</dd>
                      <dt>Match lines saved</dt>
                      <dd>{profileModal.match_predictions_saved}</dd>
                      <dt>Profile updated</dt>
                      <dd>{fmtUpdated(profileModal.updated_at)}</dd>
                    </dl>
                  </div>
                  <h3 className="modal-subtitle">Tournament picks (pool)</h3>
                  <p className="muted small">Winner and top scorers they entered for scoring.</p>
                  <dl className="participant-facts">
                    <dt>Tournament winner pick</dt>
                    <dd>{formatTeamPick(profileModal.tournament_winner_team_code, teams)}</dd>
                  </dl>
                  {profileModal.top_scorers.length > 0 ? (
                    <ul className="participant-scorer-list">
                      {profileModal.top_scorers.map((p, i) => (
                        <li key={`${scorerKey(p)}-${i}`}>
                          {p.player_name} <span className="code">({p.country_name})</span>
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <p className="muted small">No top scorer picks saved.</p>
                  )}
                </>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
