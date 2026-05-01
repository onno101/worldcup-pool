/**
 * Must stay aligned with worldcup_pool/team_tla.py canonical_team_tla().
 * Used so saved advancer codes match checkbox / winner styling when feeds use alternate TLAs.
 */
const TEAM_TLA_CANONICAL: Record<string, string> = {
  CUR: "CUW",
  DEU: "GER",
  HOL: "NED",
  SCT: "SCO",
};

export function canonicalTeamTla(raw: string | null | undefined): string {
  const s = (raw || "").trim().toUpperCase();
  if (!s) return "";
  const clipped = s.length > 16 ? s.slice(0, 16) : s;
  return TEAM_TLA_CANONICAL[clipped] ?? clipped;
}

export function teamTlaEquals(a: string | null | undefined, b: string | null | undefined): boolean {
  const ca = canonicalTeamTla(a);
  const cb = canonicalTeamTla(b);
  return !!ca && ca === cb;
}

/** Map a saved champion code to a `<select>` option `value` when the roster uses an alternate TLA. */
export function winnerCodeForTeamSelect(
  saved: string | null | undefined,
  teams: readonly { code: string }[],
): string {
  const s = (saved ?? "").trim();
  if (!s) return "";
  if (!teams.length) return canonicalTeamTla(s) || s.toUpperCase();
  const up = s.toUpperCase();
  const exact = teams.find((t) => t.code.toUpperCase() === up);
  if (exact) return exact.code;
  const alias = teams.find((t) => teamTlaEquals(t.code, s));
  if (alias) return alias.code;
  const c = canonicalTeamTla(s);
  return c && c !== "?" ? c : s.toUpperCase();
}
