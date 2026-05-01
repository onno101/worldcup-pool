/**
 * Resolve `/api/...` against the SPA’s URL (not the site root). Databricks Apps (and similar)
 * often serve the UI under a subpath; `fetch("/api/me")` would hit the workspace root and never
 * reach this app, leaving the UI stuck on “Loading…”.
 */
function resolveFetchUrl(path: string): string {
  const p = path.startsWith("/") ? path : `/${path}`;
  if (import.meta.env.DEV) {
    return p;
  }
  const viteBase = import.meta.env.BASE_URL;
  if (viteBase && viteBase !== "/" && viteBase !== "./") {
    const b = viteBase.endsWith("/") ? viteBase.slice(0, -1) : viteBase;
    return `${b}${p}`;
  }
  if (typeof window === "undefined") {
    return p;
  }
  let dirPath = window.location.pathname;
  if (dirPath.endsWith(".html")) {
    dirPath = dirPath.slice(0, dirPath.lastIndexOf("/") + 1);
  } else if (!dirPath.endsWith("/")) {
    dirPath += "/";
  }
  return new URL(`.${p}`, `${window.location.origin}${dirPath}`).href;
}

/** Keep failures visible; Lakebase cold starts can be slow but should not hang forever. */
const API_TIMEOUT_MS = 45_000;

async function fetchWithTimeout(
  path: string,
  init: RequestInit | undefined,
  ms: number,
): Promise<Response> {
  const url = resolveFetchUrl(path);
  const controller = new AbortController();
  const id = window.setTimeout(() => controller.abort(), ms);
  try {
    return await fetch(url, { ...init, signal: controller.signal, credentials: "same-origin" });
  } catch (e) {
    const name = e instanceof Error ? e.name : "";
    if (name === "AbortError") {
      throw new Error(`Request timed out after ${Math.round(ms / 1000)}s (${path})`);
    }
    throw e;
  } finally {
    window.clearTimeout(id);
  }
}

function headers(): HeadersInit {
  const h: Record<string, string> = { Accept: "application/json" };
  if (import.meta.env.DEV && import.meta.env.VITE_DEV_ACCESS_TOKEN) {
    h["x-forwarded-access-token"] = import.meta.env.VITE_DEV_ACCESS_TOKEN;
  }
  return h;
}

export async function apiGet<T>(path: string): Promise<T> {
  const r = await fetchWithTimeout(path, { headers: headers() }, API_TIMEOUT_MS);
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return r.json() as Promise<T>;
}

export async function apiPut<T>(path: string, body: unknown): Promise<T> {
  const r = await fetchWithTimeout(
    path,
    {
      method: "PUT",
      headers: { ...headers(), "Content-Type": "application/json" },
      body: JSON.stringify(body),
    },
    API_TIMEOUT_MS,
  );
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return r.json() as Promise<T>;
}

export async function apiPost<T>(path: string): Promise<T> {
  const r = await fetchWithTimeout(path, { method: "POST", headers: headers() }, API_TIMEOUT_MS);
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return r.json() as Promise<T>;
}
