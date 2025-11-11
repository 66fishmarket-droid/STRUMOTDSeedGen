// scripts/fetch-otd.js
// Strum OTD GitHub automation: fetch Wikipedia OnThisDay, classify via Wikidata.
// Usage examples:
//   node scripts/fetch-otd.js
//   node scripts/fetch-otd.js --date=2025-12-13 --out=data/otd
//   node scripts/fetch-otd.js --out=outputs --file=custom.json
//   node scripts/fetch-otd.js --stdout --kinds=events,births --debug
// Env override: WD_SPARQL=https://query.wikidata.org/sparql

import fetch from "node-fetch";

// Endpoints
const WIKI = "https://en.wikipedia.org/api/rest_v1";
const WD_SPARQL = process.env.WD_SPARQL || "https://query.wikidata.org/sparql";

// CLI helpers
const arg = (name, dflt) => {
  const raw = process.argv.find(a => a.startsWith(`--${name}=`));
  return raw ? raw.split("=").slice(1).join("=") : dflt;
};
const FLAG_DEBUG = process.argv.includes("--debug") || process.argv.includes("--debug-sparql");
const FLAG_STDOUT = process.argv.includes("--stdout");
const ARG_DATE = arg("date", process.env.TARGET_DATE);
const KINDS = (arg("kinds", "events,births,deaths,selected") || "")
  .split(",").map(s => s.trim()).filter(Boolean);
const MAX_BATCH = Math.max(1, parseInt(arg("max-batch", "35"), 10) || 35);

// Output controls
const OUT_DIR = arg("out", "data/otd");
const OUT_FILE = arg("file", null);

// Date selection (UTC)
const base = ARG_DATE ? new Date(ARG_DATE + "T00:00:00Z") : new Date();
const MM = String(base.getUTCMonth() + 1).padStart(2, "0");
const DD = String(base.getUTCDate()).padStart(2, "0");
const KEY = `${MM}-${DD}`;
const KEY_SLASH = `${MM}/${DD}`;

// Utils
const sleep = ms => new Promise(res => setTimeout(res, ms));
const uniq = arr => [...new Set(arr)];
const safe = (v, d = "") => (v ?? d);
function chunk(arr, n) { const out = []; for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n)); return out; }

// Fetch helpers
async function fetchTextWithRetry(url, opts = {}, retries = 3) {
  let lastErr;
  for (let i = 1; i <= retries; i++) {
    const r = await fetch(url, opts);
    const ct = (r.headers.get("content-type") || "").toLowerCase();
    const text = await r.text();
    if (!r.ok) {
      if (r.status === 429 || r.status >= 500) {
        lastErr = new Error(`HTTP ${r.status} ${url} :: ${text.slice(0, 200)}`);
        if (i < retries) { await sleep(1000 * i); continue; }
      }
      throw new Error(`HTTP ${r.status} ct=${ct} :: ${text.slice(0, 200)}`);
    }
    return { ct, text };
  }
  throw lastErr || new Error("fetch failed");
}

async function fetchJsonWithRetry(url, opts = {}, retries = 3) {
  const { ct, text } = await fetchTextWithRetry(url, opts, retries);
  try { return JSON.parse(text); }
  catch { throw new Error(`JSON parse error ct=${ct} :: ${text.slice(0, 200)}`); }
}

// Wikipedia OnThisDay
async function onThisDay(kind) {
  const url = `${WIKI}/feed/onthisday/${kind}/${MM}/${DD}`;
  const j = await fetchJsonWithRetry(url, {
    headers: { "User-Agent": "StrumOTD/1.0 (GitHub Actions)" }
  });
  return Array.isArray(j?.[kind]) ? j[kind] : [];
}

// QID collection tuned for the OTD payload
// Notes:
// - births/deaths: include all person QIDs found (we re-check in SPARQL).
// - events/selected: skip day pages and clearly non-arts entities; allow cultural pages to pass.
function collectQidsOTD(items, kind) {
  const qids = [];
  const dayPagePattern = /^[A-Za-z]+_\d{1,2}$/; // e.g., November_11
  const genericDescTerms = [
    "day of the year", "calendar day", "month",
    "city", "country", "province", "state", "capital",
    "war", "battle", "army", "navy", "air force", "military",
    "government", "ministry", "president", "prime minister",
    "king", "queen", "emperor", "politician", "political party"
  ];
  const culturalHints = /(singer|artist|actor|band|film|album|writer|poet|author|novel|song|composer|director|musician|performer|painter|sculptor|award|ceremony|festival|orchestra|theatre|theater|producer|screenwriter|playwright|dancer|dj|rapper|comedian)/i;

  for (const it of items) {
    const pages = it?.pages || [];
    for (const p of pages) {
      const qid = p?.wikibase_item;
      if (!qid) continue;

      const title = (p.titles?.normalized || p.title || "");
      const desc = (p.description || "").toLowerCase();

      // Ignore day pages like "November_11"
      if (dayPagePattern.test(title)) continue;

      // births/deaths: pass people; we will validate via SPARQL occupations
      if (kind === "births" || kind === "deaths") {
        qids.push(qid);
        continue;
      }

      // For events/selected: avoid obvious non-arts
      if (genericDescTerms.some(t => desc.includes(t))) continue;

      // Allow likely cultural entities
      if (culturalHints.test(desc)) {
        qids.push(qid);
        continue;
      }

      // Also allow if title or extract likely references a work category
      const tLower = title.toLowerCase();
      if (/(film|album|song|novel|play|poem|opera|symphony|single|ep|mixtape)/i.test(tLower)) {
        qids.push(qid);
        continue;
      }
    }
  }

  return uniq(qids);
}

// SPARQL runner: try GET first, then form-encoded POST on 400/415
async function runSparql(query, headers) {
  // GET
  const url = `${WD_SPARQL}?query=${encodeURIComponent(query)}`;
  const getRes = await fetch(url, { method: "GET", headers });
  const getCT = (getRes.headers.get("content-type") || "").toLowerCase();
  const getText = await getRes.text();
  if (getRes.ok && getCT.includes("application/sparql-results+json")) {
    return JSON.parse(getText);
  }

  // Fallback POST (form-encoded) on common parser/CT errors
  if (!getRes.ok && (getRes.status === 400 || getRes.status === 415)) {
    const postRes = await fetch(WD_SPARQL, {
      method: "POST",
      headers: { ...headers, "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" },
      body: `query=${encodeURIComponent(query)}`
    });
    const postCT = (postRes.headers.get("content-type") || "").toLowerCase();
    const postText = await postRes.text();
    if (!postRes.ok || !postCT.includes("application/sparql-results+json")) {
      throw new Error(`SPARQL error (POST) status=${postRes.status} ct=${postCT} snippet=${postText.slice(0,200)}`);
    }
    return JSON.parse(postText);
  }

  throw new Error(`SPARQL error (GET) status=${getRes.status} ct=${getCT} snippet=${getText.slice(0,200)}`);
}

// Arts/classification filter via Wikidata
async function filterArts(qids) {
  if (!qids || qids.length === 0) return { keep: new Set(), categoryMap: new Map() };

  const headers = {
    "User-Agent": "StrumOTD/1.0 (+https://github.com/66fishmarket-droid/STRUMOTDSeedGen)",
    "Accept": "application/sparql-results+json"
  };

  const keep = new Set();
  const categoryMap = new Map();

  // Cap batch size and re-split if URL grows too long
  const maxBatch = Math.min(MAX_BATCH, 35);

  for (const batch of chunk(qids, maxBatch)) {
    const VALUES = batch.map(q => `wd:${q}`).join(" ");

    const query = `
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>

SELECT ?item ?category WHERE {
  VALUES ?item { ${VALUES} }

  BIND(COALESCE(

    # WORKS: Music (album, single, song, EP, concert tour, award)
    IF(EXISTS {
      ?item wdt:P31/wdt:P279* ?c1 .
      FILTER(?c1 IN (
        wd:Q482994,  # album
        wd:Q134556,  # single
        wd:Q7366,    # song
        wd:Q105420,  # EP
        wd:Q182832,  # concert tour
        wd:Q222634   # music award
      ))
    },"music",""),

    # WORKS: Film/TV (film, TV series, episode, festival)
    IF(EXISTS {
      ?item wdt:P31/wdt:P279* ?c2 .
      FILTER(?c2 IN (
        wd:Q11424,     # film
        wd:Q5398426,   # television series
        wd:Q21191270,  # television episode
        wd:Q226730     # film festival
      ))
    },"film_tv",""),

    # WORKS: Books (book, written work)
    IF(EXISTS {
      ?item wdt:P31/wdt:P279* ?c3 .
      FILTER(?c3 IN (
        wd:Q7725634,   # literary work
        wd:Q571        # book
      ))
    },"books",""),

    # WORKS: Visual/Performance (painting, sculpture, dance, theatre, performance)
    IF(EXISTS {
      ?item wdt:P31/wdt:P279* ?c4 .
      FILTER(?c4 IN (
        wd:Q3305213,  # painting
        wd:Q860861,   # sculpture
        wd:Q2798201,  # performance
        wd:Q2431196,  # theatre production
        wd:Q2743      # opera
      ))
    },"visual_or_performance",""),

    # WORKS: Awards (award, prize)
    IF(EXISTS {
      ?item wdt:P31/wdt:P279* ?c5 .
      FILTER(?c5 IN (wd:Q618779, wd:Q132241))
    },"awards",""),

    # HUMANS: Music occupations
    IF(EXISTS {
      ?item wdt:P31 wd:Q5 ; wdt:P106/wdt:P279* ?o1 .
      FILTER(?o1 IN (
        wd:Q639669,  # singer
        wd:Q177220,  # musician
        wd:Q488205,  # guitarist
        wd:Q36834,   # composer
        wd:Q1128996, # songwriter
        wd:Q130857,  # conductor
        wd:Q155309,  # drummer
        wd:Q973127,  # bassist
        wd:Q158852,  # pianist
        wd:Q14623646 # DJ
      ))
    },"music",""),

    # HUMANS: Film/TV occupations
    IF(EXISTS {
      ?item wdt:P31 wd:Q5 ; wdt:P106/wdt:P279* ?o2 .
      FILTER(?o2 IN (
        wd:Q33999,    # actor
        wd:Q2526255,  # film director
        wd:Q28389,    # producer
        wd:Q3455803,  # cinematographer
        wd:Q10800557  # screenwriter
      ))
    },"film_tv",""),

    # HUMANS: Books occupations
    IF(EXISTS {
      ?item wdt:P31 wd:Q5 ; wdt:P106/wdt:P279* ?o3 .
      FILTER(?o3 IN (
        wd:Q36180,    # writer
        wd:Q482980,   # poet
        wd:Q49757,    # novelist
        wd:Q214917,   # playwright
        wd:Q6625963   # essayist
      ))
    },"books",""),

    # HUMANS: Visual/Performance occupations
    IF(EXISTS {
      ?item wdt:P31 wd:Q5 ; wdt:P106/wdt:P279* ?o4 .
      FILTER(?o4 IN (
        wd:Q33231,   # painter
        wd:Q42973,   # sculptor
        wd:Q571668,  # dancer
        wd:Q245068,  # photographer
        wd:Q1028181, # performance artist
        wd:Q1281618  # theatre director
      ))
    },"visual_or_performance","")

  ) AS ?category)

  FILTER(STRLEN(?category) > 0)
}
`.trim();

    // Safety: if GET URL would exceed ~7k, split batch once more
    const testUrl = `${WD_SPARQL}?query=${encodeURIComponent(query)}`;
    if (testUrl.length > 7000 && batch.length > 10) {
      const halves = chunk(batch, Math.ceil(batch.length / 2));
      for (const half of halves) {
        const { keep: k2, categoryMap: m2 } = await filterArts(half);
        k2.forEach(q => keep.add(q));
        for (const [k, v] of m2.entries()) categoryMap.set(k, v);
      }
      continue;
    }

    if (FLAG_DEBUG) {
      console.error(`[SPARQL] batch size=${batch.length} urlLen=${testUrl.length}`);
    }

    const j = await runSparql(query, headers);

    for (const b of (j?.results?.bindings || [])) {
      const qid = b.item.value.split("/").pop();
      keep.add(qid);
      categoryMap.set(qid, b.category.value);
    }

    // gentle pacing
    await sleep(250);
  }

  return { keep, categoryMap };
}

// Flatten items back to a minimal record set, keeping first good page per item
function flatten(items) {
  const out = new Map();
  const dayPagePattern = /^[A-Za-z]+_\d{1,2}$/;

  for (const it of items) {
    const pages = it?.pages || [];
    // pick first non-day-page with a QID
    const page = pages.find(p => p?.wikibase_item && !dayPagePattern.test(p?.title || ""));
    if (!page) continue;

    const yearFromField = it?.year;
    const yearFromText = it?.text?.match(/\b(\d{3,4})\b/)?.[1];
    const year = Number(yearFromField || yearFromText) || null;

    const qid = page.wikibase_item;
    const url = page?.content_urls?.desktop?.page
      || `https://en.wikipedia.org/wiki/${encodeURIComponent(page.title)}`;
    const title = safe(page?.titles?.normalized, page?.title);

    if (!out.has(qid)) {
      out.set(qid, {
        qid,
        key_mmdd: KEY_SLASH,
        title,
        summary: safe(it?.text, ""),
        url,
        year,
        event_mmdd: KEY_SLASH
      });
    }
  }

  return [...out.values()];
}

(async () => {
  try {
    // Pull all requested kinds
    const pulls = await Promise.all(KINDS.map(k => onThisDay(k)));
    const itemsByKind = Object.fromEntries(KINDS.map((k, i) => [k, pulls[i] || []]));
    const allItems = pulls.flat();

    // QIDs per kind (births/deaths permissive, events/selected filtered)
    const qids = [
      ...collectQidsOTD(itemsByKind.events || [], "events"),
      ...collectQidsOTD(itemsByKind.selected || [], "selected"),
      ...collectQidsOTD(itemsByKind.births || [], "births"),
      ...collectQidsOTD(itemsByKind.deaths || [], "deaths")
    ];
    const uniqQids = uniq(qids);

    if (FLAG_DEBUG) {
      console.error(`Collected ${uniqQids.length} candidate QIDs for ${KEY_SLASH}`);
    }

    // Classify via Wikidata
    const { keep, categoryMap } = await filterArts(uniqQids);

    if (FLAG_DEBUG) {
      console.error(`SPARQL kept ${keep.size} QIDs`);
    }

    // Build final records
    const flat = flatten(allItems).filter(x => keep.has(x.qid));
    const cleaned = flat.map(x => ({
      qid: x.qid,
      key_mmdd: x.key_mmdd,
      title: x.title,
      summary: x.summary,
      url: x.url,
      category: (categoryMap.get(x.qid) === "visual_or_performance")
        ? "performance"
        : categoryMap.get(x.qid),
      year: x.year,
      event_mmdd: x.event_mmdd,
      times_seen: 0
    }));

    if (FLAG_STDOUT) {
      process.stdout.write(JSON.stringify(cleaned, null, 2) + "\n");
      return;
    }

    const fs = await import("fs");
    fs.mkdirSync(OUT_DIR, { recursive: true });
    const outName = OUT_FILE || `${KEY}.json`;
    const fullPath = `${OUT_DIR.replace(/\\+$|\\(?=\/)/g, "")}/${outName}`;
    fs.writeFileSync(fullPath, JSON.stringify(cleaned, null, 2));
    console.log(`Wrote ${fullPath} with ${cleaned.length} items`);
  } catch (err) {
    console.error("OTD job failed:", err?.message || err);
    process.exit(1);
  }
})();
