// fetch-otd.js
// Strum OTD GitHub automation: pull Wikipedia On This Day, classify via Wikidata, write JSON.
// Usage:
//   node fetch-otd.js
//   node fetch-otd.js --date=2025-11-11 --kinds=events,births,deaths,selected --debug-sparql --max-batch=35
//
// Env:
//   TARGET_DATE=YYYY-MM-DD
//   WD_SPARQL (optional override of the WDQS endpoint)

import fetch from "node-fetch";

// Endpoints
const WIKI = "https://en.wikipedia.org/api/rest_v1";
const WD_SPARQL = process.env.WD_SPARQL || "https://query.wikidata.org/sparql";

// CLI flags
const arg = (name, dflt) => {
  const raw = process.argv.find(a => a.startsWith(`--${name}=`));
  return raw ? raw.split("=").slice(1).join("=") : dflt;
};
const FLAG_DEBUG = process.argv.includes("--debug-sparql");
const ARG_DATE = arg("date", process.env.TARGET_DATE);
const KINDS = (arg("kinds", "events,births,deaths,selected") || "")
  .split(",").map(s => s.trim()).filter(Boolean);
const MAX_BATCH = Math.max(1, parseInt(arg("max-batch", "40"), 10) || 40);

// Date selection (UTC)
const base = ARG_DATE ? new Date(ARG_DATE + "T00:00:00Z") : new Date();
const MM = String(base.getUTCMonth() + 1).padStart(2, "0");
const DD = String(base.getUTCDate()).padStart(2, "0");
const KEY = `${MM}-${DD}`;
const KEY_SLASH = `${MM}/${DD}`;

// Helpers
const sleep = ms => new Promise(res => setTimeout(res, ms));
const uniq = arr => [...new Set(arr)];
const safe = (val, d = "") => (val ?? d);
function chunk(arr, n) {
  const out = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

// Generic fetch with simple retry for WDQS and REST
async function fetchTextWithRetry(url, opts = {}, retries = 3) {
  let lastErr;
  for (let i = 1; i <= retries; i++) {
    const r = await fetch(url, opts);
    const ct = (r.headers.get("content-type") || "").toLowerCase();
    const text = await r.text();

    if (!r.ok) {
      // backoff on 429/5xx
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
  try {
    return JSON.parse(text);
  } catch (e) {
    throw new Error(`JSON parse error ct=${ct} :: ${text.slice(0, 200)}`);
  }
}

// Wikipedia OTD
async function onThisDay(kind) {
  const url = `${WIKI}/feed/onthisday/${kind}/${MM}/${DD}`;
  const j = await fetchJsonWithRetry(url, {
    headers: { "User-Agent": "StrumOTD/1.0 (GitHub Actions)" }
  });
  return Array.isArray(j?.[kind]) ? j[kind] : [];
}

// Collect wikibase QIDs from the page bundles
function collectQids(items) {
  const qids = [];
  for (const it of items) {
    const pages = it?.pages || [];
    for (const p of pages) {
      if (p.wikibase_item) qids.push(p.wikibase_item);
    }
  }
  return uniq(qids);
}

// SPARQL classification
async function filterArts(qids) {
  if (!qids || qids.length === 0) return { keep: new Set(), categoryMap: new Map() };

  const headers = {
    "User-Agent": "StrumOTD/1.0 (+https://github.com/66fishmarket-droid/STRUMOTDSeedGen)",
    "Accept": "application/sparql-results+json"
  };

  const keep = new Set();
  const categoryMap = new Map();

  for (const batch of chunk(qids, MAX_BATCH)) {
    const VALUES = batch.map(q => `wd:${q}`).join(" ");

    // Classes and occupations are grouped by bucket; COALESCE returns the first match.
    const query = `
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>

SELECT ?item ?category WHERE {
  VALUES ?item { ${VALUES} }

  BIND(COALESCE(

    # Works: Music
    IF(EXISTS {
      ?item wdt:P31/wdt:P279* ?c1 .
      FILTER(?c1 IN (
        wd:Q482994,   # album
        wd:Q134556,   # single
        wd:Q7366,     # song
        wd:Q105420,   # EP
        wd:Q753110,   # musical work
        wd:Q182832,   # concert tour
        wd:Q222634    # music award
      ))
    },"music", UNDEF),

    # Works: Film / TV
    IF(EXISTS {
      ?item wdt:P31/wdt:P279* ?c2 .
      FILTER(?c2 IN (
        wd:Q11424,     # film
        wd:Q5398426,   # TV series
        wd:Q21191270,  # TV episode
        wd:Q24862,     # short film
        wd:Q226730     # film festival
      ))
    },"film_tv", UNDEF),

    # Works: Books / Literature
    IF(EXISTS {
      ?item wdt:P31/wdt:P279* ?c3 .
      FILTER(?c3 IN (
        wd:Q7725634,   # literary work
        wd:Q571,       # book
        wd:Q8261,      # novel
        wd:Q25379      # play (as written work)
      ))
    },"books", UNDEF),

    # Works: Visual / Performance arts
    IF(EXISTS {
      ?item wdt:P31/wdt:P279* ?c4 .
      FILTER(?c4 IN (
        wd:Q3305213,   # painting
        wd:Q860861,    # sculpture
        wd:Q2798201,   # ballet
        wd:Q2431196,   # opera
        wd:Q2743,      # theater production
        wd:Q25379      # play (as performance)
      ))
    },"visual_or_performance", UNDEF),

    # Works: Awards (general)
    IF(EXISTS {
      ?item wdt:P31/wdt:P279* ?c5 .
      FILTER(?c5 IN (
        wd:Q618779,    # award
        wd:Q132241     # award ceremony
      ))
    },"awards", UNDEF),

    # Humans: Music people
    IF(EXISTS {
      ?item wdt:P31 wd:Q5 ;
            wdt:P106/wdt:P279* ?o1 .
      FILTER(?o1 IN (
        wd:Q639669,   # musician
        wd:Q177220,   # singer
        wd:Q488205,   # singer-songwriter
        wd:Q36834,    # composer
        wd:Q1128996,  # rapper
        wd:Q130857,   # DJ
        wd:Q155309,   # record producer
        wd:Q161251,   # guitarist
        wd:Q973127,   # bassist
        wd:Q488111,   # drummer
        wd:Q158852,   # violinist
        wd:Q14623646  # conductor
      ))
    },"music", UNDEF),

    # Humans: Film / TV people
    IF(EXISTS {
      ?item wdt:P31 wd:Q5 ;
            wdt:P106/wdt:P279* ?o2 .
      FILTER(?o2 IN (
        wd:Q33999,    # actor
        wd:Q2526255,  # film director
        wd:Q28389,    # screenwriter
        wd:Q3455803,  # cinematographer
        wd:Q48820545, # showrunner
        wd:Q10800557  # TV presenter
      ))
    },"film_tv", UNDEF),

    # Humans: Books / Literature people
    IF(EXISTS {
      ?item wdt:P31 wd:Q5 ;
            wdt:P106/wdt:P279* ?o3 .
      FILTER(?o3 IN (
        wd:Q36180,    # writer
        wd:Q482980,   # author
        wd:Q49757,    # poet
        wd:Q214917,   # playwright
        wd:Q6625963,  # novelist
        wd:Q11774202  # essayist
      ))
    },"books", UNDEF),

    # Humans: Visual / Performance people
    IF(EXISTS {
      ?item wdt:P31 wd:Q5 ;
            wdt:P106/wdt:P279* ?o4 .
      FILTER(?o4 IN (
        wd:Q33231,    # painter
        wd:Q42973,    # performance artist
        wd:Q571668,   # dancer
        wd:Q245068,   # comedian
        wd:Q256145,   # entertainer
        wd:Q1028181,  # stage actor
        wd:Q1281618   # theatre director
      ))
    },"visual_or_performance", UNDEF)

  ) AS ?category)

  FILTER(BOUND(?category))
}
`.trim();

    const url = `${WD_SPARQL}?query=${encodeURIComponent(query)}`;
    if (FLAG_DEBUG) {
      console.error("\n--- SPARQL BATCH ---");
      console.error(query);
    }

    // GET + Accept header; no Content-Type; small pacing helps avoid 429s
    const { ct, text } = await fetchTextWithRetry(url, {
      method: "GET",
      headers
    }, 3);

    if (!ct.includes("application/sparql-results+json")) {
      throw new Error(`Unexpected content-type: ${ct} :: ${text.slice(0, 200)}`);
    }

    let j;
    try { j = JSON.parse(text); }
    catch {
      throw new Error(`SPARQL parse error ct=${ct} :: ${text.slice(0,200)}`);
    }

    const rows = j?.results?.bindings || [];
    for (const b of rows) {
      const qid = b.item.value.split("/").pop();
      keep.add(qid);
      categoryMap.set(qid, b.category.value);
    }

    if (!FLAG_DEBUG) await sleep(250); // be nice to WDQS
  }

  return { keep, categoryMap };
}

// Flatten the OTD bundles to a unique map by QID
function flatten(items) {
  const out = new Map();
  for (const it of items) {
    // Pick a year: prefer numeric field, fallback to first 3-4 digit in text
    const yearFromField = it?.year;
    const yearFromText = it?.text?.match(/\b(\d{3,4})\b/)?.[1];
    const year = Number(yearFromField || yearFromText) || null;

    // Choose a page that actually has a QID
    const page = (it.pages || []).find(p => p?.wikibase_item);
    if (!page) continue;

    const qid = page.wikibase_item;
    const url = page?.content_urls?.desktop?.page
      || `https://en.wikipedia.org/wiki/${encodeURIComponent(page.title)}`;

    // Favor normalized title, fall back to raw title
    const title = safe(page?.titles?.normalized, page?.title);

    // First hit wins for each QID
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
    // Pull only requested kinds
    const pulls = await Promise.all(KINDS.map(k => onThisDay(k)));
    const allItems = pulls.flat();

    const qids = collectQids(allItems);
    if (FLAG_DEBUG) console.error(`Collected ${qids.length} unique QIDs`);

    const { keep, categoryMap } = await filterArts(qids);

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

    const fs = await import("fs");
    fs.mkdirSync("data/otd", { recursive: true });
    fs.writeFileSync(`data/otd/${KEY}.json`, JSON.stringify(cleaned, null, 2));
    console.log(`Wrote data/otd/${KEY}.json with ${cleaned.length} items`);
  } catch (err) {
    console.error("OTD job failed:", err?.message || err);
    process.exit(1);
  }
})();
