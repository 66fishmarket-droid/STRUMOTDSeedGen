// fetch-otd.js
// Strum OTD GitHub automation with --out and optional --file/--stdout.
// Usage examples:
//   node fetch-otd.js
//   node fetch-otd.js --date=2025-11-11 --out=outputs/otd
//   node fetch-otd.js --out=tmp --file=custom.json
//   node fetch-otd.js --stdout --kinds=events,births --debug-sparql

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
const FLAG_STDOUT = process.argv.includes("--stdout");
const ARG_DATE = arg("date", process.env.TARGET_DATE);
const KINDS = (arg("kinds", "events,births,deaths,selected") || "")
  .split(",").map(s => s.trim()).filter(Boolean);
const MAX_BATCH = Math.max(1, parseInt(arg("max-batch", "40"), 10) || 40);

// New: output controls
const OUT_DIR = arg("out", "data/otd");              // directory
const OUT_FILE = arg("file", null);                  // file name override (optional)

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
function chunk(arr, n) { const out = []; for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n)); return out; }

// Fetch helpers
async function runSparql(query, headers) {
  // 1) Try GET
  const url = `${WD_SPARQL}?query=${encodeURIComponent(query)}`;
  const getRes = await fetch(url, { method: "GET", headers });
  const getCT = (getRes.headers.get("content-type") || "").toLowerCase();
  const getText = await getRes.text();

  if (getRes.ok && getCT.includes("application/sparql-results+json")) {
    return JSON.parse(getText);
  }

  // If parser complains or server rejects GET, try POST (form-encoded)
  if (!getRes.ok && (getRes.status === 400 || getRes.status === 415)) {
    const postRes = await fetch(WD_SPARQL, {
      method: "POST",
      headers: {
        ...headers,
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
      },
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

// Wikipedia OTD
async function onThisDay(kind) {
  const url = `${WIKI}/feed/onthisday/${kind}/${MM}/${DD}`;
  const j = await fetchJsonWithRetry(url, {
    headers: { "User-Agent": "StrumOTD/1.0 (GitHub Actions)" }
  });
  return Array.isArray(j?.[kind]) ? j[kind] : [];
}

// Collect QIDs
function collectQids(items) {
  const qids = [];
  for (const it of items) for (const p of (it?.pages || [])) if (p.wikibase_item) qids.push(p.wikibase_item);
  return uniq(qids);
}

// SPARQL classification (GET + COALESCE)
async function filterArts(qids) {
  if (!qids || qids.length === 0) return { keep: new Set(), categoryMap: new Map() };

  const headers = {
    "User-Agent": "StrumOTD/1.0 (+https://github.com/66fishmarket-droid/STRUMOTDSeedGen)",
    "Accept": "application/sparql-results+json"
  };

  const keep = new Set();
  const categoryMap = new Map();

  // Conservative default helps avoid 400s; can still be overridden via --max-batch
  const maxBatch = Math.min(MAX_BATCH, 35);

  for (const batch of chunk(qids, maxBatch)) {
    const VALUES = batch.map(q => `wd:${q}`).join(" ");

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
        wd:Q482994, wd:Q134556, wd:Q7366, wd:Q105420, wd:Q753110, wd:Q182832, wd:Q222634
      ))
    },"music", ""),

    # Works: Film / TV
    IF(EXISTS {
      ?item wdt:P31/wdt:P279* ?c2 .
      FILTER(?c2 IN (
        wd:Q11424, wd:Q5398426, wd:Q21191270, wd:Q24862, wd:Q226730
      ))
    },"film_tv", ""),

    # Works: Books
    IF(EXISTS {
      ?item wdt:P31/wdt:P279* ?c3 .
      FILTER(?c3 IN (
        wd:Q7725634, wd:Q571, wd:Q8261, wd:Q25379
      ))
    },"books", ""),

    # Works: Visual / Performance
    IF(EXISTS {
      ?item wdt:P31/wdt:P279* ?c4 .
      FILTER(?c4 IN (
        wd:Q3305213, wd:Q860861, wd:Q2798201, wd:Q2431196, wd:Q2743, wd:Q25379
      ))
    },"visual_or_performance", ""),

    # Works: Awards
    IF(EXISTS {
      ?item wdt:P31/wdt:P279* ?c5 .
      FILTER(?c5 IN (wd:Q618779, wd:Q132241))
    },"awards", ""),

    # Humans: Music
    IF(EXISTS {
      ?item wdt:P31 wd:Q5 ; wdt:P106/wdt:P279* ?o1 .
      FILTER(?o1 IN (
        wd:Q639669, wd:Q177220, wd:Q488205, wd:Q36834, wd:Q1128996, wd:Q130857, wd:Q155309,
        wd:Q161251, wd:Q973127, wd:Q488111, wd:Q158852, wd:Q14623646
      ))
    },"music", ""),

    # Humans: Film / TV
    IF(EXISTS {
      ?item wdt:P31 wd:Q5 ; wdt:P106/wdt:P279* ?o2 .
      FILTER(?o2 IN (
        wd:Q33999, wd:Q2526255, wd:Q28389, wd:Q3455803, wd:Q48820545, wd:Q10800557
      ))
    },"film_tv", ""),

    # Humans: Books
    IF(EXISTS {
      ?item wdt:P31 wd:Q5 ; wdt:P106/wdt:P279* ?o3 .
      FILTER(?o3 IN (
        wd:Q36180, wd:Q482980, wd:Q49757, wd:Q214917, wd:Q6625963, wd:Q11774202
      ))
    },"books", ""),

    # Humans: Visual / Performance
    IF(EXISTS {
      ?item wdt:P31 wd:Q5 ; wdt:P106/wdt:P279* ?o4 .
      FILTER(?o4 IN (
        wd:Q33231, wd:Q42973, wd:Q571668, wd:Q245068, wd:Q256145, wd:Q1028181, wd:Q1281618
      ))
    },"visual_or_performance", "")

  ) AS ?category)

  FILTER( STRLEN(?category) > 0 )
}
`.trim();

    // If the encoded query would be huge, split once more to be safe
    const testUrl = `${WD_SPARQL}?query=${encodeURIComponent(query)}`;
    if (testUrl.length > 7000 && batch.length > 10) {
      // Split batch into halves and process recursively
      const halves = chunk(batch, Math.ceil(batch.length / 2));
      const rec = await Promise.all(halves.map(h => filterArts(h)));
      // Merge results
      for (const r of rec) {
        r.keep.forEach(q => keep.add(q));
        for (const [k, v] of r.categoryMap.entries()) categoryMap.set(k, v);
      }
      continue;
    }

    // Execute query (GET, fallback to POST form on 400/415)
    const j = await runSparql(query, headers);

    for (const b of (j?.results?.bindings || [])) {
      const qid = b.item.value.split("/").pop();
      keep.add(qid);
      categoryMap.set(qid, b.category.value);
    }

    await new Promise(r => setTimeout(r, 250)); // gentle pacing
  }

  return { keep, categoryMap };
}


// Flatten
function flatten(items) {
  const out = new Map();
  for (const it of items) {
    const yearFromField = it?.year;
    const yearFromText = it?.text?.match(/\b(\d{3,4})\b/)?.[1];
    const year = Number(yearFromField || yearFromText) || null;
    const page = (it.pages || []).find(p => p?.wikibase_item);
    if (!page) continue;
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
      category: (categoryMap.get(x.qid) === "visual_or_performance") ? "performance" : categoryMap.get(x.qid),
      year: x.year,
      event_mmdd: x.event_mmdd,
      times_seen: 0
    }));

    if (FLAG_STDOUT) {
      // Print JSON to stdout (no files written)
      process.stdout.write(JSON.stringify(cleaned, null, 2) + "\n");
      return;
    }

    const fs = await import("fs");
    fs.mkdirSync(OUT_DIR, { recursive: true });
    const outName = OUT_FILE || `${KEY}.json`;
    const fullPath = `${OUT_DIR.replace(/\\+$|\\(?=\/)/g,"")}/${outName}`;
    fs.writeFileSync(fullPath, JSON.stringify(cleaned, null, 2));
    console.log(`Wrote ${fullPath} with ${cleaned.length} items`);
  } catch (err) {
    console.error("OTD job failed:", err?.message || err);
    process.exit(1);
  }
})();
