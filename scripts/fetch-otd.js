// File: scripts/fetch-otd.js
// Purpose: Fetch Wikipedia "On This Day" for a given date, keep only arts-related items,
//          and write JSON to data/otd/MM-DD.json (or print to stdout if --stdout).
//
// Node 20+ required (uses global fetch).
//
// Usage examples:
//   node scripts/fetch-otd.js
//   node scripts/fetch-otd.js --date=2025-11-11 --debug --stdout
//
// Also respects env vars:
//   TARGET_DATE=YYYY-MM-DD  DEBUG=1  STDOUT=1

// ------------------------ Config ------------------------

const WD_SPARQL = "https://query.wikidata.org/sparql";
const MAX_BATCH = 25; // safer for WDQS; will auto-split further on 400/413/414/431
const USER_AGENT = "StrumOTD/1.0 (+https://github.com/66fishmarket-droid/STRUMOTDSeedGen)";

// ------------------------ CLI / ENV ------------------------

const argv = process.argv.slice(2);
const argMap = Object.fromEntries(
  argv
    .filter(a => a.startsWith("--"))
    .map(a => {
      const [k, v] = a.replace(/^--/, "").split("=");
      return [k, v === undefined ? true : v];
    })
);

const DEBUG = !!(process.env.DEBUG || argMap.debug);
const TO_STDOUT = !!(process.env.STDOUT || argMap.stdout);

// Resolve date: --date=YYYY-MM-DD | TARGET_DATE | today (UTC)
const targetISO =
  (typeof argMap.date === "string" && argMap.date) ||
  process.env.TARGET_DATE ||
  new Date().toISOString().slice(0, 10);

const target = new Date(targetISO + "T00:00:00Z");
if (isNaN(target.getTime())) {
  console.error(`[fetch-otd] Invalid date: ${targetISO}`);
  process.exit(1);
}

const MM = String(target.getUTCMonth() + 1).padStart(2, "0");
const DD = String(target.getUTCDate()).padStart(2, "0");
const KEY_SLASH = `${MM}-${DD}`;

// ------------------------ Utils ------------------------

function sleep(ms) {
  return new Promise(res => setTimeout(res, ms));
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function safeGet(obj, path, dflt = undefined) {
  try {
    return path.split(".").reduce((acc, k) => (acc == null ? undefined : acc[k]), obj) ?? dflt;
  } catch {
    return dflt;
  }
}

function logDebug(...args) {
  if (DEBUG) console.log(...args);
}

// ------------------------ Fetch Wikipedia OTD ------------------------

async function fetchOtdAll(mm, dd) {
  const url = `https://en.wikipedia.org/api/rest_v1/feed/onthisday/all/${mm}/${dd}`;
  const res = await fetch(url, {
    headers: {
      "User-Agent": USER_AGENT,
      "Accept": "application/json"
    }
  });
  if (!res.ok) {
    throw new Error(`OTD fetch failed ${res.status} ${res.statusText}`);
  }
  return res.json();
}

// Collect items from the REST payload (events, births, deaths, selected, holidays)
function collectAllItems(payload) {
  const buckets = ["events", "births", "deaths", "selected", "holidays"];
  const items = [];
  for (const b of buckets) {
    const arr = payload[b];
    if (!Array.isArray(arr)) continue;
    for (const it of arr) items.push(it);
  }
  return items;
}

// Extract unique QIDs from the items' pages
function extractCandidateQIDs(items) {
  const qids = new Set();
  for (const it of items) {
    const pages = Array.isArray(it.pages) ? it.pages : [];
    for (const p of pages) {
      if (p && typeof p.wikibase_item === "string") {
        qids.add(p.wikibase_item);
      }
    }
  }
  return Array.from(qids);
}

// ------------------------ SPARQL (Arts filter) ------------------------

// Robust WDQS runner: POST the SPARQL in the body (prevents 431 due to long URLs).
// Retries on 429/503 with exponential backoff, honors Retry-After when present.
// Falls back to form-POST if application/sparql-query is rejected by some proxy.
async function runSparql(query, headers, attempt = 1) {
  const maxAttempts = 5;

  const backoffFor = (res, attempt) => {
    const hdr = res.headers?.get?.("retry-after");
    if (hdr) {
      const secs = Number(hdr);
      if (!Number.isNaN(secs) && secs > 0) return secs * 1000;
    }
    return Math.min(2000 * attempt + Math.floor(Math.random() * 300), 10000);
  };

  const common = {
    method: "POST",
    headers: {
      ...headers,
      "User-Agent": USER_AGENT,
      "Accept": "application/sparql-results+json",
      "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
      "Cache-Control": "no-cache"
    },
    body: `query=${encodeURIComponent(query)}&format=json`
  };

  const res = await fetch(WD_SPARQL, common);

  if ([429, 502, 503, 504].includes(res.status) && attempt < maxAttempts) {
    const wait = backoffFor(res, attempt);
    logDebug(`[WDQS] ${res.status} ${res.statusText}; retrying in ${wait}ms (attempt ${attempt + 1})`);
    await sleep(wait);
    return runSparql(query, headers, attempt + 1);
  }

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    const err = new Error(`WDQS failed ${res.status} ${res.statusText} :: ${text.slice(0, 300)}`);
    err.status = res.status;
    err.detail = text;
    throw err;
  }

  return res.json();
}


// BROAD arts classifier: works/events + human occupations (music, film/TV/theatre, books, visual/performance)
async function filterArts(qids) {
  if (!qids || qids.length === 0) return { keep: new Set(), categoryMap: new Map() };

  const headers = {
    "User-Agent": USER_AGENT,
    "Accept": "application/sparql-results+json"
  };

  const keep = new Set();
  const categoryMap = new Map();
  const maxBatch = Math.min(MAX_BATCH, 35);

  for (const batch of chunk(qids, maxBatch)) {
  const VALUES = batch.map(q => `wd:${q}`).join(" ");

  const query = `
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>

SELECT ?item ?category WHERE {
  VALUES ?item { ${VALUES} }

  BIND(COALESCE(
    /* works/events: music */
    IF(EXISTS {
      ?item wdt:P31/wdt:P279* ?c1 .
      FILTER(?c1 IN (wd:Q482994, wd:Q134556, wd:Q7366, wd:Q179415, wd:Q1263612, wd:Q34508, wd:Q182832, wd:Q222634, wd:Q17489659))
    },"music", UNDEF),
    /* film/tv */
    IF(EXISTS {
      ?item wdt:P31/wdt:P279* ?c2 .
      FILTER(?c2 IN (wd:Q11424, wd:Q5398426, wd:Q21191270, wd:Q24862, wd:Q226730, wd:Q41298))
    },"film_tv", UNDEF),
    /* books */
    IF(EXISTS {
      ?item wdt:P31/wdt:P279* ?c3 .
      FILTER(?c3 IN (wd:Q7725634, wd:Q571, wd:Q8261, wd:Q25379, wd:Q5185279))
    },"books", UNDEF),
    /* visual/performance works */
    IF(EXISTS {
      ?item wdt:P31/wdt:P279* ?c4 .
      FILTER(?c4 IN (wd:Q3305213, wd:Q179700, wd:Q22669, wd:Q207694, wd:Q2431196, wd:Q2743, wd:Q860861))
    },"visual_or_performance", UNDEF),
    /* orgs tied to arts */
    IF(EXISTS {
      ?item wdt:P31/wdt:P279* ?c5 .
      FILTER(?c5 IN (wd:Q215380, wd:Q2088357, wd:Q16887380, wd:Q18127))
    },"music", UNDEF),
    /* human occupations: music */
    IF(EXISTS {
      ?item wdt:P31 wd:Q5 ; wdt:P106/wdt:P279* ?o1 .
      FILTER(?o1 IN (wd:Q639669, wd:Q177220, wd:Q36834, wd:Q130857, wd:Q155309, wd:Q161251, wd:Q488111, wd:Q1128996, wd:Q753110, wd:Q158852, wd:Q820232, wd:Q186360, wd:Q14623646))
    },"music", UNDEF),
    /* film/tv/theatre occupations */
    IF(EXISTS {
      ?item wdt:P31 wd:Q5 ; wdt:P106/wdt:P279* ?o2 .
      FILTER(?o2 IN (wd:Q33999, wd:Q2526255, wd:Q10798782, wd:Q28389, wd:Q2500638, wd:Q48820545, wd:Q36180))
    },"film_tv", UNDEF),
    /* literature occupations */
    IF(EXISTS {
      ?item wdt:P31 wd:Q5 ; wdt:P106/wdt:P279* ?o3 .
      FILTER(?o3 IN (wd:Q36180, wd:Q482980, wd:Q11774202, wd:Q49757))
    },"books", UNDEF),
    /* visual/performance occupations */
    IF(EXISTS {
      ?item wdt:P31 wd:Q5 ; wdt:P106/wdt:P279* ?o4 .
      FILTER(?o4 IN (wd:Q1028181, wd:Q33231, wd:Q42973, wd:Q245068, wd:Q256145, wd:Q1281618, wd:Q245341))
    },"visual_or_performance", UNDEF)
  ) AS ?category)

  FILTER(BOUND(?category))
}
`.trim();

  let j;
  try {
    j = await runSparql(query, headers);
  } catch (e) {
    // If the endpoint complains about request size or bad request on this VALUES set, split and recurse.
    if ([400, 413, 414, 431].includes(e.status || 0) && batch.length > 1) {
      logDebug(`[WDQS] ${e.status} on batch of ${batch.length}; splitting and retrying...`);
      const halves = chunk(batch, Math.ceil(batch.length / 2));
      const parts = await Promise.all(halves.map(h => filterArts(h)));
      for (const r of parts) {
        r.keep.forEach(q => keep.add(q));
        for (const [k, v] of r.categoryMap.entries()) categoryMap.set(k, v);
      }
      await sleep(250);
      continue; // proceed to next outer batch
    }
    throw e; // rethrow unexpected errors
  }

  for (const b of (j?.results?.bindings || [])) {
    const qid = b.item.value.split("/").pop();
    keep.add(qid);
    categoryMap.set(qid, b.category.value);
  }

  await sleep(250);
}


  return { keep, categoryMap };
}

// ------------------------ Keyword Heuristic Fallback ------------------------

function looksArtsByText(pages = []) {
  const hay = (pages
    .map(p => `${p?.title || ""} ${p?.displaytitle || ""} ${p?.description || ""} ${p?.extract || ""}`)
    .join(" ") || "").toLowerCase();

  const music = /(album|single|song|ep|mixtape|music video|band|musician|singer|rapper|guitarist|drummer|bassist|pianist|composer|conductor|orchestra|ensemble|label)\b/;
  const film  = /\b(film|movie|television|tv series|episode|director|screenwriter|cinematograph|actor|actress|producer|festival|award)\b/;
  const books = /\b(book|novel|poem|poetry|play|playwright|author|writer|literary|publication)\b/;
  const perf  = /\b(painter|sculptor|photograph|photographer|ballet|dance|choreograph|performance art|exhibition|museum|gallery|artist|comedian)\b/;

  if (music.test(hay)) return "music";
  if (film.test(hay)) return "film_tv";
  if (books.test(hay)) return "books";
  if (perf.test(hay)) return "performance";
  return null;
}

// ------------------------ Main ------------------------

async function main() {
  try {
    // 1) Fetch OTD REST
    const payload = await fetchOtdAll(MM, DD);

    // 2) Gather all items and candidate QIDs
    const allItems = collectAllItems(payload);
    const candidateQIDs = extractCandidateQIDs(allItems);

    console.log(`Collected ${candidateQIDs.length} candidate QIDs for ${KEY_SLASH}`);

    if (candidateQIDs.length === 0) {
      logDebug("[warn] No QIDs in REST payload. Will attempt keyword fallback later.");
    }

    // 3) WDQS filter for arts
    let kept = new Set();
    let categoryMap = new Map();

    if (candidateQIDs.length > 0) {
      const { keep, categoryMap: cMap } = await filterArts(candidateQIDs);
      kept = keep;
      categoryMap = cMap;
      console.log(`SPARQL kept ${kept.size} QIDs`);
    } else {
      console.log("SPARQL kept 0 QIDs");
    }

    // 4) Build preliminary list from SPARQL results
    const flat = [];
    for (const it of allItems) {
      const pages = Array.isArray(it.pages) ? it.pages : [];
      const page = pages.find(p => p && typeof p.wikibase_item === "string" && kept.has(p.wikibase_item));
      if (!page) continue;

      const qid = page.wikibase_item;
      const categoryRaw = categoryMap.get(qid) || null;
      const category =
        categoryRaw === "visual_or_performance"
          ? "performance"
          : categoryRaw;

      const title =
        safeGet(page, "titles.normalized") ||
        page.title ||
        "Untitled";

      const url =
        safeGet(page, "content_urls.desktop.page") ||
        (page.title ? `https://en.wikipedia.org/wiki/${encodeURIComponent(page.title)}` : null);

      const summary = typeof it.text === "string" ? it.text : "";

      const year =
        typeof it.year === "number"
          ? it.year
          : (summary.match(/\b(\d{3,4})\b/)?.[1] ? Number(summary.match(/\b(\d{3,4})\b/)[1]) : null);

      flat.push({
        qid,
        key_mmdd: KEY_SLASH,
        title,
        summary,
        url,
        category,
        year,
        event_mmdd: KEY_SLASH,
        times_seen: 0
      });
    }

    // 5) Normalize & (if empty) use keyword fallback
    const cleaned = flat.map(x => ({
      qid: x.qid,
      key_mmdd: x.key_mmdd,
      title: x.title,
      summary: x.summary,
      url: x.url,
      category: x.category,
      year: x.year,
      event_mmdd: x.event_mmdd,
      times_seen: 0
    }));

    if (cleaned.length === 0) {
      const heur = [];
      for (const it of allItems) {
        const pages = Array.isArray(it.pages) ? it.pages : [];
        const pageWithQid = pages.find(p => p?.wikibase_item);
        if (!pageWithQid) continue;

        const cat = looksArtsByText(pages);
        if (!cat) continue;

        const title =
          safeGet(pageWithQid, "titles.normalized") ||
          pageWithQid.title ||
          "Untitled";

        const url =
          safeGet(pageWithQid, "content_urls.desktop.page") ||
          (pageWithQid.title ? `https://en.wikipedia.org/wiki/${encodeURIComponent(pageWithQid.title)}` : null);

        const summary = typeof it.text === "string" ? it.text : "";

        const year =
          typeof it.year === "number"
            ? it.year
            : (summary.match(/\b(\d{3,4})\b/)?.[1] ? Number(summary.match(/\b(\d{3,4})\b/)[1]) : null);

        heur.push({
          qid: pageWithQid.wikibase_item,
          key_mmdd: KEY_SLASH,
          title,
          summary,
          url,
          category: cat,
          year,
          event_mmdd: KEY_SLASH,
          times_seen: 0
        });
      }
      // Dedup on QID, keep first
      const seen = new Set();
      const dedup = [];
      for (const x of heur) {
        if (seen.has(x.qid)) continue;
        seen.add(x.qid);
        dedup.push(x);
      }
      if (dedup.length > 0) {
        console.log(`[fallback] Using ${dedup.length} keyword-matched arts items.`);
        cleaned.push(...dedup);
      } else {
        console.log("[fallback] No keyword matches either.");
      }
    }

    // 6) Sort for consistency (optional): by category then title
    cleaned.sort((a, b) => {
      const ca = a.category || "";
      const cb = b.category || "";
      if (ca !== cb) return ca.localeCompare(cb);
      return (a.title || "").localeCompare(b.title || "");
    });

    // 7) Output
    if (TO_STDOUT) {
      console.log(JSON.stringify(cleaned, null, 2));
      return;
    }

    const fs = await import("node:fs/promises");
    const path = await import("node:path");
    const outDir = path.resolve(process.cwd(), "data", "otd");
    const outFile = path.join(outDir, `${KEY_SLASH}.json`);

    await fs.mkdir(outDir, { recursive: true });
    await fs.writeFile(outFile, JSON.stringify(cleaned, null, 2) + "\n", "utf8");

    console.log(`Wrote ${outFile} with ${cleaned.length} items`);
  } catch (err) {
    console.error(`[fetch-otd] ERROR: ${err.message || err}`);
    if (DEBUG) console.error(err.stack);
    process.exit(1);
  }
}

main();
