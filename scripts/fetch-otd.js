// File: scripts/fetch-otd.js
// Purpose: Fetch Wikipedia "On This Day" for a given date, keep only arts-related items,
//          and write JSON to data/otd/MM-DD.json (or print to stdout if --stdout).
//
// Node 20+ required (uses global fetch).
//
// Usage:
//   node scripts/fetch-otd.js
//   node scripts/fetch-otd.js --date=2025-11-11 --debug --stdout
//
// Env:
//   TARGET_DATE=YYYY-MM-DD  DEBUG=1  STDOUT=1

// ------------------------ Config ------------------------

const WD_API = "https://www.wikidata.org/w/api.php";
const MAX_ENTITY_BATCH = 50; // wbgetentities supports up to 50 ids per request
const USER_AGENT = "StrumOTD/1.0 (+https://github.com/66fishmarket-droid/STRUMOTDSeedGen)";
const DEFAULT_FETCH_TIMEOUT_MS = Number(process.env.WDQS_TIMEOUT_MS || 45000); // just reused name

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

async function fetchJSON(url, init) {
  const signal = AbortSignal.timeout(DEFAULT_FETCH_TIMEOUT_MS);
  const res = await fetch(url, { ...init, signal });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`HTTP ${res.status} ${res.statusText} :: ${body.slice(0, 300)}`);
  }
  return res.json();
}

// ------------------------ Fetch Wikipedia OTD ------------------------

async function fetchOtdAll(mm, dd) {
  const url = `https://en.wikipedia.org/api/rest_v1/feed/onthisday/all/${mm}/${dd}`;
  const res = await fetch(url, {
    headers: { "User-Agent": USER_AGENT, "Accept": "application/json" }
  });
  if (!res.ok) throw new Error(`OTD fetch failed ${res.status} ${res.statusText}`);
  return res.json();
}

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

function extractCandidateQIDs(items) {
  const qids = new Set();
  for (const it of items) {
    const pages = Array.isArray(it.pages) ? it.pages : [];
    for (const p of pages) {
      if (p && typeof p.wikibase_item === "string") qids.add(p.wikibase_item);
    }
  }
  return Array.from(qids);
}

// ------------------------ Wikidata API classifier (no SPARQL) ------------------------

// Curated sets for quick membership checks (non-transitive).
const P31_ALLOWED = new Set([
  // music works/events/orgs
  "Q482994","Q134556","Q7366","Q179415","Q1263612","Q34508","Q182832","Q222634","Q17489659","Q215380","Q2088357","Q16887380","Q18127",
  // film/tv
  "Q11424","Q5398426","Q21191270","Q24862","Q226730","Q41298",
  // books/writing/lit works
  "Q7725634","Q571","Q8261","Q25379","Q5185279",
  // visual/performance arts
  "Q3305213","Q179700","Q22669","Q207694","Q2431196","Q2743","Q860861"
]);

const P106_ALLOWED = new Set([
  // music occupations
  "Q639669","Q177220","Q36834","Q130857","Q155309","Q161251","Q488111","Q1128996","Q753110","Q158852","Q820232","Q186360","Q14623646",
  // film/tv/theatre
  "Q33999","Q2526255","Q10798782","Q28389","Q2500638","Q48820545","Q36180",
  // literature
  "Q36180","Q482980","Q11774202","Q49757",
  // visual/performance
  "Q1028181","Q33231","Q42973","Q245068","Q256145","Q1281618","Q245341"
]);

function mapCategoryFromP31(qid) {
  if (["Q482994","Q134556","Q7366","Q179415","Q1263612","Q34508","Q182832","Q222634","Q17489659","Q215380","Q2088357","Q16887380","Q18127"].includes(qid)) return "music";
  if (["Q11424","Q5398426","Q21191270","Q24862","Q226730","Q41298"].includes(qid)) return "film_tv";
  if (["Q7725634","Q571","Q8261","Q25379","Q5185279"].includes(qid)) return "books";
  if (["Q3305213","Q179700","Q22669","Q207694","Q2431196","Q2743","Q860861"].includes(qid)) return "performance";
  return null;
}

function mapCategoryFromP106(qid) {
  if (["Q639669","Q177220","Q36834","Q130857","Q155309","Q161251","Q488111","Q1128996","Q753110","Q158852","Q820232","Q186360","Q14623646"].includes(qid)) return "music";
  if (["Q33999","Q2526255","Q10798782","Q28389","Q2500638","Q48820545","Q36180"].includes(qid)) return "film_tv";
  if (["Q36180","Q482980","Q11774202","Q49757"].includes(qid)) return "books";
  if (["Q1028181","Q33231","Q42973","Q245068","Q256145","Q1281618","Q245341"].includes(qid)) return "performance";
  return null;
}

// Hit Wikidata API for up to 50 ids; return map QID -> category or null.
async function classifyViaWikidataAPI(qids) {
  const keep = new Set();
  const categoryMap = new Map();

  for (const batch of chunk(qids, MAX_ENTITY_BATCH)) {
    // wbgetentities supports POST or GET; we use POST form to be safe.
    const body = new URLSearchParams({
      action: "wbgetentities",
      ids: batch.join("|"),
      props: "claims",
      format: "json",
      origin: "*"
    }).toString();

    const data = await fetchJSON(WD_API, {
      method: "POST",
      headers: {
        "User-Agent": USER_AGENT,
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "application/json"
      },
      body
    });

    const entities = data?.entities || {};
    for (const qid of batch) {
      const ent = entities[qid];
      if (!ent || ent.missing === "") continue;

      const claims = ent.claims || {};
      const p31 = (claims.P31 || []).map(s => safeGet(s, "mainsnak.datavalue.value.id")).filter(Boolean);
      const p106 = (claims.P106 || []).map(s => safeGet(s, "mainsnak.datavalue.value.id")).filter(Boolean);

      let category = null;

      // Prefer works/orgs (P31) first, then occupations (P106)
      for (const t of p31) {
        if (!P31_ALLOWED.has(t)) continue;
        category = mapCategoryFromP31(t);
        if (category) break;
      }
      if (!category) {
        for (const o of p106) {
          if (!P106_ALLOWED.has(o)) continue;
          category = mapCategoryFromP106(o);
          if (category) break;
        }
      }

      if (category) {
        keep.add(qid);
        categoryMap.set(qid, category);
      }
    }

    // be polite
    await sleep(300);
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

    // 3) Classify via Wikidata API (no SPARQL)
    let kept = new Set();
    let categoryMap = new Map();

    if (candidateQIDs.length > 0) {
      const { keep, categoryMap: cMap } = await classifyViaWikidataAPI(candidateQIDs);
      kept = keep;
      categoryMap = cMap;
      console.log(`Wikidata API kept ${kept.size} QIDs`);
    } else {
      console.log("Classifier kept 0 QIDs");
    }

    // 4) Build preliminary list from entity classifications
    const flat = [];
    for (const it of allItems) {
      const pages = Array.isArray(it.pages) ? it.pages : [];
      const page = pages.find(p => p && typeof p.wikibase_item === "string" && kept.has(p.wikibase_item));
      if (!page) continue;

      const qid = page.wikibase_item;
      const category = categoryMap.get(qid) || null;

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

    // 5) If nothing via entities, try keyword fallback
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

    // 6) Sort for consistency: by category then title
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
