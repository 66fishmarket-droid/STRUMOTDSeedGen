// File: scripts/fetch-otd.js
// Purpose: Build a "births & deaths (arts-only)" factoid list for a given date.
//          Output file name: data/otd/BDMM-DD.json
//
// Node 20+ required (uses global fetch).
//
// Usage examples:
//   node scripts/fetch-otd.js
//   node scripts/fetch-otd.js --date=2025-12-13 --debug --stdout
//
// Env vars (optional):
//   TARGET_DATE=YYYY-MM-DD  DEBUG=1  STDOUT=1
//
// Notes:
// - Only births/deaths buckets are considered.
// - Classification: humans via P106 (occupations); non-humans via P31 (type).
// - No SPARQL is used; we call Wikidata wbgetentities in batches.

const WD_API = "https://www.wikidata.org/w/api.php";
const USER_AGENT = "StrumOTD/1.0 (+https://github.com/66fishmarket-droid/STRUMOTDSeedGen)";
const MAX_WB_BATCH = 45; // safe batch size for wbgetentities
const DEBUG_DEFAULT = false;

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

const DEBUG = !!(process.env.DEBUG || argMap.debug || DEBUG_DEFAULT);
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

// ------------------------ Wikipedia OTD ------------------------

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

// Collect items and tag with bucket for filtering later
function collectAllItems(payload) {
  const buckets = ["events", "births", "deaths", "selected", "holidays"];
  const items = [];
  for (const b of buckets) {
    const arr = payload[b];
    if (!Array.isArray(arr)) continue;
    for (const it of arr) items.push({ ...it, __bucket: b });
  }
  return items;
}

const ALLOWED_BUCKETS = new Set(["births", "deaths"]);

// Extract unique QIDs from the items' pages
function extractCandidateQIDs(items) {
  const qids = new Set();
  for (const it of items) {
    if (!ALLOWED_BUCKETS.has(it.__bucket)) continue;
    const pages = Array.isArray(it.pages) ? it.pages : [];
    for (const p of pages) {
      if (p && typeof p.wikibase_item === "string") {
        qids.add(p.wikibase_item);
      }
    }
  }
  return Array.from(qids);
}

// ------------------------ Wikidata Entities (no SPARQL) ------------------------

// Robust POST to wbgetentities with retries/backoff
async function wbgetentities(ids) {
  const body = new URLSearchParams({
    action: "wbgetentities",
    ids: ids.join("|"),
    props: "claims|sitelinks|labels",
    format: "json",
    origin: "*"
  });

  let attempt = 1;
  while (true) {
    const res = await fetch(WD_API, {
      method: "POST",
      headers: {
        "User-Agent": USER_AGENT,
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "application/json"
      },
      body: body.toString()
    });

    if ([429, 502, 503, 504].includes(res.status) && attempt < 6) {
      const wait = Math.min(2000 * attempt + Math.floor(Math.random() * 300), 10000);
      logDebug(`[WD] ${res.status} ${res.statusText}; retry in ${wait}ms (attempt ${attempt + 1})`);
      await sleep(wait);
      attempt += 1;
      continue;
    }

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new Error(`wbgetentities failed ${res.status} ${res.statusText} :: ${txt.slice(0, 300)}`);
    }

    const json = await res.json();
    return json;
  }
}

async function fetchEntitiesMap(qids) {
  const out = new Map();
  for (const batch of chunk(qids, MAX_WB_BATCH)) {
    const j = await wbgetentities(batch);
    const ents = j && j.entities ? j.entities : {};
    for (const [qid, entity] of Object.entries(ents)) {
      if (entity && !entity.missing) out.set(qid, entity);
    }
    await sleep(200); // be polite
  }
  return out;
}

// Helpers to pull P-values from claims
function getIdsFromClaims(entity, prop) {
  const claims = safeGet(entity, "claims", {});
  const arr = claims[prop] || [];
  const ids = [];
  for (const c of arr) {
    const id = safeGet(c, "mainsnak.datavalue.value.id");
    if (id) ids.push(id);
  }
  return ids;
}

// ------------------------ Category Logic ------------------------

// Non-human works/orgs via P31 (instance of)
const P31_ALLOWED = new Set([
  // Music works/events
  "Q482994","Q134556","Q7366","Q179415","Q1263612","Q34508","Q182832","Q222634","Q17489659",
  // Film/TV/theatre works/events
  "Q11424","Q5398426","Q21191270","Q24862","Q226730","Q41298",
  // Books/writing works
  "Q7725634","Q571","Q8261","Q25379","Q5185279",
  // Visual/performance arts
  "Q3305213","Q179700","Q22669","Q207694","Q2431196","Q2743","Q860861",
  // Music orgs
  "Q215380","Q2088357","Q16887380","Q18127"
]);

function mapCategoryFromP31(qid) {
  if (["Q482994","Q134556","Q7366","Q179415","Q1263612","Q34508","Q182832","Q222634","Q17489659","Q215380","Q2088357","Q16887380","Q18127"].includes(qid)) return "music";
  if (["Q11424","Q5398426","Q21191270","Q24862","Q226730","Q41298"].includes(qid)) return "film_tv";
  if (["Q7725634","Q571","Q8261","Q25379","Q5185279"].includes(qid)) return "books";
  if (["Q3305213","Q179700","Q22669","Q207694","Q2431196","Q2743","Q860861"].includes(qid)) return "performance";
  return null;
}

// Humans via P106 (occupation)
const P106_ALLOWED = new Set([
  // music
  "Q639669","Q177220","Q36834","Q130857","Q155309","Q161251","Q488111","Q1128996","Q753110","Q158852","Q820232","Q186360","Q14623646",
  // film/tv/theatre (no generic "writer" here)
  "Q33999","Q2526255","Q10798782","Q28389","Q2500638","Q48820545",
  // literature
  "Q482980","Q11774202","Q49757","Q36180",
  // visual/performance
  "Q1028181","Q33231","Q42973","Q245068","Q256145","Q1281618","Q245341"
]);

function mapCategoryFromP106(qid) {
  if (["Q639669","Q177220","Q36834","Q130857","Q155309","Q161251","Q488111","Q1128996","Q753110","Q158852","Q820232","Q186360","Q14623646"].includes(qid)) return "music";
  if (["Q33999","Q2526255","Q10798782","Q28389","Q2500638","Q48820545"].includes(qid)) return "film_tv";
  if (["Q482980","Q11774202","Q49757","Q36180"].includes(qid)) return "books";
  if (["Q1028181","Q33231","Q42973","Q245068","Q256145","Q1281618","Q245341"].includes(qid)) return "performance";
  return null;
}

function categorizeEntity(entity) {
  const p31 = getIdsFromClaims(entity, "P31");
  const p106 = getIdsFromClaims(entity, "P106");
  const isHuman = p31.includes("Q5");

  if (isHuman) {
    for (const o of p106) {
      if (!P106_ALLOWED.has(o)) continue;
      const cat = mapCategoryFromP106(o);
      if (cat) return cat;
    }
    return null; // human but not an arts occupation
  } else {
    for (const t of p31) {
      if (!P31_ALLOWED.has(t)) continue;
      const cat = mapCategoryFromP31(t);
      if (cat) return cat;
    }
    return null;
  }
}

// ------------------------ Main ------------------------

async function main() {
  try {
    // 1) Fetch OTD (all buckets)
    const payload = await fetchOtdAll(MM, DD);

    // 2) Gather items and candidate QIDs (births/deaths only)
    const allItems = collectAllItems(payload);
    const candidateQIDs = extractCandidateQIDs(allItems);

    console.log(`Collected ${candidateQIDs.length} candidate QIDs for ${KEY_SLASH}`);

    // 3) Fetch Wikidata entities for candidates
    const entities = await fetchEntitiesMap(candidateQIDs);

    // 4) Build list constrained to births/deaths and arts categories
    const results = [];
    for (const it of allItems) {
      if (!ALLOWED_BUCKETS.has(it.__bucket)) continue;

      const pages = Array.isArray(it.pages) ? it.pages : [];
      // Find first page with a QID we can categorize
      let chosen = null;
      let chosenCat = null;

      for (const p of pages) {
        const qid = p && p.wikibase_item;
        if (!qid) continue;
        const ent = entities.get(qid);
        if (!ent) continue;

        const cat = categorizeEntity(ent);
        if (!cat) continue;

        chosen = p;
        chosenCat = cat;
        break;
      }

      if (!chosen) continue; // skip non-arts or unclassifiable

      const qid = chosen.wikibase_item;
      const title =
        safeGet(chosen, "titles.normalized") ||
        chosen.title ||
        "Untitled";

      const url =
        safeGet(chosen, "content_urls.desktop.page") ||
        (chosen.title ? `https://en.wikipedia.org/wiki/${encodeURIComponent(chosen.title)}` : null);

      const summary = typeof it.text === "string" ? it.text : "";

      const year =
        typeof it.year === "number"
          ? it.year
          : (summary.match(/\b(\d{3,4})\b/)?.[1] ? Number(summary.match(/\b(\d{3,4})\b/)[1]) : null);

      // Tag the category with birth/death (e.g., music_birth)
      const categoryTagged = `${chosenCat}_${it.__bucket}`;

      results.push({
        qid,
        key_mmdd: KEY_SLASH,
        title,
        summary,
        url,
        category: categoryTagged,
        year,
        event_mmdd: KEY_SLASH,
        times_seen: 0
      });
    }

    // 5) Dedupe by QID
    const dedupMap = new Map();
    for (const x of results) {
      if (!dedupMap.has(x.qid)) dedupMap.set(x.qid, x);
    }
    const deduped = Array.from(dedupMap.values());

    // 6) Sort for consistency: by category then title
    deduped.sort((a, b) => {
      const ca = a.category || "";
      const cb = b.category || "";
      if (ca !== cb) return ca.localeCompare(cb);
      return (a.title || "").localeCompare(b.title || "");
    });

    // 7) Output
    if (TO_STDOUT) {
      console.log(JSON.stringify(deduped, null, 2));
      return;
    }

    const fs = await import("node:fs/promises");
    const path = await import("node:path");
    const outDir = path.resolve(process.cwd(), "data", "otd");
    const outFile = path.join(outDir, `BD${KEY_SLASH}.json`); // e.g., BD12-13.json

    await fs.mkdir(outDir, { recursive: true });
    await fs.writeFile(outFile, JSON.stringify(deduped, null, 2) + "\n", "utf8");

    console.log(`Wrote ${outFile} with ${deduped.length} items`);
  } catch (err) {
    console.error(`[fetch-otd] ERROR: ${err.message || err}`);
    if (DEBUG) console.error(err.stack);
    process.exit(1);
  }
}

main();
