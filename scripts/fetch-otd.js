import fetch from "node-fetch";

// Wikipedia + Wikidata endpoints
const WIKI = "https://en.wikipedia.org/api/rest_v1";
const WD_SPARQL = "https://query.wikidata.org/sparql";

// Date selection
// Default = today (UTC). Override with --date=YYYY-MM-DD or env TARGET_DATE.
const argDate = process.argv.find(a => a.startsWith("--date="))?.split("=")[1] || process.env.TARGET_DATE;
const base = argDate ? new Date(argDate + "T00:00:00Z") : new Date();
const MM = String(base.getUTCMonth() + 1).padStart(2, "0");
const DD = String(base.getUTCDate()).padStart(2, "0");
const KEY = `${MM}-${DD}`;
const KEY_SLASH = `${MM}/${DD}`;

// Helpers
function uniq(arr) { return [...new Set(arr)]; }
function safe(val, d = "") { return val ?? d; }
function chunk(arr, n) { const out = []; for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n)); return out; }

async function onThisDay(kind) {
  const r = await fetch(`${WIKI}/feed/onthisday/${kind}/${MM}/${DD}`, {
    headers: { "User-Agent": "StrumOTD/1.0 (GitHub Actions)" }
  });
  if (!r.ok) return [];
  const j = await r.json();
  return Array.isArray(j?.[kind]) ? j[kind] : [];
}

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

async function filterArts(qids) {
  if (qids.length === 0) return { keep: new Set(), categoryMap: new Map() };

  const headers = {
    "User-Agent": "StrumOTD/1.0 (+https://github.com/66fishmarket-droid/STRUMOTDSeedGen)",
    "Accept": "application/sparql-results+json",
    "Content-Type": "application/sparql-query"
  };

  const keep = new Set();
  const categoryMap = new Map();

  function chunk(arr, n) { const out = []; for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n)); return out; }

  for (const batch of chunk(qids, 50)) {
    const VALUES = batch.map(q => `wd:${q}`).join(" ");

    const PREFIX = `
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
`;

    const query = `
${PREFIX}
SELECT ?item ?category WHERE {
  VALUES ?item { ${VALUES} }

  BIND(
    IF(
      EXISTS { ?item wdt:P31/wdt:P279* ?c .
               FILTER(?c IN (wd:Q482994, wd:Q134556, wd:Q7366, wd:Q182832, wd:Q222634)) }, "music",
    IF(
      EXISTS { ?item wdt:P31/wdt:P279* ?c .
               FILTER(?c IN (wd:Q11424, wd:Q5398426, wd:Q21191270, wd:Q226730)) }, "film_tv",
    IF(
      EXISTS { ?item wdt:P31/wdt:P279* ?c .
               FILTER(?c IN (wd:Q7725634, wd:Q571)) }, "books",
    IF(
      EXISTS { ?item wdt:P31/wdt:P279* ?c .
               FILTER(?c IN (wd:Q4502142, wd:Q3305213, wd:Q860861, wd:Q2798201, wd:Q2743)) }, "visual_or_performance",
    IF(
      EXISTS { ?item wdt:P31/wdt:P279* ?c .
               FILTER(?c IN (wd:Q618779, wd:Q132241)) }, "awards",
    IF(
      EXISTS { ?item wdt:P31 wd:Q5; wdt:P106/wdt:P279* ?occ .
               FILTER(?occ IN (wd:Q639669, wd:Q177220, wd:Q753110, wd:Q36834, wd:Q183945, wd:Q1128996, wd:Q1320489, wd:Q155309, wd:Q488205, wd:Q130857)) }, "music",
    IF(
      EXISTS { ?item wdt:P31 wd:Q5; wdt:P106/wdt:P279* ?occ .
               FILTER(?occ IN (wd:Q33999, wd:Q2526255, wd:Q28389, wd:Q3455803, wd:Q1373334)) }, "film_tv",
    IF(
      EXISTS { ?item wdt:P31 wd:Q5; wdt:P106/wdt:P279* ?occ .
               FILTER(?occ IN (wd:Q36180, wd:Q49757, wd:Q6625963, wd:Q214917, wd:Q482980)) }, "books",
    IF(
      EXISTS { ?item wdt:P31 wd:Q5; wdt:P106/wdt:P279* ?occ .
               FILTER(?occ IN (wd:Q1028181, wd:Q1281618, wd:Q33231, wd:Q18394549, wd:Q12299841, wd:Q42973)) }, "visual_or_performance",
    IF(
      EXISTS { ?item wdt:P31 wd:Q5; wdt:P106/wdt:P279* ?occ .
               FILTER(?occ IN (wd:Q245068, wd:Q571668, wd:Q3282637, wd:Q1267013, wd:Q82955)) }, "visual_or_performance",
      "other"
    )))))))))) AS ?category)

  FILTER(?category != "other")
}
`;

    for (let attempt = 1; attempt <= 3; attempt++) {
      const r = await fetch("https://query.wikidata.org/sparql", { method: "POST", headers, body: query });
      const ct = (r.headers.get("content-type") || "").toLowerCase();
      const text = await r.text();

      if (!r.ok && (r.status === 429 || r.status >= 500)) {
        if (attempt < 3) { await new Promise(res => setTimeout(res, 1000 * attempt)); continue; }
      }
      if (!r.ok || !ct.includes("application/sparql-results+json")) {
        throw new Error(`SPARQL error status=${r.status} ct=${ct} snippet=${text.slice(0,200)}`);
      }

      let j;
      try { j = JSON.parse(text); }
      catch { throw new Error(`SPARQL parse error ct=${ct} status=${r.status} snippet=${text.slice(0,200)}`); }

      const rows = j?.results?.bindings || [];
      for (const b of rows) {
        const qid = b.item.value.split("/").pop();
        keep.add(qid);
        categoryMap.set(qid, b.category.value);
      }
      break; // batch OK
    }
  }

  return { keep, categoryMap };
}


function flatten(items) {
  const out = new Map();
  for (const it of items) {
    const yearFromField = it?.year;
    const yearFromText = it?.text?.match(/\b(\d{3,4})\b/)?.[1];
    const year = Number(yearFromField || yearFromText) || null;
    const page = (it.pages || []).find(p => p?.wikibase_item);
    if (!page) continue;

    const qid = page.wikibase_item;
    const url = page?.content_urls?.desktop?.page || `https://en.wikipedia.org/wiki/${encodeURIComponent(page.title)}`;

    out.set(qid, {
      qid,
      key_mmdd: KEY_SLASH,
      title: safe(page?.titles?.normalized, page?.title),
      summary: safe(it?.text, ""),
      url,
      year,
      event_mmdd: KEY_SLASH
    });
  }
  return [...out.values()];
}

(async () => {
  const [events, births, deaths, selected] = await Promise.all([
    onThisDay("events"),
    onThisDay("births"),
    onThisDay("deaths"),
    onThisDay("selected")
  ]);

  const allItems = [...events, ...births, ...deaths, ...selected];
  const qids = collectQids(allItems);
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

  const fs = await import("fs");
  fs.mkdirSync("data/otd", { recursive: true });
  fs.writeFileSync(`data/otd/${KEY}.json`, JSON.stringify(cleaned, null, 2));
  console.log(`Wrote data/otd/${KEY}.json with ${cleaned.length} items`);
})();
