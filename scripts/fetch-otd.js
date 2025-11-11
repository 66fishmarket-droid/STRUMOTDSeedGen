import fetch from "node-fetch";

// Wikipedia + Wikidata endpoints
const WIKI = "https://en.wikipedia.org/api/rest_v1";
const WD_SPARQL = "https://query.wikidata.org/sparql";

// Today (UTC) for filename and key
const now = new Date();
const MM = String(now.getUTCMonth() + 1).padStart(2, "0");
const DD = String(now.getUTCDate()).padStart(2, "0");
const KEY = `${MM}-${DD}`;
const KEY_SLASH = `${MM}/${DD}`;

// Helpers
function uniq(arr) { return [...new Set(arr)]; }
function safe(val, d = "") { return val ?? d; }

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

  const VALUES = qids.map(q => `wd:${q}`).join(" ");
  const query = `
SELECT ?item (SAMPLE(?cat) AS ?category)
WHERE {
  VALUES ?item { ${VALUES} }
  OPTIONAL {
    ?item wdt:P31/wdt:P279* ?class .
    BIND(
      IF(?class IN (wd:Q482994, wd:Q134556, wd:Q7366, wd:Q182832, wd:Q222634), "music",
      IF(?class IN (wd:Q11424, wd:Q5398426, wd:Q21191270, wd:Q226730), "film_tv",
      IF(?class IN (wd:Q7725634, wd:Q571, wd:Q36180, wd:Q49757), "books",
      IF(?class IN (wd:Q4502142, wd:Q3305213, wd:Q860861, wd:Q2743, wd:Q2798201, wd:Q245068), "visual_or_performance",
      IF(?class IN (wd:Q618779, wd:Q132241), "awards", "other"))))) AS ?cat)
  }
}
GROUP BY ?item
HAVING(?category != "other")
`;

  const headers = {
    "User-Agent": "StrumOTD/1.0 (+https://github.com/66fishmarket-droid/STRUMOTDSeedGen)",
    "Accept": "application/sparql-results+json",
    "Content-Type": "application/sparql-query"
  };

  // POST to avoid URL length limits and handle transient errors
  for (let attempt = 1; attempt <= 3; attempt++) {
    const r = await fetch(WD_SPARQL, { method: "POST", headers, body: query });
    const ct = r.headers.get("content-type") || "";
    if (!r.ok || !ct.includes("application/sparql-results+json")) {
      if (attempt < 3 && (r.status === 429 || r.status >= 500)) {
        await new Promise(res => setTimeout(res, 1000 * attempt));
        continue;
      }
      const bodyText = await r.text();
      throw new Error(`SPARQL error ${r.status} ${r.statusText} | ct=${ct} | snippet=${bodyText.slice(0,200)}`);
    }
    const j = await r.json();
    const rows = j?.results?.bindings || [];
    const keep = new Set(rows.map(b => b.item.value.split("/").pop()));
    const categoryMap = new Map(rows.map(b => [b.item.value.split("/").pop(), b.category.value]));
    return { keep, categoryMap };
  }

  return { keep: new Set(), categoryMap: new Map() };
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

