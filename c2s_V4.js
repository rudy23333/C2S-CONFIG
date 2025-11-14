// c2s_V4.7.0.js
/**
 * 客户端数据收集与 Dashboard 渲染系统
 * 版本：V4.7.0
 * 最后修改：2025-11-14
 * 更新内容：支持 init-only 模式，完成后打印统计并停止
 */
const C2S_VERSION = 'V4.7.0';
/* ========= 1) 接口模块：API & 数据逻辑（并发优化） ========= */
console.log('[C2S/Client] Version:', C2S_VERSION);
const API_VER = 'v22.0';

const token = window.__accessToken;
if (!token) { console.error('❌ 未找到 window.__accessToken'); throw new Error('no-token'); }

const num = v => { const n = parseFloat(v); return Number.isFinite(n) ? n : 0; };
const f2  = n => Number.isFinite(n) ? n.toFixed(2) : '0.00';
const f0  = n => String(Math.round(Number.isFinite(n)?n:0));
const normAct = id => String(id).startsWith('act_') ? String(id) : `act_${id}`;
const plainId  = id => String(id).replace(/^act_/, '');

// 本地时间格式化：YYYY-MM-DD HH:mm:ss
function pad2(n){ return String(n).padStart(2,'0'); }
function formatLocalTs(d){ try{ const dt = (d instanceof Date) ? d : new Date(d); return dt.getFullYear() + '-' + pad2(dt.getMonth()+1) + '-' + pad2(dt.getDate()) + ' ' + pad2(dt.getHours()) + ':' + pad2(dt.getMinutes()) + ':' + pad2(dt.getSeconds()); } catch(e){ return String(d); } }

function getRange() {
  const p = new URLSearchParams(location.search);
  const d = p.get('date') || p.get('insights_date') || '';
  const m = d.match(/(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})/);
  if (m) return { since: m[1], until: m[2] };
  const t = new Date();
  const yyyy = t.getFullYear(), mm = String(t.getMonth()+1).padStart(2,'0'), dd = String(t.getDate()).padStart(2,'0');
  return { since: `${yyyy}-${mm}-${dd}`, until: `${yyyy}-${mm}-${dd}` };
}
const range = getRange();

function logV4Marker() {
  console.log('[C2S/V4] helper log invoked');
}
window.C2S_logV4 = logV4Marker;

/* 货币换算 */
const MINOR_UNITS = new Map(Object.entries({
  JPY:0, KRW:0, VND:0, IDR:0, CLP:0,
  BHD:3, JOD:3, KWD:3, OMR:3, TND:3, LYD:3, IQD:3
}));
function minorToMajor(amount_with_offset, currency='USD'){
  const minor = parseInt(amount_with_offset||0,10) || 0;
  const decimals = MINOR_UNITS.has(currency) ? MINOR_UNITS.get(currency) : 2;
  return minor / Math.pow(10, decimals);
}

function statusLabel(code){
  if (code === 1) return 'Active';
  if (code === 2) return 'Disabled';
  if (code === 3) return 'Unsettled';
  if (code === 7) return 'Pending';
  if (code === 8) return 'Banned';
  return '—';
}

/* 系列清单（带预算/状态，含删除） */
async function fetchAllCampaigns(actId) {
  const out = [];
  let url = `https://graph.facebook.com/${API_VER}/${actId}/campaigns` +
            `?fields=id,name,status,effective_status,daily_budget,lifetime_budget` +
            `&include_deleted=true&limit=500&access_token=${encodeURIComponent(token)}`;
  while (url) {
    const r = await fetch(url, { credentials: 'include' });
    const j = await r.json();
    if (Array.isArray(j?.data)) out.push(...j.data);
    url = j?.paging?.next || null;
  }
  return out;
}

const INACTIVE_STATUS_PATTERNS = ['DELETED', 'ARCHIVED'];
function isCampaignActive(status) {
  if (!status) return true;
  const upper = String(status).toUpperCase();
  return !INACTIVE_STATUS_PATTERNS.some(p => upper.includes(p));
}


/* helpers: 兼容数组/对象形式的 results & cost_per_result */
function readResultsField(results){
  if (Array.isArray(results)) {
    let sum = 0;
    for (const item of results) {
      const vals = Array.isArray(item?.values) ? item.values : [];
      for (const v of vals) sum += num(v?.value);
    }
    return sum;
  }
  return num(results);
}
function readCprField(cpr){
  if (Array.isArray(cpr)) {
    const first = cpr[0];
    const v = first && Array.isArray(first.values) && first.values[0] ? first.values[0].value : 0;
    return num(v);
  }
  return num(cpr);
}

/* 系列级 Insights（区间） */
async function fetchCampaignInsights(actId, range) {
  const base = `https://graph.facebook.com/${API_VER}/${actId}/insights`;
  const params = new URLSearchParams({
    level: 'campaign',
    fields: [
      'campaign_id','campaign_name','objective',
      'spend','impressions','clicks',
      'results','cost_per_result',
      'actions',
      'date_start','date_stop'
    ].join(','),
    limit: '5000',
    use_unified_attribution_setting: 'true',
    action_report_time: 'conversion',
    time_range: JSON.stringify({ since: range.since, until: range.until }),
    time_increment: 'all_days',
    access_token: token
  });
  let url = `${base}?${params.toString()}`;
  const rows = [];
  while (url) {
    const r = await fetch(url, { credentials:'include' });
    const j = await r.json();
    if (Array.isArray(j?.data)) rows.push(...j.data);
    url = j?.paging?.next || null;
  }
  return rows;
}



// —— 账单户：按当前累计（lifetime）取系列级 Insights —— //
async function fetchCampaignInsightsLifetime(actId) {
  const base = `https://graph.facebook.com/${API_VER}/${actId}/insights`;
  const params = new URLSearchParams({
    level: 'campaign',
    fields: [
      'campaign_id','campaign_name','objective',
      'spend','impressions','clicks',
      'results','cost_per_result',
      'actions',
      'date_start','date_stop'
    ].join(','),
    limit: '5000',
    use_unified_attribution_setting: 'true',
    action_report_time: 'conversion',
    time_increment: 'all_days',
    date_preset: 'lifetime',
    access_token: token
  });
  let url = `${base}?${params.toString()}`;
  const rows = [];
  while (url) {
    const r = await fetch(url, { credentials: 'include' });
    const j = await r.json();
    if (Array.isArray(j?.data)) rows.push(...j.data);
    url = j?.paging?.next || null;
  }
  return rows;
}



/* ---------- GraphQL 账务：一次收集、全局复用 ---------- */
function parseGraphQLTextResponse(text) {
  text = String(text || '').replace(/^for\s*\(\s*;;\s*\);\s*/g, '');
  const parts = text.match(/\{[\s\S]*?\}(?=\s*\{|\s*$)/g) || [text];
  for (const p of parts) { try { const obj = JSON.parse(p); const node = obj?.data?.billable_account_by_asset_id; if (node) return node; } catch {} }
  return null;
}
function collectDocIds() {
  const ids = new Set(["6401661393282937"]);
  try {
    const entries = performance.getEntriesByType("resource") || [];
    for (const e of entries) {
      const m = String(e.name||'').match(/[?&]doc_id=(\d{8,})/);
      if (m) ids.add(m[1]);
    }
    const html = document.documentElement.innerHTML;
    const re = /(?:["'?&]doc_id=|doc_id["']\s*:\s*["'])(\d{8,})/g;
    let mm; while ((mm = re.exec(html))!==null) ids.add(mm[1]);
  } catch {}
  return [...ids];
}
function getFbTokensCached(){
  if (getFbTokensCached._cache) return getFbTokensCached._cache;
  const fb_dtsg =
    document.querySelector('input[name="fb_dtsg"]')?.value
    || (window.require && require("DTSGInitialData")?.token)
    || (window.require && require("DTSG")?.getToken && require("DTSG").getToken());
  const lsd = window.__globalLSDToken || (window.require && require("LDS")?.token) || (window.require && require("LSD")?.token);
  const out = { fb_dtsg: fb_dtsg || null, lsd: lsd || null };
  getFbTokensCached._cache = out;
  return out;
}
const DOC_IDS_GLOBAL = collectDocIds();

async function fetchBillingNode(assetIdNumeric, docIds = DOC_IDS_GLOBAL) {
  const { fb_dtsg, lsd } = getFbTokensCached();
  if (!fb_dtsg || !lsd) return null;

  const ENDPOINTS = [
    'https://adsmanager.facebook.com/api/graphql/?_flowletID=1',
    'https://business.facebook.com/api/graphql/?_flowletID=1'
  ];

  for (const doc_id of docIds) {
    const params = new URLSearchParams();
    params.set('doc_id', doc_id);
    params.set('__aaid', String(assetIdNumeric));
    params.set('variables', JSON.stringify({ assetID: String(assetIdNumeric) }));
    params.set('fb_dtsg', fb_dtsg);
    params.set('lsd', lsd);
    const headers = {'Content-Type':'application/x-www-form-urlencoded','x-fb-lsd':lsd};

    for (const ep of ENDPOINTS) {
      try {
        const resp = await fetch(ep, { method:'POST', credentials:'include', headers, body:params });
        const text = await resp.text();
        const node = parseGraphQLTextResponse(text);
        if (node) return node;
      } catch {}
    }
  }
  return null;
}

// 获取账户元信息（安全降级）：返回 { name, currency, status_code }
async function getAccountMeta(actId) {
  try {
    const id = String(actId || '').trim();
    if (!id) return { name: '', currency: 'USD', status_code: 1 };
    const url = `https://graph.facebook.com/${API_VER}/${id}?fields=name,currency,account_status,account_id&access_token=${encodeURIComponent(token)}`;
    const resp = await fetch(url, { credentials: 'include' });
    const j = await resp.json();
    const name = j?.name || (`Account ${String(id).replace(/^act_?/i, '')}`);
    const currency = j?.currency || j?.currency_for_transactions || 'USD';
    const status_code = Number(j?.account_status) || Number(j?.account_status_code) || 1;
    return { name, currency, status_code };
  } catch (e) {
    console.warn('[C2S] getAccountMeta failed for', actId, e && e.message || e);
    return { name: `Account ${String(actId).replace(/^act_?/i, '')}`, currency: 'USD', status_code: 1 };
  }
}

/* ---------- 并发限制工具（简单 p-limit） ---------- */
function createLimiter(max = 3){
  let active = 0;
  const queue = [];
  const next = () => {
    if (active >= max || queue.length === 0) return;
    const { fn, resolve, reject } = queue.shift();
    active++;
    Promise.resolve().then(fn).then(
      (v)=>{ active--; resolve(v); next(); },
      (e)=>{ active--; reject(e); next(); }
    );
  };
  return (fn) => new Promise((resolve, reject)=>{
    queue.push({ fn, resolve, reject });
    next();
  });
}
const limit3 = createLimiter(3);

/* 汇总一个账户（并发请求：meta / campaigns / insights / billing） */
async function runOneAccount(raw) {
  // 兼容：raw 可能是 "act_123"/"123"（旧），也可能是 {account, billing, ...}（新 rows）
  const isRow = raw && typeof raw === 'object';
  const account_id  = normAct(isRow ? (raw.account || '') : raw);
  const account_num = plainId(account_id);
  const isBilling   = isRow ? (Number(raw.billing) === 1) : false;

  // 四个 API 并发获取（账单户：insights 用 lifetime；普通：用范围）
  const [meta, camps, insRaw, node] = await Promise.all([
    getAccountMeta(account_id),
    fetchAllCampaigns(account_id),
    isBilling ? fetchCampaignInsightsLifetime(account_id)
              : fetchCampaignInsights(account_id, range),
    fetchBillingNode(account_num)
  ]);

  const currency = meta.currency || 'USD';

  // 系列清单：预算/状态/名称映射（保持你原逻辑）
  const nameMap   = new Map();
  const statusMap = new Map();
  const budgetMap = new Map();
  camps.forEach(c=>{
    nameMap.set(c.id, c.name || '');
    statusMap.set(c.id, (c?.effective_status||c?.status||'').toUpperCase());
    const db = c?.daily_budget ? minorToMajor(c.daily_budget, currency) : null;
    const lb = c?.lifetime_budget ? minorToMajor(c.lifetime_budget, currency) : null;
    const prefill = db ?? lb ?? 0;
    budgetMap.set(c.id, Number(prefill));
  });

  // 账户预算合计（保持你原逻辑）
  const budgetTotal = camps.reduce((s,c)=>{
    const eff = (c?.effective_status||c?.status||'').toUpperCase();
    if (!eff.includes('ACTIVE')) return s;
    const db = c?.daily_budget ? Number(minorToMajor(c.daily_budget, currency)) : null;
    const lb = c?.lifetime_budget ? Number(minorToMajor(c.lifetime_budget, currency)) : null;
    return s + (db ?? lb ?? 0);
  }, 0);

  // 系列成效（按有效 campaign 过滤）
  let ins = Array.isArray(insRaw) ? insRaw : [];
  const activeCampaignIds = new Set();
  statusMap.forEach((status, campaignId) => {
    if (isCampaignActive(status)) activeCampaignIds.add(campaignId);
  });

  let insightsForT0 = ins.filter(row => {
    const cid = row?.campaign_id || '';
    return cid && activeCampaignIds.has(cid);
  });

  if (insightsForT0.length === 0 && activeCampaignIds.size > 0) {
    insightsForT0 = camps
      .filter(c => activeCampaignIds.has(c.id))
      .map(c => ({
        campaign_id: c.id,
        campaign_name: c.name || '',
        spend: 0,
        impressions: 0,
        clicks: 0,
        results: 0,
        cost_per_result: 0,
        actions: []
      }));
    console.warn(`[Insights] ${account_id} 无有效系列数据 → 使用系列清单兜底：`, insightsForT0.length);
  }

  const rows = insightsForT0.map(row=>{
    const campaign_id   = row.campaign_id || '';
    const campaign_name = row.campaign_name || nameMap.get(campaign_id) || '';

    const spend  = num(row.spend);
    const clicks = num(row.clicks);
    const imps   = num(row.impressions);

    const results = readResultsField(row.results);
    const cpr     = readCprField(row.cost_per_result);

    // 评论
    let comments = 0;
    if (Array.isArray(row.actions)) {
      for (const a of row.actions) if (a?.action_type === 'comment') comments += num(a?.value);
    }

    return {
      account_id, account_num,
      account_name: meta.name,
      currency,
      campaign_id, campaign_name,
      spend, results, cpr, clicks, impressions: imps, comments,
      budget: Number(budgetMap.get(campaign_id) || 0),
      enabled: (statusMap.get(campaign_id)||'').includes('ACTIVE'),
      eff_label: statusMap.get(campaign_id)||''
    };
  });

  // 步骤 1：查询得到的合计（T0 = 当前累计）
  const T0 = rows.reduce((a,r)=>({
    spend: a.spend + r.spend,
    results: a.results + r.results,
    clicks: a.clicks + r.clicks,
    impressions: a.impressions + r.impressions,
    comments: a.comments + r.comments
  }), {spend:0, results:0, clicks:0, impressions:0, comments:0});
  T0.cpr = T0.results>0 ? T0.spend/T0.results : 0;

  // 步骤 2：账单户：系列级差值计算
  let T = T0;           // 账户总计（普通户=T0，账单户=差值）
  let rowsForOutput = rows; // 系列数据（普通户=T0原始，账单户=按系列差值调整）
  
  if (isBilling) {
    // 步骤 2.1：获取该账户的系列级历史消耗（从 GAS doGet 返回的 campaign_history）
    // campaign_history 现在是系列级数据，不是账户级聚合
    const campaignHistory = window.__C2S_CAMPAIGN_HISTORY__ || [];
    const billingInitialized = window.__C2S_BILLING_INITIALIZED__;
    
    // 调试：打印原始历史数据
    console.log('[Billing] 原始 campaign_history 数据:', {
      length: campaignHistory.length,
      sample: campaignHistory.length > 0 ? campaignHistory[0] : null,
      account_num,
      lookingFor: account_num
    });
    
    // 调试：检查 campaign_history 是否为空或无数据
    if (campaignHistory.length === 0) {
      console.warn('[Billing] ⚠️ 历史消耗数据为空，账单户系统尚未初始化！', { 
        account: account_num,
        hint: '需要执行 init 阶段来初始化账单户历史。请调用 startC2S(..., 1) 进行初始化。'
      });
    }
    
    // 步骤 2.2：按系列级别计算差值
    // 构建历史消耗的系列映射：account_id + campaign_id -> history
    const historyMap = {};
    for (const h of campaignHistory) {
      const histAccId = String(h.account_id || '').replace(/^act_/i, '');
      const histCampaignId = String(h.campaign_id || '').trim();
      
      console.log('[Billing] 处理历史记录:', { histAccId, histCampaignId, account_num, match: histAccId === account_num });
      
      // 只关心当前账户的历史数据
      if (histAccId === account_num && histCampaignId) {
        historyMap[histCampaignId] = {
          spend:       num(h.spend),
          results:     num(h.results),
          clicks:      num(h.clicks),
          impressions: num(h.impressions),
          comments:    num(h.comments)
        };
      }
    }
    
    console.log('[Billing] 构建的 historyMap:', { keys: Object.keys(historyMap), mapSize: Object.keys(historyMap).length });
    
    // 步骤 2.3：对每个系列计算差值（当前 - 历史 = 本期消耗）
    rowsForOutput = rows.map(r => {
      const campaignId = String(r.campaign_id || '').trim();
      const hist = historyMap[campaignId] || {
        spend: 0, results: 0, clicks: 0, impressions: 0, comments: 0
      };
      
      const diff = {
        spend:       r.spend - hist.spend,
        results:     r.results - hist.results,
        clicks:      r.clicks - hist.clicks,
        impressions: r.impressions - hist.impressions,
        comments:    r.comments - hist.comments
      };
      
      // 记录负数增量（允许但标记）
      for (const [k, v] of Object.entries(diff)) {
        if (v < 0) {
          console.warn('[Billing] 系列负数增量', { 
            account: account_num, 
            campaign_id: campaignId,
            field: k, 
            current: r[k], 
            baseline: hist[k], 
            diff: v 
          });
        }
      }
      
      diff.cpr = diff.results > 0 ? (diff.spend / diff.results) : 0;
      
      return {
        ...r,
        spend:       diff.spend,
        results:     diff.results,
        clicks:      diff.clicks,
        impressions: diff.impressions,
        comments:    diff.comments,
        cpr:         diff.cpr
      };
    });
    
    // 步骤 2.4：重新聚合系列数据得到账户级总计
    const accDiff = rowsForOutput.reduce((a, r) => ({
      spend:       a.spend + r.spend,
      results:     a.results + r.results,
      clicks:      a.clicks + r.clicks,
      impressions: a.impressions + r.impressions,
      comments:    a.comments + r.comments
    }), {spend: 0, results: 0, clicks: 0, impressions: 0, comments: 0});
    accDiff.cpr = accDiff.results > 0 ? (accDiff.spend / accDiff.results) : 0;
    
    T = accDiff;
    
    const logLevel = campaignHistory.length === 0 ? 'warn' : 'log';
    console[logLevel]('[Billing] ✅ 账单户系列级计算完成', { 
      account: account_num, 
      currentSpend: T0.spend, 
      periodSpend: accDiff.spend,
      processedSeriesCount: rowsForOutput.length,
      foundHistoryCount: Object.keys(historyMap).length
    });
  }

  // 账务信息（保持你原逻辑）
  let accInfo = {threshold:'—', dsl:'—', unpaid:'—', currency};
  try{
    if (node){
      const th = node?.billing_threshold_currency_amount;
      if (th?.amount_with_offset != null) accInfo.threshold = fmtMoney(th.amount_with_offset, th.currency || currency);

      const dslFmt = node?.formatted_dsl;
      const dslRaw = node?.account_dsl?.amount_with_offset;
      if (dslFmt) accInfo.dsl = dslFmt.replace(/\s+/g,' ');
      else if (dslRaw != null) accInfo.dsl = fmtMoney(dslRaw, node?.account_dsl?.currency || currency);

      const unpaid = node?.account_balance_with_tax?.amount_with_offset;
      const unpaidCur = node?.account_balance_with_tax?.currency || currency;
      if (unpaid != null) accInfo.unpaid = fmtMoney(unpaid, unpaidCur);
    }
  }catch{}

  return {
    account_id,
    account_num,
    account_name: meta.name,
    account_status_label: statusLabel(meta.status_code),
    currency,
    rows: rowsForOutput,
    total: T,            // 普通：区间合计；账单户：差值后的当期
    budgetTotal,
    accInfo,
    billing: isBilling   // 可用于渲染时加小标识（不强制）
  };
}


/* 拉取所有账户块 & 页面合计（受控并发） */
async function loadAllBlocks() {
  const rows = (typeof window !== 'undefined') ? window.__accountRows : null;
  if (!Array.isArray(rows) || rows.length === 0) {
    throw new Error('[C2S] __accountRows 未准备好或为空，无法开始抓取');
  }

  const tasks  = rows.map(row => limit3(() => runOneAccount(row))); // 并发上限 3
  const blocks = await Promise.all(tasks);

  const grand = blocks.reduce((A, b) => {
    A.spend       += b.total.spend;
    A.results     += b.total.results;
    A.clicks      += b.total.clicks;
    A.impressions += b.total.impressions;
    A.comments    += b.total.comments;
    A.budget      += b.budgetTotal;
    return A;
  }, { spend:0, results:0, clicks:0, impressions:0, comments:0, budget:0 });

  grand.cpr = grand.results > 0 ? grand.spend / grand.results : 0;

  return { blocks, grand };
}


/* ========= 2) 样式模块：CSS ========= */
const CSS_TEXT = `
:root{
  --green:#22c55e; --green-dark:#16a34a;
  --bg:#f5f7fa; --text:#0f172a; --muted:#64748b;
  --border:#e2e8f0; --zebra:#f8fafc; --total:#eefaf3;
  --radius:12px;
}
*{box-sizing:border-box}
body{
  font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,"PingFang SC","Hiragino Sans GB","Microsoft YaHei",sans-serif;
  margin:0; padding:24px; display:flex; justify-content:center; background:var(--bg); color:var(--text);
}
.wrap{width:1240px;}

h2{margin:0 0 16px; text-align:center; font-weight:800;}
.toolbar{display:flex; gap:10px; justify-content:center; margin: 8px 0 14px; flex-wrap:wrap;}
.btn{appearance:none; border:1px solid var(--border); background:#fff; color:#111; padding:8px 14px; border-radius:8px; cursor:pointer; font-weight:700;}
.btn.primary{background:var(--green); color:#fff; border-color:var(--green);}
.btn.primary:hover{background:var(--green-dark);}

.card{background:white; border:1px solid var(--border); border-radius:var(--radius); padding:16px; box-shadow:0 1px 2px rgba(0,0,0,.05);}

table{
  width:100%;
  border-collapse:separate; border-spacing:0;
  border:1px solid var(--border); border-radius:10px; overflow:hidden;
  table-layout:fixed;
}
thead th{background:var(--green); color:#fff; padding:10px 8px; text-align:center; font-weight:700;}
tbody td, tfoot td{border-bottom:1px solid var(--border); padding:10px 8px; text-align:center; white-space:nowrap;}

/* 账户合计（父行）——保留柔和底色，但移除左侧绿边 */
tr.acc-row td{ background:#f7fffb; font-weight:600 !important; }
tr.acc-row td:first-child{ text-align:left; }

/* 总计行美化：柔和底 + 600 字重；天然对齐 */
tfoot tr{ background:var(--total); }
tfoot td{ font-weight:600; border-top:1px solid var(--border); }
tfoot td:first-child{ text-align:left; }
.total-label{
  display:inline-block; padding:3px 10px; border-radius:999px;
  background:#d9f6e6; color:#0a7a3f; font-weight:700; font-size:12px; margin-right:6px;
}

tbody tr.camp-row{background:#fff; font-size:13px; font-weight:500;}
tbody tr.camp-row:nth-child(odd){background:var(--zebra);}

.tree{display:flex; align-items:center; gap:8px; justify-content:flex-start;}
.cell-left{ text-align:left; overflow:hidden; white-space:nowrap; text-overflow:ellipsis; }

.toggle{width:22px; height:22px; line-height:20px; text-align:center; border:1px solid var(--border); background:#fff; cursor:pointer; border-radius:6px; font-weight:700}

.acc-name{ color:#0a7a3f; text-decoration: underline; cursor:pointer; }
.acc-name:hover{ color:#076233; }

.badge-link{ text-decoration:none; }
.badge{display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; font-weight:700; background:#e2fbe5; color:#0a7a3f; border:1px solid #b7f0c0;}

/* 开关（仅视觉） */
.switch{position:relative; width:46px; height:26px; border-radius:999px; background:#e5e7eb; transition:.2s; display:inline-block; vertical-align:middle; flex:0 0 46px;}
.switch.on{background:#5aa9ff;}
.switch .knob{position:absolute; top:3px; left:3px; width:20px; height:20px; border-radius:50%; background:#fff; transition:.2s;}
.switch.on .knob{left:23px;}

/* 输入宽度与列宽匹配 */
.budget-input{
  width:80px; padding:6px 8px;
  border:1px solid var(--border); border-radius:8px;
  background:#f9fafb; text-align:center; font-weight:700; color:#333;
}

/* 二级标题 */
.section-title {
  margin: 24px 0 8px;
  font-weight: 800;
  font-size: 18px;
}

/* 列宽（与 colgroup 一致） */
col.col-name{ width:32%; }
col.col-id{   width:19%; }
col.col-num{  width:7%;  }

.info-note{margin:10px 0 0; text-align:center; color:#667085; font-size:12px;}
`;








/* ========= 3) 生成 HTML 模块 ========= */
function buildAccountRow(b){
  const actLink = `https://adsmanager.facebook.com/adsmanager/manage/campaigns?act=${b.account_num}&date=${range.since}_${range.until}%2Ctoday&insights_date=${range.since}_${range.until}%2Ctoday`;
  const supportLink = `https://www.facebook.com/business-support-home/${b.account_num}/`;
  return `
    <tr class="acc-row" data-acc="${b.account_id}">
      <td class="tree cell-left">
        <button class="toggle" data-acc="${b.account_id}" aria-expanded="false">＋</button>
        <a class="acc-name" href="${actLink}" target="_blank" rel="noopener noreferrer">${b.account_name}</a>
        <a class="badge-link" href="${supportLink}" target="_blank" rel="noopener noreferrer" style="margin-left:8px;">
          <span class="badge">${b.account_status_label||'—'}</span>
        </a>
      </td>
      <td>${b.account_id}</td>
      <td>${f2(b.budgetTotal)}</td>
      <td>${f2(b.total.spend)}</td>
      <td>${f0(b.total.results)}</td>
      <td>${f2(b.total.cpr)}</td>
      <td>${f0(b.total.clicks)}</td>
      <td>${f0(b.total.impressions)}</td>
      <td>${f0(b.total.comments)}</td>
    </tr>
  `;
}
function buildCampaignRow(r){
  const on = !!r.enabled;
  return `
    <tr class="camp-row" data-parent="${r.account_id}" style="display:none">
      <td class="tree cell-left">
        <span class="switch ${on?'on':''}" aria-checked="${on?'true':'false'}"><span class="knob"></span></span>
        <span style="margin-left:8px;">${r.campaign_name || ''}</span>
      </td>
      <td>${r.campaign_id || ''}</td>
      <td><input class="budget-input" value="${f2(Number(r.budget||0))}" /></td>
      <td>${f2(r.spend)}</td>
      <td>${f0(r.results)}</td>
      <td>${f2(r.cpr)}</td>
      <td>${f0(r.clicks)}</td>
      <td>${f0(r.impressions)}</td>
      <td>${f0(r.comments)}</td>
    </tr>
  `;
}
function buildInfoRow(b){
  const billingLink = `https://business.facebook.com/billing_hub/accounts/details?asset_id=${b.account_num}`;
  return `
    <tr>
      <td class="cell-left"><a class="acc-name" href="${billingLink}" target="_blank" rel="noopener noreferrer">${b.account_name}</a></td>
      <td>${b.account_id}</td>
      <td><a class="badge-link" href="https://www.facebook.com/business-support-home/${b.account_num}/" target="_blank" rel="noopener noreferrer"><span class="badge">${b.account_status_label||'—'}</span></a></td>
      <td>${b.accInfo.threshold}</td>
      <td>${b.accInfo.dsl}</td>
      <td>${b.accInfo.unpaid}</td>
    </tr>
  `;
}

function buildHTML(blocks, grand){
  const tableBodyHTML = blocks.map(b=> buildAccountRow(b) + b.rows.map(buildCampaignRow).join('')).join('');
  const infoRows = blocks.map(buildInfoRow).join('');
  return `
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>广告账户统计（${range.since} ~ ${range.until}）</title>
<style>${CSS_TEXT}</style>
</head>
<body>
  <div class="wrap">
    <h2>广告账户统计（<strong>${range.since} ~ ${range.until}</strong>）</h2>
    <div class="toolbar">
      <button id="expandAll" class="btn">全部展开</button>
      <button id="collapseAll" class="btn">全部折叠</button>
      <button id="exportAccount" class="btn">导出广告账户 CSV</button>
      <button id="exportCampaign" class="btn primary">导出系列 CSV</button>
    </div>

    <div class="card">
      <table id="pivot">
        <colgroup>
          <col class="col-name">
          <col class="col-id">
          <col class="col-num"><col class="col-num"><col class="col-num"><col class="col-num">
          <col class="col-num"><col class="col-num"><col class="col-num">
        </colgroup>
        <thead>
          <tr>${["账户/系列","ID","预算","消耗","成效","成效单价","点击","展示","评论"].map(c=>`<th>${c}</th>`).join('')}</tr>
        </thead>
        <tbody>${tableBodyHTML}</tbody>
        <tfoot>
          <tr>
            <td class="cell-left"><span class="total-label">总计</span></td>
            <td></td>
            <td>${f2(grand.budget)}</td>
            <td>${f2(grand.spend)}</td>
            <td>${f0(grand.results)}</td>
            <td>${f2(grand.cpr)}</td>
            <td>${f0(grand.clicks)}</td>
            <td>${f0(grand.impressions)}</td>
            <td>${f0(grand.comments)}</td>
          </tr>
        </tfoot>
      </table>
    </div>

    <div class="section-title">广告账户信息表</div>
    <div class="card">
      <table>
        <thead>
          <tr>
            <th>账户</th><th>账户ID</th><th>状态</th><th>门槛</th><th>临时限额</th><th>未支付</th>
          </tr>
        </thead>
        <tbody>${infoRows}</tbody>
      </table>
    </div>
  </div>

  <script>${INLINE_SCRIPT}</script>
</body></html>
`;
}

async function renderPage(){
  const { blocks, grand } = await loadAllBlocks();
  const html = buildHTML(blocks, grand);
  // 统一数据口（有 range 就带上，没有可传 null）
  const dto = { range: (window.__range || null), blocks, grand };
  // 复用同一展示页
  C2S_Viewer.openOnce();
  C2S_Viewer.renderHTML(html, dto);
  // 暴露给 TM：挂全局 + 派事件（便于 TM 监听每一轮）
  window.__C2S_DTO = dto;
  
  // 输出版本信息
  const gasVersion = window.__GAS_VERSION__ || 'unknown';
  console.log(`[C2S] 系统版本信息 - Client: ${C2S_VERSION}, GAS: ${gasVersion}`);
  
  window.dispatchEvent(new CustomEvent('C2S:DID_FETCH', { detail: { dto, lastAt: Date.now() } }));
}


/* ========= 4) 生成页面所需的内联 JS ========= */
const INLINE_SCRIPT = `
  // 折叠/展开单个账户
  document.querySelectorAll('.toggle').forEach(btn=>{
    btn.addEventListener('click', ()=>{
      const acc = btn.getAttribute('data-acc');
      const open = btn.getAttribute('aria-expanded') === 'true';
      const rows = document.querySelectorAll(\`tr.camp-row[data-parent="\${acc}"]\`);
      rows.forEach(r => r.style.display = open ? 'none' : '');
      btn.setAttribute('aria-expanded', open ? 'false' : 'true');
      btn.textContent = open ? '＋' : '－';
    });
  });

  // 全部展开/折叠
  document.getElementById('expandAll').onclick = ()=>{
    document.querySelectorAll('.toggle').forEach(btn=>{
      const acc = btn.getAttribute('data-acc');
      document.querySelectorAll(\`tr.camp-row[data-parent="\${acc}"]\`).forEach(r => r.style.display = '');
      btn.setAttribute('aria-expanded','true'); btn.textContent='－';
    });
  };
  document.getElementById('collapseAll').onclick = ()=>{
    document.querySelectorAll('.toggle').forEach(btn=>{
      const acc = btn.getAttribute('data-acc');
      document.querySelectorAll(\`tr.camp-row[data-parent="\${acc}"]\`).forEach(r => r.style.display = 'none');
      btn.setAttribute('aria-expanded','false'); btn.textContent='＋';
    });
  };

  // 系列开关点击（仅视觉，不调 API）
  document.getElementById('pivot').addEventListener('click', (e)=>{
    const sw = e.target.closest('.switch');
    if (!sw) return;
    sw.classList.toggle('on');
    sw.setAttribute('aria-checked', sw.classList.contains('on') ? 'true' : 'false');
  });

  // 导出账户合计 CSV
  document.getElementById('exportAccount').onclick = ()=>{
    const headers = ["账户","账户ID","预算","消耗","成效","成效单价","点击","展示","评论"];
    const lines = [headers];
    document.querySelectorAll('tr.acc-row').forEach(tr=>{
      const tds = [...tr.children].map(td => td.innerText.trim());
      if (tds.length){
        lines.push([tds[0], tds[1], tds[2], tds[3], tds[4], tds[5], tds[6], tds[7], tds[8]]);
      }
    });
    const total = [...document.querySelectorAll('tfoot td')].map(td => td.innerText.trim());
    lines.push(["总计","", total[2], total[3], total[4], total[5], total[6], total[7], total[8]]);
    exportCSV(lines, "accounts_summary.csv");
  };

  // 导出系列 CSV
  document.getElementById('exportCampaign').onclick = ()=>{
    const headers = ["账户","账户ID","系列","系列ID","预算","消耗","成效","成效单价","点击","展示","评论"];
    const lines = [headers];
    document.querySelectorAll('tr.camp-row').forEach(tr=>{
      const [nameTd, idTd, budTd, spendTd, resTd, cprTd, clkTd, impTd, comTd] = [...tr.children];
      const accId = tr.getAttribute('data-parent');
      const accRow = document.querySelector(\`tr.acc-row[data-acc="\${accId}"]\`);
      const accName = accRow ? accRow.querySelector('.acc-name').innerText : '';
      const accIdText = accRow ? accRow.children[1].innerText : accId;
      const budgetVal = budTd.querySelector('input')?.value ?? '';
      lines.push([
        accName, accIdText,
        nameTd.innerText.replace(/^\\s*/,'').trim(), idTd.innerText.trim(),
        budgetVal, spendTd.innerText.trim(), resTd.innerText.trim(), cprTd.innerText.trim(),
        clkTd.innerText.trim(), impTd.innerText.trim(), comTd.innerText.trim()
      ]);
    });
    const tds = [...document.querySelectorAll('tfoot td')].map(td => td.innerText.trim());
    lines.push(["合计","","","", tds[2], tds[3], tds[4], tds[5], tds[6], tds[7], tds[8]]);
    exportCSV(lines, "campaign_detail.csv");
  };

  function exportCSV(lines, filename){
    const csv = "\\uFEFF" + lines.map(row => row.map(v => '"' + String(v).replace(/"/g,'""') + '"').join(',')).join("\\n");
    const blob = new Blob([csv], {type:'text/csv;charset=utf-8;'});
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = filename; a.click();
    URL.revokeObjectURL(url);
  }
`;


renderPage();


/* =====================================  主控追加：单窗口 + 循环刷新（与原文件风格统一）  ===================================== */
/* ========= 1) 模块：Input（读取依赖：token / accountIds / 日期区间） ========= */
(function(){
  function _getRange(){
    try {
      var p = new URLSearchParams(location.search);
      var d = p.get('date') || p.get('insights_date') || '';
      var m = d && d.match(/(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})/);
      if (m) return { since: m[1], until: m[2] };
    } catch(_){}
    var t = new Date(), yyyy = t.getFullYear(), mm = String(t.getMonth()+1).padStart(2,'0'), dd = String(t.getDate()).padStart(2,'0');
    return { since: yyyy + '-' + mm + '-' + dd, until: yyyy + '-' + mm + '-' + dd };
  }

  function InputModule(){
    var token = (typeof window !== 'undefined' && window.__accessToken) ? window.__accessToken : '';
    if (!token) throw new Error('Missing __accessToken');

    var rows = (typeof window !== 'undefined' && Array.isArray(window.__accountRows)) ? window.__accountRows : null;
    if (!rows || rows.length === 0)
      throw new Error('[C2S] __accountRows 未准备好或为空');

    function _getRange(){
      try {
        var p = new URLSearchParams(location.search);
        var d = p.get('date') || p.get('insights_date') || '';
        var m = d && d.match(/(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})/);
        if (m) return { since: m[1], until: m[2] };
      } catch(_){}
      var t = new Date(), yyyy = t.getFullYear(), mm = String(t.getMonth()+1).padStart(2,'0'), dd = String(t.getDate()).padStart(2,'0');
      return { since: yyyy + '-' + mm + '-' + dd, until: yyyy + '-' + mm + '-' + dd };
    }

    return { token: token, rows: rows, range: _getRange() };
  }

  // 保留这行：导出到全局
  window.C2S_InputModule = InputModule;

})();


/* ========= 2) 模块：Fetch（并发抓取 + 聚合；复用你原文件中的 runOneAccount / createLimiter 等） ========= */
(function(){
  function FetchModule(ctx){
    var limiter = (typeof createLimiter === 'function') ? createLimiter(3) : (function(fn){ return fn(); });

    function _limitWrap(task){
      if (typeof createLimiter === 'function') return limiter(task);
      return task();
    }

    async function run(){
      var rows = ctx.rows || [];
      var tasks = rows.map(function(row){
        return _limitWrap(function(){ return runOneAccount(row); });
      });
      var blocks = await Promise.all(tasks);
      var grand = { spend:0, results:0, clicks:0, impressions:0, comments:0, budget:0, cpr:0 };
      for (var i=0;i<blocks.length;i++){
        var b = blocks[i] || {};
        var t = b.total || {};
        grand.spend       += +((t.spend)||0);
        grand.results     += +((t.results)||0);
        grand.clicks      += +((t.clicks)||0);
        grand.impressions += +((t.impressions)||0);
        grand.comments    += +((t.comments)||0);
        grand.budget      += +((b.budgetTotal)||0);
      }
      grand.cpr = grand.results > 0 ? (grand.spend / grand.results) : 0;
      return { blocks: blocks, grand: grand };
    }


    return { run: run };
  }

  window.C2S_FetchModule = FetchModule;
})();


/* ========= 3) 模块：Model（DTO 统一模型；可在此做二次衍生指标） ========= */
(function(){
  function ModelModule(){
    function createDTO(range, blocks){
      var grand = { spend:0, results:0, clicks:0, impressions:0, comments:0, budget:0, cpr:0 };
      for (var i=0;i<(blocks||[]).length;i++){
        var b = blocks[i] || {};
        var t = b.total || {};
        grand.spend       += +((t.spend)||0);
        grand.results     += +((t.results)||0);
        grand.clicks      += +((t.clicks)||0);
        grand.impressions += +((t.impressions)||0);
        grand.comments    += +((t.comments)||0);
        grand.budget      += +((b.budgetTotal)||0);
      }
      grand.cpr = grand.results ? (grand.spend / grand.results) : 0;
      return { range: range, blocks: blocks, grand: grand };
    }
    return { createDTO: createDTO };
  }
  window.C2S_ModelModule = ModelModule;
})();


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/* ========= 4) 模块：Viewer（单窗口 + 固定地址；iframe 承载完整 HTML；显示 Fetched 与 Next 倒计时） ========= */
(function(){
  var WIN_NAME = "C2S_Viewer_SINGLE";
  var winRef = null;

  // —— 单文件“壳”页：顶栏含 Fetched 与 Next；主体用 <iframe id="stage"> 承载你的完整 HTML —— //
  var SHELL = '<!doctype html><html><head><meta charset="utf-8">'
    + '<meta name="viewport" content="width=device-width,initial-scale=1">'
    + '<title>Million volts Dashboard</title>'
    + '<style>html,body{height:100%;margin:0}'
    + '#wrap{height:100%;display:flex;flex-direction:column;background:#0b0f17;color:#fff}'
    + '#bar{padding:8px 12px;background:#121826;display:flex;align-items:center;gap:12px;box-shadow:0 1px 0 rgba(255,255,255,.06)}'
    + '#ft{margin-left:12px}'
    + '#cd{margin-left:auto;font-weight:600}'
    + '#stage{flex:1;border:0;width:100%}'
    + '</style></head><body>'
    + '<div id="wrap">'
      + '<div id="bar">'
        + '<div>Million volts Dashboard</div>'
        + '<div id="ft">本轮完成: --:--:--</div>'
        + '<div id="cd">下轮倒计时: --:--</div>'
      + '</div>'
      + '<iframe id="stage"></iframe>'
    + '</div>'
    + '<script>'
    + '(function(){'
    + '  var stage=null, nextAt=0, timer=null;'
    + '  function fmt(ms){ if(ms<0)ms=0; var s=Math.floor(ms/1000); var m=Math.floor(s/60); s=s%60; return String(m).padStart(2,"0")+":"+String(s).padStart(2,"0"); }'
    + '  function fmtTime(ts){ if(!ts) return "--:--:--"; var d=new Date(ts); var h=String(d.getHours()).padStart(2,"0"); var m=String(d.getMinutes()).padStart(2,"0"); var s=String(d.getSeconds()).padStart(2,"0"); return h+":"+m+":"+s; }'
    + '  function setCountdown(ts){ nextAt=ts||0; if(timer) clearInterval(timer);'
    + '    if(!nextAt){ document.getElementById("cd").textContent="Next: --:--"; return; }'
    + '    timer=setInterval(function(){ var left=nextAt-Date.now(); document.getElementById("cd").textContent="Next: "+fmt(left); console.log("[Viewer] next in", Math.max(0,Math.floor(left/1000)),"s"); }, 1000); }'
    + '  function mount(){ stage=document.getElementById("stage"); }'
    + '  function injectDto(html,dtoJson){'
    + '    try{ if(!html) return html; var marker="</body>";'
    + '      var inject="<script id=\\"__C2S_dto\\" type=\\"application/json\\">"+dtoJson+"</"+"script><script>try{window.__C2S_DTO=JSON.parse(document.getElementById(\\"__C2S_dto\\").textContent||\\"{}\\");}catch(e){}</"+"script>";'
    + '      if(html.indexOf(marker)>=0) return html.replace(marker, inject+marker); return html+inject;'
    + '    }catch(_){ return html; }'
    + '  }'
    + '  window.addEventListener("message",function(ev){'
    + '    var d=ev.data||{};'
    + '    if(d.type==="C2S_HTML"){'
    + '      if(!stage) mount();'
    + '      var html=String(d.payload && d.payload.html || "");'
    + '      var dto=d.payload && d.payload.dto ? JSON.stringify(d.payload.dto) : "{}";'
    + '      var withDto=injectDto(html,dto);'
    + '      if(stage) stage.srcdoc=withDto;'
    + '    } else if (d.type==="C2S_TICK"){'
    + '      setCountdown(d.payload && d.payload.nextAt || 0);'
    + '    } else if (d.type==="C2S_META"){'
    + '      var p=d.payload||{};'
    + '      var ft=document.getElementById("ft"); if(ft) ft.textContent="Fetched: "+fmtTime(p.lastAt);'
    + '      setCountdown(p.nextAt||0);'
    + '    }'
    + '  });'
    + '  document.addEventListener("DOMContentLoaded", mount);'
    + '})();'
    + '</' + 'script>'
    + '</body></html>';

  function openOnce(){
    winRef = window.open('', WIN_NAME);
    if (winRef && winRef.document && winRef.document.body && !winRef.document.getElementById('stage')){
      winRef.document.open(); winRef.document.write(SHELL); winRef.document.close();
    }
  }

  function renderHTML(html, dto){
    if (!winRef || winRef.closed) openOnce();
    if (!winRef) return;
    winRef.postMessage({ type:'C2S_HTML', payload:{ html:html, dto:dto } }, '*');
  }

  function tick(nextAt){
    if (!winRef || winRef.closed) openOnce();
    if (!winRef) return;
    winRef.postMessage({ type:'C2S_TICK', payload:{ nextAt: nextAt } }, '*');
  }

  function meta(meta){
    if (!winRef || winRef.closed) openOnce();
    if (!winRef) return;
    winRef.postMessage({ type:'C2S_META', payload: meta || {} }, '*');
  }

  window.C2S_Viewer = { openOnce: openOnce, renderHTML: renderHTML, tick: tick, meta: meta };
})();



/* ========= 5) 模块：Render（把 DTO 转完整 HTML，并交给 Viewer；不再创建 blob 页） ========= */
(function(){
  function RenderModule(){
    function toHTML(dto){
      return (typeof buildHTML === 'function')
        ? buildHTML(dto.blocks, dto.grand)
        : '<!doctype html><meta charset="utf-8"><title>JL</title><pre>buildHTML() 未定义</pre>';
    }
    function renderToViewer(dto){
      var html = toHTML(dto);
      window.C2S_Viewer.renderHTML(html, dto);
    }
    return { toHTML: toHTML, renderToViewer: renderToViewer };
  }
  window.C2S_RenderModule = RenderModule;
})();


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/* ========= 6) 模块：LoopRunner（单一调度器：抓数与倒计时一体；含 5s 日志节流 & 过时立刻抓取） ========= */
(function(){
  var TIMER        = null;        // 单一计时器（每秒调度）
  var RUNNING      = false;       // 抓数中标志，避免重入
  var NEXT_AT      = 0;           // 下次抓取时间戳（ms）
  var LAST_DONE_AT = 0;           // 上一轮“完成时间戳”（ms）
  var LAST_LOG_TS  = 0;           // 上一次打印倒计时日志的时间（ms）
  var LOG_STEP_MS  = 20000;        // 控制台倒计时日志间隔（默认 5 秒）
  window.__C2S_TIMER = null;   // 对外暴露计时器句柄（供 TM/控制台查看）

  function sleep(ms){ return new Promise(function(r){ setTimeout(r, ms); }); }
  function now(){ return Date.now(); }
  
  // init-only 模式统计输出
  function printInitOnlyStats() {
    console.log('%c[C2S/Init] ===== INIT 初始化统计 =====', 'color: #22c55e; font-weight: bold; font-size: 14px;');
    
    try {
      const lastCore = window.__C2S_LAST_CORE__ || {};
      const blocks = Array.isArray(lastCore.blocks_raw) ? lastCore.blocks_raw : [];
      const baseline = Array.isArray(lastCore.baseline_blocks) ? lastCore.baseline_blocks : [];
      
      // 统计账单户数量
      let billingCount = 0;
      const billingAccounts = [];
      for (const block of blocks) {
        if (block.billing) {
          billingCount++;
          billingAccounts.push({
            account_id: block.account_id,
            account_name: block.account_name,
            series_count: (block.rows || []).length,
            total_spend: block.total?.spend || 0,
            total_results: block.total?.results || 0
          });
        }
      }
      
      console.log(`%c账单户数量: ${billingCount}`, 'color: #3b82f6; font-weight: bold;');
      console.log(`%c保存基线记录: ${baseline.length}`, 'color: #3b82f6; font-weight: bold;');
      
      if (billingAccounts.length > 0) {
        console.log('%c账单户详情:', 'color: #0ea5e9;');
        for (const acc of billingAccounts) {
          console.log(
            `  • ${acc.account_name} (${acc.account_id}): ${acc.series_count} 个系列, 消耗 $${acc.total_spend.toFixed(2)}, 成效 ${acc.total_results}`
          );
        }
      }
      
      console.log('%c[C2S/Init] =====================================', 'color: #22c55e; font-weight: bold;');
      console.log('%c💡 提示：请运行以下命令进行常规抓取:', 'color: #f59e0b;');
      const meta = window.__C2S_META__ || {};
      console.log(`  startC2S('${meta.user || 'USER'}', '${meta.geo || 'GEO'}', '${meta.sign || 'SIGN'}')`);
    } catch (e) {
      console.warn('[C2S/Init] 统计异常:', e);
    }
  }


  async function main_loop(intervalMs){
    // 检查 init-only 模式
    const isInitOnlyMode = Number(intervalMs || 0) === -1;
    if (isInitOnlyMode) {
      intervalMs = 60000; // 虚拟 interval
      console.log('%c[Loop] 进入 INIT-ONLY 模式，完成一轮 init 后停止', 'color: #f97316; font-weight: bold;');
    } else {
      intervalMs = Number(intervalMs||0) || (5*60*1000);
      console.log('[Loop] start, interval =', Math.floor(intervalMs/1000), 'sec');
    }

    var input, fetcher, model, render;
    try {
      input   = window.C2S_InputModule();
      fetcher = window.C2S_FetchModule(input);
      model   = window.C2S_ModelModule();
      render  = window.C2S_RenderModule();
    } catch (e) {
      console.error('[Loop] init failed:', (e && e.message) || e, {
        hasToken: !!window.__accessToken,
        rowsType: Object.prototype.toString.call(window.__accountRows),
        rowsLen: Array.isArray(window.__accountRows) ? window.__accountRows.length : null
      });
      return; // 直接返回，避免挂了半截的调度器
    }


    window.C2S_Viewer.openOnce();

    // —— 页面从后台回到前台：若已过期，立刻拉取 —— //
    document.addEventListener('visibilitychange', function(){
      if (document.hidden) return;
      if (!RUNNING && LAST_DONE_AT && (now() - LAST_DONE_AT >= intervalMs)) {
        // 标记为到期，立刻调度一次
        NEXT_AT = 0;
        // 立即调度（不等到下一秒）
        if (TIMER) { clearTimeout(TIMER); TIMER = null; window.__C2S_TIMER = null; }
        runScheduler();
      }
    });

    async function runScheduler(){
      // 正在抓数：下一秒再检查，避免重入
      if (RUNNING) { TIMER = setTimeout(runScheduler, 1000); window.__C2S_TIMER = TIMER; return; }

      var leftMsRaw = NEXT_AT - now();
      var leftMs    = Math.max(0, leftMsRaw);

      // “是否应立即抓取”的三个条件：
      // 1) 首轮（NEXT_AT=0）；2) 倒计时到点（leftMs==0）；3) 距上次完成已超出 interval（窗口最小化/后台导致定时停滞时）
      var overdue = LAST_DONE_AT && (now() - LAST_DONE_AT >= intervalMs);
      var shouldFetchNow = (!NEXT_AT) || (leftMs === 0) || overdue;

      if (shouldFetchNow){
        RUNNING = true;
        console.log('[Loop] fetching…');

      try{
        console.time('[Loop] fetcher.run');
        var raw = await fetcher.run();
        console.timeEnd('[Loop] fetcher.run');

        console.log('[Loop] raw.range =', raw && raw.range, 'blocks.len =', raw && raw.blocks && raw.blocks.length);
        var dto = model.createDTO(raw.range, raw.blocks);
        console.log('[Loop] fetched, blocks =', (dto.blocks||[]).length);

        console.time('[Loop] render');
        render.renderToViewer(dto);
        console.timeEnd('[Loop] render');
        window.__C2S_DTO = dto;
        // 兼容老逻辑（window）+ 新核心（document）
        try {
          window.dispatchEvent(new CustomEvent('C2S:DID_FETCH', { detail: { dto: dto, lastAt: Date.now() } }));
        } catch (_) {}
        try {
          document.dispatchEvent(new Event('C2S:DID_FETCH'));
          console.log('[C2S] event dispatched: C2S:DID_FETCH');
        } catch (e) {
          console.warn('[C2S] failed to dispatch C2S:DID_FETCH:', e);
        }

        // 若为 init-only 模式且当前轮是 init 轮，则等待 poster 将 __BILLING_INITED__ 置为 true
        if (isInitOnlyMode) {
          try {
            const isInitRound = (window.__C2S_LAST_CORE__ && window.__C2S_LAST_CORE__.is_init) || false;
            if (isInitRound) {
              const waitTimeout = 30 * 1000; // 最长等待 30s
              const pollInterval = 500;
              const t0 = Date.now();
              while (Date.now() - t0 < waitTimeout) {
                if (Boolean(window.__BILLING_INITED__)) break;
                await new Promise(r => setTimeout(r, pollInterval));
              }
            }
          } catch (e) {
            console.warn('[Loop] wait for billing init failed:', e);
          }
        }

      }catch(e){
        console.error('[Loop] error:', e && e.message || e, e && e.stack);
      } finally {
        RUNNING      = false;
        LAST_DONE_AT = now();                      // 记录本轮完成时间
        NEXT_AT      = LAST_DONE_AT + intervalMs;  // 重置下一轮时间
        LAST_LOG_TS  = 0;                          // 重置日志节流
        window.C2S_Viewer.meta({ lastAt: LAST_DONE_AT, nextAt: NEXT_AT }); // 同步到壳页
        
        // init-only 模式：检查是否已完成 init，完成则停止
        if (isInitOnlyMode) {
          const billingInited = Boolean(window.__BILLING_INITED__);
          if (billingInited) {
            console.log('%c[C2S/Loop] ✅ INIT-ONLY 模式完成！初始化已完成，停止循环', 'color: #22c55e; font-weight: bold;');
            // 打印 init 统计信息
            printInitOnlyStats();
            // 清空定时器，停止循环
            if (TIMER) { clearTimeout(TIMER); TIMER = null; window.__C2S_TIMER = null; }
            return;
          }
        }
      }

        // 立刻进入下一秒的调度
        TIMER = setTimeout(runScheduler, 1000);
        window.__C2S_TIMER = TIMER;
        return;
      }

      // —— 未到点：只更新倒计时、并按 5s 节流打印日志；≤10s 时改为每秒 —— //
      var leftSec = Math.floor(leftMs / 1000);
      window.C2S_Viewer.tick(NEXT_AT);

      var needLog = (leftSec <= 10) || (now() - LAST_LOG_TS >= LOG_STEP_MS);
      if (needLog){
        console.log('[Loop] next refresh in', leftSec, 's');
        LAST_LOG_TS = now();
      }

      TIMER = setTimeout(runScheduler, 1000);
      window.__C2S_TIMER = TIMER;
    }

    // —— 启动调度：首轮标记为“到期”，立即执行 —— //
    NEXT_AT = 0;
    if (TIMER) { clearTimeout(TIMER); TIMER = null; window.__C2S_TIMER = null; }
    runScheduler();

  }

  // 唯一入口
  window.C2S_main_loop = main_loop;
  
  // 一次性初始化入口：只做一次抓取/POST（用于 startC2S(...,1) 的一键初始化）
  async function run_once_init() {
    console.log('[C2S.runOnceInit] one-shot strict init start');
    const root = (typeof unsafeWindow !== 'undefined' ? unsafeWindow : window);
    const C2S = root.C2S || (root.C2S = {});
    const U = C2S.util || {};

    const meta = root.__C2S_META__ || {};
    const user = String(meta.user || '').trim();
    const geo  = String(meta.geo || '').trim();
    const sign = String(meta.sign || '').trim();
    if (!user || !geo || !sign) { console.warn('[C2S.runOnceInit] meta incomplete'); return; }

    const rows = Array.isArray(root.__accountRows) ? root.__accountRows : [];
    if (!rows.length) { console.warn('[C2S.runOnceInit] __accountRows empty'); return; }

    const billingSet = (typeof U.buildBillingSet === 'function') ? U.buildBillingSet(rows) : new Set();
    if (!billingSet || billingSet.size === 0) { console.log('[C2S.runOnceInit] no billing accounts found'); return; }

    const baseline_blocks_all = [];

    for (const r of rows) {
      try {
        const acc = (typeof U.normalizeAccountId === 'function') ? U.normalizeAccountId(r) : String(r.account || r.account_id || r.acc || r.id || '').replace(/^act_?/i,'');
        if (!acc) continue;
        if (!billingSet.has(acc)) continue;
        const actId = acc.startsWith('act_') ? acc : ('act_' + acc);

        // 1) 尝试获取 lifetime insights
        let insights = [];
        try { insights = await fetchCampaignInsightsLifetime(actId); } catch (e) { insights = []; }

        // 2) 若没有返回 insights，则回退到 campaigns 列表，生成零值 baseline
        if (!Array.isArray(insights) || insights.length === 0) {
          try {
            const camps = await fetchAllCampaigns(actId);
            insights = camps.map(c => ({ campaign_id: c.id, campaign_name: c.name || '', spend: 0, impressions: 0, clicks: 0, results: 0, cost_per_result: 0, actions: [], currency: c.currency || 'USD' }));
          } catch (e) {
            insights = [];
          }
        }

        // 3) 转换为 baseline_blocks 项（不做任何差分）
        for (const row of (insights || [])) {
          const campaignId = String(row.campaign_id || row.campaign || row.id || '').trim();
          if (!campaignId) continue;
          const entry = {
            account_id: acc,
            campaign_id: campaignId,
            campaign_name: String(row.campaign_name || row.name || ''),
            spend: num(row.spend),
            results: readResultsField(row.results),
            clicks: num(row.clicks),
            impressions: num(row.impressions),
            comments: (Array.isArray(row.actions) ? row.actions.filter(a=>a.action_type==='comment').reduce((s,a)=>s+num(a.value),0) : 0),
            cpr: readCprField(row.cost_per_result),
            budget: 0,
            currency: String(row.currency || 'USD'),
            updated_at: formatLocalTs(new Date())
          };
          baseline_blocks_all.push(entry);
        }
      } catch (e) {
        console.warn('[C2S.runOnceInit] account processing failed:', e && e.message || e);
      }
    }

    if (!baseline_blocks_all.length) { console.warn('[C2S.runOnceInit] no baseline rows collected'); return; }

    const pack = { meta: { user, geo, sign }, is_init: true, baseline_blocks: baseline_blocks_all };
    try {
      if (C2S.poster && typeof C2S.poster.post === 'function') {
        console.log('[C2S.runOnceInit] posting baseline to GAS, rows=', baseline_blocks_all.length);
        await C2S.poster.post(pack);
        root.__BILLING_INITED__ = true;
      } else {
        const url = root.__GAS_POST_URL__ || '';
        if (!url) throw new Error('GAS URL missing');
        const bridge = root.__C2S_POST_BRIDGE__;
        if (typeof bridge === 'function') { await bridge(url, pack); root.__BILLING_INITED__ = true; }
        else { const resp = await fetch(url, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(pack)}); if (resp.ok) root.__BILLING_INITED__ = true; }
      }
      console.log('[C2S.runOnceInit] done, baseline rows=', baseline_blocks_all.length);
    } catch (e) {
      console.warn('[C2S.runOnceInit] post error:', e && e.message || e);
    }
  }
  window.C2S_run_once_init = run_once_init;
})();


/* ========= X) C2S.util：结算/聚合/账号工具 ========= */

// ===== 工具：账号规范化 & 布尔判断 =====
function _acc(v) { return String(v ?? "").replace(/^act_?/i, "").trim(); }
function _truthy(v) {
  const s = String(v ?? "").trim().toLowerCase();
  return s === "1" || s === "true" || s === "y" || s === "yes" || s === "是";
}
function _num(v) {
  const n = Number(String(v ?? "").replace(/[^\d.\-]/g, ""));
  return Number.isFinite(n) ? n : 0;
}





(function initC2SUtil(){
  const root = (typeof unsafeWindow !== 'undefined' ? unsafeWindow : window);
  const C2S = root.C2S || (root.C2S = {});

  // ---- 基础工具 ----
  function parseNumberSafe(v, fallback = 0) {
    if (v === null || v === undefined) return fallback;
    if (typeof v === 'number' && Number.isFinite(v)) return v;
    if (typeof v === 'string') {
      const s = v.trim().replace(/,/g, '');
      const n = Number(s);
      return Number.isFinite(n) ? n : fallback;
    }
    return fallback;
  }

  function clone(obj) {
    try { return structuredClone(obj); } catch(_) { return JSON.parse(JSON.stringify(obj || {})); }
  }

  function sumField(rows, key) {
    if (!Array.isArray(rows) || !key) return 0;
    let sum = 0;
    for (const r of rows) sum += parseNumberSafe(r?.[key], 0);
    return sum;
  }

  // 账号ID标准化：去掉 act_ 前缀
  function normalizeAccountId(block) {
    const id = (block && (block.account_id || block.acc || block.id)) || '';
    if (!id) return '';
    return String(id).replace(/^act_?/i, '');
  }

  // 侦测币种：优先 block.currency -> rows[0].currency -> 默认 "USD"
  function detectCurrency(block) {
    if (!block) return 'USD';
    if (block.currency) return String(block.currency);
    const r0 = Array.isArray(block.rows) && block.rows.length ? block.rows[0] : null;
    if (r0 && r0.currency) return String(r0.currency);
    return 'USD';
  }

  // 从 block.rows 聚合出 T0（本轮原始值，逐字段相加）
  // 数值字段自动发现：扫描所有行的可数值字段汇总
  function t0FromBlock(block) {
    const rows = Array.isArray(block?.rows) ? block.rows : [];
    const t0 = { account_id: normalizeAccountId(block), currency: detectCurrency(block) };
    if (!rows.length) return t0;

    // 收集候选的数值字段（排除明显非数值/标识类）
    const NON_NUM_KEYS = new Set(['account_id','acc','id','name','campaign','adset','ad','currency','date','time','ts','type']);
    const numericKeys = new Set();
    for (const r of rows) {
      if (!r || typeof r !== 'object') continue;
      for (const k of Object.keys(r)) {
        if (NON_NUM_KEYS.has(k)) continue;
        const val = r[k];
        if (val === null || val === undefined) continue;
        // 只把“可解析为数值”的键纳入
        const n = parseNumberSafe(val, NaN);
        if (!Number.isNaN(n)) numericKeys.add(k);
      }
    }
    // 聚合
    for (const key of numericKeys) {
      t0[key] = sumField(rows, key);
    }
    return t0;
  }

  // 逐字段差值：t0 - h（允许为负，不做裁剪）
  // 除 account_id/currency 外，其他被识别为数值的键都做差
  function calcBillingC(t0, h) {
    const out = clone(t0);
    const NON_NUM_KEYS = new Set(['account_id','currency']);
    const keys = new Set([...Object.keys(t0 || {}), ...Object.keys(h || {})]);
    for (const k of keys) {
      if (NON_NUM_KEYS.has(k)) continue;
      const a = parseNumberSafe(t0?.[k], 0);
      const b = parseNumberSafe(h?.[k], 0);
      out[k] = a - b;
    }
    return out;
  }

// ===== 从账号 rows 构建“账单户集合”（兼容中文/多种写法；不依赖 TM） =====
function buildBillingSet(accountRows) {
  const set = new Set();
  if (!Array.isArray(accountRows)) return set;

  for (const row of accountRows) {
    if (!row) continue;

    // ✅ 增加 row.account；去掉 act_ 前缀
    const acc = _acc(row.account || row.account_id || row.acc || row.id || row["账号"]);
    if (!acc) continue;

    const flag =
      row.is_billing ||
      row.billing ||
      row.bill ||
      row.billing_flag ||
      String(row.type || "").toLowerCase() === "billing" ||
      String(row.mode || "").toLowerCase() === "billing" ||
      _truthy(row["账单户"]) ||
      _truthy(row["是否账单户"]) ||
      _truthy(row["账单"]);

    if (flag) set.add(acc);
  }
  return set;
}


function buildHMap(accountRows) {
  const map = Object.create(null);
  if (!Array.isArray(accountRows)) return map;

  const zh2std = {
    "历史消耗": "spend",
    "历史成效": "results",
    "历史单价": "cpr",
    "历史点击": "clicks",
    "历史展示": "impressions",
    "历史评论": "comments",
    "币种":     "currency",
    "更新时间": "updated_at",
  };

  for (const row of accountRows) {
    if (!row) continue;

    // ✅ 增加 row.account
    const acc = _acc(row.account || row.account_id || row.acc || row.id || row["账号"]);
    if (!acc) continue;

    let h = null;

    if (row.history && typeof row.history === "object") {
      h = JSON.parse(JSON.stringify(row.history));
    } else if (row.H && typeof row.H === "object") {
      h = JSON.parse(JSON.stringify(row.H));
    } else {
      const hObj = {};

      // a) 直接英文列（你 rows 里就是这种：spend/results/clicks/...）
      if (row.spend != null)       hObj.spend       = _num(row.spend);
      if (row.results != null)     hObj.results     = _num(row.results);
      if (row.clicks != null)      hObj.clicks      = _num(row.clicks);
      if (row.impressions != null) hObj.impressions = _num(row.impressions);
      if (row.comments != null)    hObj.comments    = _num(row.comments);
      if (row.cpr != null)         hObj.cpr         = Number(row.cpr) || 0;

      // b) 英文前缀：h_*/hist_*（保留你原有逻辑）
      for (const k of Object.keys(row)) {
        if (/^(h_|hist_)/i.test(k)) {
          const pure = k.replace(/^(h_|hist_)/i, "");
          if (hObj[pure] == null) hObj[pure] = _num(row[k]);
        }
      }

      // c) 中文“历史*”（保留你原有逻辑）
      for (const [zh, std] of Object.entries(zh2std)) {
        if (Object.prototype.hasOwnProperty.call(row, zh) && hObj[std] == null) {
          const v = row[zh];
          if (std === "currency") {
            hObj.currency = String(v ?? "").trim() || String(row.currency || "USD");
          } else if (std === "updated_at") {
            hObj.updated_at = v;
          } else {
            hObj[std] = std === "cpr" ? Number(v) || 0 : _num(v);
          }
        }
      }

      if (Object.keys(hObj).length) h = hObj;
    }

    if (h) {
      h.account_id = acc;
      if (!h.currency) h.currency = String(row.currency || row["币种"] || "USD");
      if (h.results > 0 && !("cpr" in h)) h.cpr = h.spend / h.results;
      map[acc] = h;
    }
  }
  return map;
}










  // 从账单户 blocks 生成 baseline_blocks 与 baseline_map
  // baseline_blocks：[{account_id,currency, ...各数值字段(T0)}]
  // baseline_map：按 account+campaign key 定义的历史数据
  function buildBaselineFromBlocks(billingBlocks) {
    const baseline_blocks = [];
    const baseline_map = Object.create(null);
    if (!Array.isArray(billingBlocks)) return { baseline_blocks, baseline_map };

    for (const b of billingBlocks) {
      const acc = normalizeAccountId(b);
      if (!acc) continue;
      const rows = Array.isArray(b.rows) ? b.rows : [];
      for (const row of rows) {
        if (!row) continue;
        const campaignId = String(row.campaign_id || row.campaign || row.id || '').trim();
        if (!campaignId) continue;
        const key = `${acc}-${campaignId}`;
        const entry = {
          account_id: acc,
          campaign_id: campaignId,
          campaign_name: String(row.campaign_name || row.name || '') ,
          spend: num(row.spend),
          results: num(row.results),
          clicks: num(row.clicks),
          impressions: num(row.impressions),
          comments: num(row.comments),
          cpr: num(row.cpr),
          budget: num(row.budget),
          currency: String(row.currency || detectCurrency(b) || 'USD'),
        };
        if (row.updated_at) entry.updated_at = row.updated_at;
        baseline_blocks.push(entry);
        baseline_map[key] = clone(entry);
      }
    }
    return { baseline_blocks, baseline_map };
  }

  function buildCampaignHistoryMap(rows) {
    const map = Object.create(null);
    if (!Array.isArray(rows)) return map;
    for (const r of rows) {
      if (!r) continue;
      const acc = normalizeAccountId({ account_id: r.account || r.account_id || r.account_num || r.acc });
      const campaignId = String(r.campaign_id || r.campaign || '').trim();
      if (!acc || !campaignId) continue;
      const key = `${acc}-${campaignId}`;
      map[key] = {
        account_id: acc,
        campaign_id: campaignId,
        campaign_name: String(r.campaign_name || r.name || r["系列名称"] || ""),
        spend: num(r.spend),
        results: num(r.results),
        clicks: num(r.clicks),
        impressions: num(r.impressions),
        comments: num(r.comments),
        cpr: num(r.cpr),
        currency: String(r.currency || r["币种"] || "USD") || "USD",
        updated_at: r.updated_at || r["更新时间"] || ""
      };
    }
    return map;
  }

  // 暴露
  function toCampaignHistoryMapFromBaseline(baseline_blocks) {
    const map = Object.create(null);
    for (const b of (baseline_blocks || [])) {
      const acc = normalizeAccountId(b);
      const campaignId = String(b?.campaign_id || b?.campaign || b?.campaign_id || '').trim();
      if (!acc || !campaignId) continue;
      map[`${acc}-${campaignId}`] = { ...b, account_id: acc, campaign_id: campaignId };
    }
    return map;
  }

  C2S.util = {
    parseNumberSafe,
    sumField,
    normalizeAccountId,
    detectCurrency,
    t0FromBlock,
    calcBillingC,
    buildBillingSet,
    buildHMap,
    buildBaselineFromBlocks,
    buildCampaignHistoryMap,
    toCampaignHistoryMapFromBaseline
  };
  Object.freeze(C2S.util);
})();


/* ========= Y) C2S.core：统一流水线（每轮同一流程） ========= */
(function initC2SCore(){
  const root = (typeof unsafeWindow !== 'undefined' ? unsafeWindow : window);
  const C2S = root.C2S || (root.C2S = {});
  const U   = C2S.util;

  const state = {
    inited: false,
    BILLING_SET: new Set(),
    H_CACHE: Object.create(null),     // acc -> H 对象
    CAMPAIGN_HISTORY_MAP: Object.create(null), // acc+campaign -> history
    lastMeta: null                    // {user, geo, sign}
  };

  function readMeta() {
    const m = root.__C2S_META__ || {};
    return {
      user: String(m.user || '').trim(),
      geo:  String(m.geo  || '').trim(),
      sign: String(m.sign || '').trim()
    };
  }

  // 仅在首次生效：从账号表构建账单户集合与 H 映射
  function initOnce() {
    if (state.inited) return;
    state.lastMeta = readMeta();
    try {
      const rows = root.__accountRows || root.__account_rows || [];


// ===== [PATCH] 补齐 C2S.util 的两个函数（就地覆盖），确保能识别 row.account 与 billing=1/true =====
(function ensureUtilForBillingAndH(){
  const U = C2S.util || (C2S.util = {});

  // 小工具
  function _acc(v){
    return String(v == null ? "" : v).replace(/^act_?/i, "").trim();
  }
  function _truthy(v){
    const s = String(v == null ? "" : v).trim().toLowerCase();
    return v === true || v === 1 || s === "1" || s === "true" || s === "y" || s === "yes" || s === "是";
  }
  function _num(v){
    const n = Number(String(v == null ? "" : v).replace(/[^\d.\-]/g,""));
    return Number.isFinite(n) ? n : 0;
  }

  // 覆盖/定义：账单户集合
  U.buildBillingSet = function(rows){
    const set = new Set();
    if (!Array.isArray(rows)) return set;
    for (const r of rows){
      if (!r) continue;
      // ✅ 支持 row.account（你的 rows 用这个键）
      const acc = _acc(r.account || r.account_id || r.acc || r.id || r["账号"]);
      if (!acc) continue;
      // ✅ 兼容 1/true 以及中文
      const raw = (r.billing !== undefined ? r.billing : undefined)
               ?? (r.is_billing !== undefined ? r.is_billing : undefined)
               ?? (r.bill !== undefined ? r.bill : undefined)
               ?? (r.billing_flag !== undefined ? r.billing_flag : undefined)
               ?? r["账单户"] ?? r["是否账单户"] ?? r["账单"];
      const isBilling = _truthy(raw)
        || String(r.type||"").toLowerCase()==="billing"
        || String(r.mode||"").toLowerCase()==="billing";
      if (isBilling) set.add(acc);
    }
    return set;
  };

  // 覆盖/定义：把 rows 里的“历史* / 英文字段”拼成 H
  U.buildHMap = function(rows){
    const map = Object.create(null);
    if (!Array.isArray(rows)) return map;

    const zh2std = {
      "历史消耗":"spend",
      "历史成效":"results",
      "历史单价":"cpr",
      "历史点击":"clicks",
      "历史展示":"impressions",
      "历史评论":"comments",
      "币种":"currency",
      "更新时间":"updated_at"
    };

    for (const r of rows){
      if (!r) continue;
      const acc = _acc(r.account || r.account_id || r.acc || r.id || r["账号"]);
      if (!acc) continue;

      let h = null;

      // 1) 直接对象
      if (r.history && typeof r.history === "object"){
        h = JSON.parse(JSON.stringify(r.history));
      } else if (r.H && typeof r.H === "object"){
        h = JSON.parse(JSON.stringify(r.H));
      } else {
        const o = {};

        // 2) ✅ 英文直接列（你 rows 里就是这种）
        if (r.spend       !== undefined) o.spend       = _num(r.spend);
        if (r.results     !== undefined) o.results     = _num(r.results);
        if (r.clicks      !== undefined) o.clicks      = _num(r.clicks);
        if (r.impressions !== undefined) o.impressions = _num(r.impressions);
        if (r.comments    !== undefined) o.comments    = _num(r.comments);
        if (r.cpr         !== undefined) o.cpr         = Number(r.cpr) || 0;
        if (r.currency    !== undefined) o.currency    = String(r.currency||"").trim();
        if (r.updated_at  !== undefined) o.updated_at  = r.updated_at;

        // 3) 英文前缀 h_*/hist_*
        for (const k of Object.keys(r)){
          if (/^(h_|hist_)/i.test(k)){
            const pure = k.replace(/^(h_|hist_)/i,"");
            if (o[pure] === undefined) o[pure] = _num(r[k]);
          }
        }

        // 4) 中文“历史*”
        for (const [zh,std] of Object.entries(zh2std)){
          if (Object.prototype.hasOwnProperty.call(r, zh) && o[std] === undefined){
            const v = r[zh];
            if (std === "currency"){
              o.currency = String(v||"").trim() || String(r.currency || "USD");
            } else if (std === "updated_at"){
              o.updated_at = v;
            } else if (std === "cpr"){
              o.cpr = Number(v) || 0;
            } else {
              o[std] = _num(v);
            }
          }
        }

        if (Object.keys(o).length) h = o;
      }

      if (h){
        h.account_id = acc;
        if (!h.currency) h.currency = String(r.currency || r["币种"] || "USD");
        if (h.results > 0 && (h.cpr === undefined || h.cpr === null)) h.cpr = h.spend / h.results;
        map[acc] = h;
      }
    }
    return map;
  };

})(); // ===== [/PATCH] =====



      
      state.BILLING_SET = U.buildBillingSet(rows);
      state.H_CACHE     = U.buildHMap(rows);
      const campaignRows = Array.isArray(root.__C2S_CAMPAIGN_HISTORY__) ? root.__C2S_CAMPAIGN_HISTORY__ : [];
      state.CAMPAIGN_HISTORY_MAP = U.buildCampaignHistoryMap(campaignRows);

      // ⬇️ 新增：init 轮构建 baseline_blocks
      const dto = root.__C2S_DTO || {};
      const isBillingMode = Number(root.__BILLING_MODE__ || 0) === 1;
      const isInitedFlag  = Boolean(root.__BILLING_INITED__);
      const is_init_round = isBillingMode && !isInitedFlag;

      if (is_init_round) {
        // 从账单户 blocks_raw 生成 baseline_blocks，写入 state
        const blocks_raw = Array.isArray(dto.blocks) ? dto.blocks : [];
        const billing_blocks = blocks_raw.filter(b => {
          const acc = U.normalizeAccountId(b);
          return acc && state.BILLING_SET.has(acc);
        });
        const { baseline_blocks, baseline_map } = U.buildBaselineFromBlocks(billing_blocks);
        state.baseline_blocks = baseline_blocks;
        state.baseline_map = baseline_map;
        state.CAMPAIGN_HISTORY_MAP = baseline_map;
      }

      state.inited = true;
      console.log('[C2S/core] initOnce:',
        'billing_set=' + state.BILLING_SET.size + ',',
        'H.size=' + Object.keys(state.H_CACHE).length + ',',
        'baseline=' + ((state.baseline_blocks && state.baseline_blocks.length) || 0)
      );
    } catch (e) {
      console.warn('[C2S/core] initOnce error:', e);
      state.inited = true;
    }
  }


  // —— 主监听：每轮抓完触发（依赖你已有的 dispatchEvent('C2S:DID_FETCH')）——
  function onDidFetch() {
    initOnce();

    // 读入 dto（保持与现有约定一致）
    const dto   = root.__C2S_DTO || {};
    const blocks_raw = Array.isArray(dto.blocks) ? dto.blocks : [];
    const range = dto.range || {};
    const grand = dto.grand || {};

    // 账单模式与 init 判断
    const isBillingMode = Number(root.__BILLING_MODE__ || 0) === 1;
    const isInitedFlag  = Boolean(root.__BILLING_INITED__); // init 成功后会置为 true（第5步处理）
    const is_init_round = isBillingMode && !isInitedFlag;

    // 1) 计算每个 block 的 T0（按系列差值），区分账单户/普通户
    const blocks_c = [];
    const billing_blocks_this_round = []; // 仅账单户的原始块，用于 init 生成 baseline
    const campaignHistoryMap = state.CAMPAIGN_HISTORY_MAP || Object.create(null);
    for (const b of blocks_raw) {
      const acc = U.normalizeAccountId(b);
      if (!acc) continue;
      const rows = Array.isArray(b.rows) ? b.rows : [];
      const isBillingAcc = state.BILLING_SET.has(acc);
      const totals = { spend:0, results:0, clicks:0, impressions:0, comments:0 };
      for (const row of rows) {
        if (!row) continue;
        const campaignId = String(row.campaign_id || row.campaign || row.id || '').trim();
        if (!campaignId) continue;
        const historyKey = `${acc}-${campaignId}`;
        const historyForCalc = (isBillingAcc && !is_init_round)
          ? (campaignHistoryMap[historyKey] || {})
          : row;
        const diffRow = isBillingAcc ? U.calcBillingC(row, historyForCalc) : row;
        totals.spend       += Number(diffRow.spend     || 0);
        totals.results     += Number(diffRow.results   || 0);
        totals.clicks      += Number(diffRow.clicks    || 0);
        totals.impressions += Number(diffRow.impressions|| 0);
        totals.comments    += Number(diffRow.comments  || 0);
      }
      const total = {
        spend: totals.spend,
        results: totals.results,
        clicks: totals.clicks,
        impressions: totals.impressions,
        comments: totals.comments,
        cpr: totals.results > 0 ? (totals.spend / totals.results) : 0
      };
      total.account_id = acc;
      total.currency = total.currency || U.detectCurrency(b);
      if (isBillingAcc) billing_blocks_this_round.push(b);
      blocks_c.push(total);
    }

    // 2) 若是 init 轮：构造 baseline
    let baseline_blocks = null;
    let baseline_map    = null;
    if (is_init_round) {
      const { baseline_blocks: blks, baseline_map: bmap } =
        U.buildBaselineFromBlocks(billing_blocks_this_round);
      baseline_blocks = blks;
      baseline_map    = bmap;
    }

    // 3) 暴露给页面便于调试/后续 Poster 使用
    root.__C2S_C_BLOCKS__   = blocks_c;
    root.__C2S_LAST_CORE__  = {
      meta: readMeta(),
      is_init: is_init_round,
      range, grand,
      blocks_raw,
      blocks_c,
      baseline_blocks
    };

    // 4) 若 Poster 已就绪（第5步会注入），直接调用一次性 POST；否则静默等待第5步接管
    if (C2S.poster && typeof C2S.poster.post === 'function') {
      try {
        C2S.poster.post(root.__C2S_LAST_CORE__);
      } catch (e) {
        console.warn('[C2S/core] poster.post error:', e);
      }
    } else {
      // 预留：Poster 未注入前仅记录
      console.log('[C2S/core] tick (poster pending):',
        'init=', is_init_round, 'blocks_c=', blocks_c.length);
    }
  }

  // 绑定事件监听（只绑定一次）
  try {
    // 你的代码里应在抓完数据后派发 document.dispatchEvent(new Event('C2S:DID_FETCH'))
    document.addEventListener('C2S:DID_FETCH', onDidFetch, { passive: true });
    console.log('[C2S/core] listener attached: C2S:DID_FETCH');
  } catch (e) {
    console.warn('[C2S/core] failed to attach listener:', e);
  }

  C2S.core = { onDidFetch, initOnce, state };
  Object.freeze(C2S.core);
})();



/* ========= Z) C2S.poster：一次性 POST 到 GAS（修正版：使用结算后的 totals） ========= */
(function initC2SPoster(){
  const root = (typeof unsafeWindow !== 'undefined' ? unsafeWindow : window);
  const C2S  = root.C2S || (root.C2S = {});

  // —— GAS URL 读取 —— 
  function getGasUrl() {
    return root.__GAS_POST_URL__ || root.__GAS_URL__ || root.__GAS_ENDPOINT__ || '';
  }

  // —— 统一 POST（优先 GM 桥，降级 fetch）——
  async function postJSON(url, payload) {
    if (!url) throw new Error('GAS URL missing');
    const bridge = root.__C2S_POST_BRIDGE__;
    if (typeof bridge === 'function') {
      return await bridge(url, payload);
    }
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json;charset=utf-8' },
      body: JSON.stringify(payload),
      credentials: 'omit',
      cache: 'no-store',
    });
    let data = null; try { data = await res.json(); } catch(_) {}
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return data || { ok: true };
  }

  // —— 幂等：同一轮只发一次 —— 
  const postedTags = new Set();
  function markOnce(pack) {
    if (!pack.__post_tag) pack.__post_tag = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    if (postedTags.has(pack.__post_tag)) return false;
    postedTags.add(pack.__post_tag);
    return true;
  }

  // —— 用 blocks_c 覆盖 raw 的 total（账单户在 init=0；非 init=差值；普通户=T0）——
  function normalizeAcc(v){ return String(v||'').replace(/^act_?/i,''); }
  function cookBlocks(rawBlocks, cookedList){
    const byAcc = Object.create(null);
    for (const c of (cookedList||[])) {
      const acc = normalizeAcc(c.account_id);
      if (!acc) continue;
      const spend       = Number(c.spend||0);
      const results     = Number(c.results||0);
      const clicks      = Number(c.clicks||0);
      const impressions = Number(c.impressions||0);
      const comments    = Number(c.comments||0);
      const cpr         = results > 0 ? (spend / results) : 0;
      byAcc[acc] = { spend, results, clicks, impressions, comments, cpr };
    }
    return (rawBlocks||[]).map(b=>{
      const acc = normalizeAcc(b.account_id);
      const t = byAcc[acc];
      if (!t) return b;
      return { ...b, total: { ...t } };
    });
  }

  // —— 把 baseline_blocks 转成 H 映射（acc -> baseline 对象）——
  function toHMapFromBaseline(baseline_blocks) {
    const map = Object.create(null);
    for (const b of (baseline_blocks || [])) {
      const acc = normalizeAcc(b?.account_id);
      if (!acc) continue;
      map[acc] = { ...b, account_id: acc };
    }
    return map;
  }

  function toCampaignHistoryMapFromBaseline(baseline_blocks) {
    const map = Object.create(null);
    for (const b of (baseline_blocks || [])) {
      const acc = normalizeAcc(b?.account_id);
      const campaignId = String(b?.campaign_id || '').trim();
      if (!acc || !campaignId) continue;
      map[`${acc}-${campaignId}`] = { ...b, account_id: acc, campaign_id: campaignId };
    }
    return map;
  }

  async function post(pack) {
    try {
      if (!pack || typeof pack !== 'object') return;
      if (!markOnce(pack)) return;

      const meta = pack.meta || {};
      const url  = getGasUrl();
      if (!meta.user || !meta.geo || !meta.sign) {
        console.warn('[C2S/poster] missing meta(user|geo|sign), skip this round');
        return;
      }
      if (!url) {
        console.warn('[C2S/poster] GAS URL missing, skip this round');
        return;
      }

      // ✅ 关键：用“已结算”的 blocks_c 覆盖 raw 的 total
      const blocks_for_post = cookBlocks(pack.blocks_raw || [], pack.blocks_c || []);

      const payload = {
        user: meta.user,
        geo:  meta.geo,
        sign: meta.sign,
        blocks: blocks_for_post,
        range:  pack.range || {},
        grand:  pack.grand || {},
        ts_client: formatLocalTs(new Date())
      };

      // init 首轮：携带 baseline_blocks 供 GAS 写 H
      if (pack.is_init && Array.isArray(pack.baseline_blocks)) {
        payload.billing_mode  = 1;
        payload.billing_stage = 'init';
        payload.baseline_blocks = pack.baseline_blocks;
      }

      const resp = await postJSON(url, payload);
      console.log('[C2S/poster] POST done:', { init: !!pack.is_init, resp });

      if (pack.is_init) {
        try {
          // 本地也固化 H，避免第2轮前取不到
          const Hmap = toHMapFromBaseline(pack.baseline_blocks || []);
          if (C2S.core && C2S.core.state && Hmap) {
            C2S.core.state.H_CACHE = Hmap;
            C2S.core.state.CAMPAIGN_HISTORY_MAP = toCampaignHistoryMapFromBaseline(pack.baseline_blocks || []);
          }
          root.__BILLING_INITED__ = true;
          console.log('[C2S/poster] init finalized: H updated, __BILLING_INITED__ = true');
        } catch (e) {
          console.warn('[C2S/poster] finalize init error:', e);
        }
      }
    } catch (e) {
      console.warn('[C2S/poster] post error:', e);
    }
  }

  C2S.poster = { post };
  Object.freeze(C2S.poster);

  // 若 Core 已经跑出一轮，补打一发
  try { if (root.__C2S_LAST_CORE__) { post(root.__C2S_LAST_CORE__); } }
  catch (e) { console.warn('[C2S/poster] late-post error:', e); }
})();



/* ========= 7) 启动交由 TM/手动 ========= */
window.__C2S_LOOP_STARTED__ = !!window.__C2S_LOOP_STARTED__;
