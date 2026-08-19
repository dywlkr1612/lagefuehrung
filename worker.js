/* =============================================================
   LAGEFÜHRUNG – Cloud-Variante (Cloudflare Worker + Durable Object)
   -------------------------------------------------------------
   Standortübergreifender Betrieb über das Internet. Eine gemeinsame
   Lage (kein Mehrraum-System), ohne Zugangsschutz.

   Der Worker leitet alle Anfragen an EIN Durable Object ("die Lage").
   Das Durable Object:
     - hält den gemeinsamen Lage-Zustand
     - verteilt Änderungen per WebSocket an alle verbundenen Arbeitsplätze
     - speichert den Zustand dauerhaft im Durable-Object-Storage
     - stellt ein Handy-Meldeformular unter /melden bereit

   Deploy:  wrangler deploy    (siehe README_cloud.md)
   ============================================================= */

/* ---------- Worker: routet alles an das eine Durable Object ---------- */
export default {
  async fetch(request, env) {
    // Feste, einzelne Instanz -> eine gemeinsame Lage für alle
    const id = env.LAGE.idFromName('einzige-lage');
    const stub = env.LAGE.get(id);
    return stub.fetch(request);
  }
};

/* ---------- Durable Object: der zentrale Lagezustand ---------- */
export class LageRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sessions = new Set();   // aktive WebSocket-Verbindungen {ws, role}
    this.STATE = null;           // im Speicher gehaltener Lagezustand
    this.saveTimer = null;
    // Zustand beim ersten Zugriff aus dem Storage laden
    this.state.blockConcurrencyWhile(async () => {
      const stored = await this.state.storage.get('lage');
      this.STATE = stored ? normalize(stored) : initialState();
    });
  }

  async fetch(request) {
    const url = new URL(request.url);

    // WebSocket-Verbindung
    if (url.pathname === '/ws') {
      if (request.headers.get('Upgrade') !== 'websocket') {
        return new Response('WebSocket erwartet', { status: 426 });
      }
      const pair = new WebSocketPair();
      const client = pair[0], server = pair[1];
      this.handleSession(server);
      return new Response(null, { status: 101, webSocket: client });
    }

    // Meldeformular (Handy): GET zeigt Formular, POST nimmt Meldung entgegen
    if (url.pathname === '/melden' && request.method === 'GET') {
      return html(MELDE_FORM);
    }
    if (url.pathname === '/melden' && request.method === 'POST') {
      return this.handleMelden(request);
    }

    // Debug/Backup: aktueller Zustand als JSON
    if (url.pathname === '/state') {
      return new Response(JSON.stringify(this.STATE, null, 2),
        { headers: { 'Content-Type': 'application/json; charset=utf-8' } });
    }

    // Oberfläche ausliefern
    if (url.pathname === '/' || url.pathname === '/index.html' || url.pathname.startsWith('/lagedarstellung')) {
      return html(PAGE_HTML);
    }

    return new Response('not found', { status: 404 });
  }

  /* ----- WebSocket-Sitzung ----- */
  handleSession(ws) {
    ws.accept();
    const session = { ws, role: null };
    this.sessions.add(session);

    // aktuellen Gesamtzustand an neuen Client senden
    this.sendTo(session, { t: 'state', state: { __full: this.STATE } });

    ws.addEventListener('message', (evt) => {
      let msg;
      try { msg = JSON.parse(evt.data); } catch (e) { return; }
      this.handleMessage(session, msg);
    });
    const drop = () => { this.sessions.delete(session); };
    ws.addEventListener('close', drop);
    ws.addEventListener('error', drop);
  }

  handleMessage(session, msg) {
    if (msg.t === 'hello') { session.role = msg.role || 'fg'; return; }
    if (msg.t === 'mut' && msg.op === 'full') {
      const incoming = msg.payload && msg.payload.state;
      if (!incoming) return;
      this.mergeState(session.role, incoming);
      this.scheduleSave();
      this.broadcast({ t: 'state', state: { __full: this.STATE } });
    }
  }

  /* ----- Meldeformular-Eingang ----- */
  async handleMelden(request) {
    let name = '', text = '', art = 'meldung';
    try {
      const ct = request.headers.get('content-type') || '';
      if (ct.indexOf('application/json') >= 0) {
        const j = await request.json();
        name = (j.name || '').toString(); text = (j.text || '').toString(); if (j.art) art = j.art;
      } else {
        const form = await request.formData();
        name = (form.get('name') || '').toString();
        text = (form.get('text') || '').toString();
        if (form.get('art')) art = form.get('art').toString();
      }
    } catch (e) {}
    name = name.trim().slice(0, 60);
    text = text.trim().slice(0, 1000);
    if (!text) return html(meldePage('Bitte einen Meldungstext eingeben.', false), 400);

    const by = name ? ('📱 ' + name) : '📱 Meldung';
    const entry = {
      id: 'L' + Date.now() + '_' + Math.floor(Math.random() * 1e6),
      ts: hhmm(), by, type: (art === 'lage' ? 'lage' : 'meldung'),
      text, prio: false, done: false, decision: null
    };
    this.STATE.log.unshift(normLog(entry));
    this.scheduleSave();
    this.broadcast({ t: 'state', state: { __full: this.STATE } });
    return html(meldePage('Meldung übermittelt. Vielen Dank.', true));
  }

  /* ----- Rollenbasierter Merge (identisch zur Server-Variante) ----- */
  mergeState(role, inc) {
    if (role === 'fg' || !role) { this.STATE = normalize(inc); return; }
    const ea = role;
    const incEA = (inc.plan && inc.plan.eas || []).find(e => e.name === ea);
    this.STATE.plan.eas = this.STATE.plan.eas.map(e => (e.name === ea && incEA) ? normEA(incEA) : e);
    const incUnits = (inc.units || []).filter(u => u.ea === ea);
    const keepUnits = this.STATE.units.filter(u => u.ea !== ea);
    this.STATE.units = keepUnits.concat(incUnits.map(normUnit));
    this.STATE.markers = (inc.markers || []).map(normUnit);
    const known = new Set(this.STATE.log.map(sig));
    (inc.log || []).forEach(e => { if (!known.has(sig(e))) this.STATE.log.unshift(normLog(e)); });
    if (inc.seq && inc.seq > this.STATE.seq) this.STATE.seq = inc.seq;
  }

  /* ----- Verteilen an alle verbundenen Clients ----- */
  broadcast(obj) {
    const txt = JSON.stringify(obj);
    for (const s of [...this.sessions]) {
      try { s.ws.send(txt); } catch (e) { this.sessions.delete(s); }
    }
  }
  sendTo(session, obj) {
    try { session.ws.send(JSON.stringify(obj)); } catch (e) { this.sessions.delete(session); }
  }

  /* ----- Persistenz (leicht entzerrt) ----- */
  scheduleSave() {
    if (this.saveTimer) return;
    this.saveTimer = setTimeout(async () => {
      this.saveTimer = null;
      try { await this.state.storage.put('lage', this.STATE); } catch (e) {}
    }, 800);
  }
}

/* ================= gemeinsame Hilfsfunktionen ================= */
function initialState() {
  return {
    op: { name: 'Übungslage', section: '', anlass: '', pf: '' },
    plan: { eas: [
      { id: 'ea1', name: 'EA 1', fields: {}, staerke: { h: 0, g: 0, m: 0 }, uas: [] },
      { id: 'ea2', name: 'EA 2', fields: {}, staerke: { h: 0, g: 0, m: 0 }, uas: [] }
    ], statusCols: ['moz','eoz','ds_begonnen','zp_angetroffen','zp_festgenommen','objekt_verlassen','entlassen'] },
    units: [], markers: [], log: [],
    opStarted: false, opStartTime: null,
    seq: 1, planSeq: 3
  };
}
function normalize(inc) {
  return {
    op: Object.assign({ name: 'Übungslage', section: '', anlass: '', pf: '' }, inc.op || {}),
    plan: { eas: (inc.plan && inc.plan.eas || []).map(normEA), statusCols: (inc.plan && Array.isArray(inc.plan.statusCols)) ? inc.plan.statusCols.slice() : [] },
    units: (inc.units || []).map(normUnit),
    markers: (inc.markers || []).map(normUnit),
    log: (inc.log || []).map(normLog),
    opStarted: !!inc.opStarted, opStartTime: inc.opStartTime || null,
    seq: inc.seq || 1, planSeq: inc.planSeq || 1
  };
}
function normLog(e) { return { id: e.id, ts: e.ts, by: e.by || '', type: e.type, text: e.text, prio: !!e.prio, done: !!e.done, decision: e.decision || null }; }
function normEA(e) {
  return { id: e.id, name: e.name, fields: e.fields || {}, staerke: e.staerke || { h: 0, g: 0, m: 0 },
    uas: (e.uas || []).map(u => ({ id: u.id, name: u.name, fields: u.fields || {}, staerke: u.staerke || { h: 0, g: 0, m: 0 } })) };
}
function normUnit(u) {
  return { id: u.id, type: u.type, name: u.name, label: u.label, call: u.call, status: u.status,
    ea: u.ea, ua: u.ua, auftrag: u.auftrag, s_f: u.s_f, s_u: u.s_u, s_m: u.s_m, beteiligte: u.beteiligte || 0,
    start: u.start, lat: u.lat, lng: u.lng, mobile: u.mobile };
}
function sig(e) { return (e.ts || '') + '|' + (e.type || '') + '|' + (e.text || ''); }
function hhmm() { const d = new Date(); return String(d.getHours()).padStart(2, '0') + ':' + String(d.getMinutes()).padStart(2, '0'); }

function html(body, status) {
  return new Response(body, { status: status || 200, headers: { 'Content-Type': 'text/html; charset=utf-8' } });
}
function meldePage(msg, ok) {
  return `<!DOCTYPE html><html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Meldung</title><style>body{font-family:system-ui,Arial,sans-serif;background:#0f1216;color:#e6e9ef;margin:0;display:flex;align-items:center;justify-content:center;min-height:100vh;padding:20px}
  .card{background:#161b22;border:1px solid #2a3140;border-radius:14px;padding:26px;max-width:420px;width:100%;text-align:center}
  .ic{font-size:40px}.msg{margin:14px 0 18px;font-size:16px}a{display:inline-block;background:#f4a300;color:#111;padding:11px 18px;border-radius:8px;text-decoration:none;font-weight:700}</style></head>
  <body><div class="card"><div class="ic">${ok ? '✅' : '⚠️'}</div><div class="msg">${msg}</div><a href="/melden">Neue Meldung</a></div></body></html>`;
}
const MELDE_FORM = `<!DOCTYPE html><html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Meldung an die Einsatzleitung</title>
<style>
  *{box-sizing:border-box}
  body{font-family:system-ui,Arial,sans-serif;background:#0f1216;color:#e6e9ef;margin:0;padding:18px;display:flex;justify-content:center}
  .wrap{width:100%;max-width:460px}
  h1{font-size:19px;margin:6px 0 2px}
  p.sub{color:#8b98a8;font-size:13px;margin:0 0 18px}
  label{display:block;font-size:12px;color:#8b98a8;margin:14px 0 5px}
  input,select,textarea{width:100%;background:#161b22;border:1px solid #2a3140;color:#e6e9ef;border-radius:9px;padding:12px;font-size:16px;font-family:inherit}
  input:focus,select:focus,textarea:focus{outline:none;border-color:#f4a300}
  textarea{min-height:130px;resize:vertical}
  button{width:100%;margin-top:18px;background:#f4a300;color:#111;border:none;border-radius:9px;padding:14px;font-size:16px;font-weight:800}
  .brand{display:flex;align-items:center;gap:9px;font-weight:800;letter-spacing:.4px}
  .dot{width:11px;height:11px;border-radius:2px;background:#f4a300;box-shadow:0 0 9px #f4a300}
  .hint{font-size:11px;color:#5a6270;margin-top:14px;line-height:1.5}
</style></head>
<body><div class="wrap">
  <div class="brand"><span class="dot"></span> LAGEFÜHRUNG</div>
  <h1>Meldung an die Einsatzleitung</h1>
  <p class="sub">Diese Meldung erscheint im Einsatztagebuch.</p>
  <form method="POST" action="/melden">
    <label>Dein Name / Rufname</label>
    <input name="name" maxlength="60" placeholder="z. B. Streife 12 / Müller" autocomplete="off">
    <label>Art</label>
    <select name="art"><option value="meldung">Meldung</option><option value="lage">Lagemeldung</option></select>
    <label>Meldungstext</label>
    <textarea name="text" maxlength="1000" placeholder="Was ist zu melden?" required></textarea>
    <button type="submit">Meldung senden</button>
  </form>
  <div class="hint">Hinweis: Bitte keine besonders sensiblen personenbezogenen Daten senden, wenn nicht ausdrücklich freigegeben. Die Meldung geht an die Einsatzleitung.</div>
</div></body></html>`;

/* Die Oberfläche wird beim Build hier eingesetzt (Platzhalter unten). */
const PAGE_HTML = `<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Lageführung – Einsatzführungs- und Lagedarstellungstool</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.css" />
<style>
  :root{
    --bg:#0d1117; --panel:#161b22; --panel-2:#1c2330; --line:#2a3140;
    --ink:#e6edf3; --ink-dim:#8b98a8; --accent:#e63946; --accent-2:#f4a300;
    --ok:#3fb950; --warn:#f4a300; --crit:#e63946; --info:#4a90d9;
    --mono:"JetBrains Mono",ui-monospace,"SFMono-Regular",Consolas,monospace;
    --sans:"Inter",-apple-system,system-ui,"Segoe UI",Roboto,sans-serif;
  }
  *{box-sizing:border-box}
  html,body{margin:0;height:100%;font-family:var(--sans);background:var(--bg);color:var(--ink);overflow:hidden}
  button{font-family:inherit;cursor:pointer}
  input,select{font-family:inherit}

  .app{display:grid;grid-template-rows:auto 1fr;height:100vh;min-height:0;position:relative}
  .app>header{grid-row:1;position:relative;z-index:100}
  /* Inhaltsbereich = Zeile 2. Alle Ansichten liegen deckungsgleich darin und
     füllen ihn absolut aus; sichtbar ist per display:none/block bzw. .hidden
     immer genau eine. Dieses Muster kann layouttechnisch nicht kollabieren. */
  .content{grid-row:2;grid-column:1;position:relative;min-height:0;overflow:hidden}
  .content>.view{position:absolute;inset:0;min-height:0}
  header{display:flex;align-items:center;gap:16px;padding:8px 14px;background:var(--panel);border-bottom:1px solid var(--line)}
  .brand{display:flex;align-items:center;gap:10px;font-weight:800;letter-spacing:.5px}
  .brand .dot{width:12px;height:12px;border-radius:2px;background:var(--accent);box-shadow:0 0 10px var(--accent)}
  .brand small{font-weight:500;color:var(--ink-dim);letter-spacing:0}
  .op-meta{display:flex;gap:18px;align-items:center;margin-left:8px;font-family:var(--mono);font-size:12px;color:var(--ink-dim)}
  .op-meta b{color:var(--ink)}
  .op-meta b[contenteditable]{border-bottom:1px dashed var(--line);padding:0 2px;cursor:text;outline:none}
  .op-meta b[contenteditable]:hover{border-bottom-color:var(--accent)}
  .op-meta b[contenteditable]:focus{border-bottom-color:var(--accent);background:var(--panel-2)}
  .tabs{display:flex;gap:6px;margin-left:8px}
  .tab{background:var(--panel-2);border:1px solid var(--line);color:var(--ink-dim);padding:6px 14px;border-radius:6px;font-size:13px;font-weight:600}
  .tab.active{color:var(--ink);border-color:var(--accent);background:rgba(230,57,70,.12)}
  .spacer{margin-left:auto}
  .hbtn{background:var(--panel-2);border:1px solid var(--line);color:var(--ink);padding:6px 12px;border-radius:6px;font-size:13px}
  .hbtn:hover{border-color:var(--accent)}
  .hbtn:disabled,.hbtn.role-locked{opacity:.45;cursor:not-allowed;filter:grayscale(.5)}
  .hbtn:disabled:hover,.hbtn.role-locked:hover{border-color:var(--line)}
  .clock{font-family:var(--mono);font-size:14px;background:var(--panel-2);padding:6px 12px;border-radius:6px;border:1px solid var(--line)}

  .view{position:absolute;inset:0;height:auto;min-height:0}
  /* Palette-Spalte ist einklappbar: Breite 0 wenn zu */
  #viewEinsatz{display:grid;grid-template-columns:var(--pal,0px) var(--rzl,0px) var(--cen,2fr) 6px var(--right,1fr);grid-template-rows:100%;height:100%;min-height:0;transition:none}
  #viewEinsatz.pal-open{--pal:230px;--rzl:6px}
  /* feste Spaltenzuordnung, damit die Bereiche unabhängig von der Sichtbarkeit in ihrer Spalte bleiben */
  #palcol{grid-column:1}
  #rzL{grid-column:2}
  #centerCol{grid-column:3}
  #rzR{grid-column:4}
  #rightCol{grid-column:5}
  #viewEinsatz:not(.pal-open) #palcol{visibility:hidden}
  #viewEinsatz:not(.pal-open) #rzL{visibility:hidden}
  .center{display:grid;grid-template-rows:var(--maprow,2fr) 6px var(--krow,1fr);min-height:0;min-width:0;height:100%;overflow:hidden}
  .resizer-col{background:var(--line);cursor:col-resize;width:100%;height:100%}
  .resizer-col:hover{background:var(--accent)}
  .resizer-row{background:var(--line);cursor:row-resize;height:100%;width:100%}
  .resizer-row:hover{background:var(--accent)}
  #rightCol{display:grid;grid-template-rows:var(--etbrow,1fr) 6px var(--hbrow,1fr);min-height:0;height:100%;overflow:hidden}
  #mapPane,#kraeftePane,#etbPane,#hbPane{min-height:0;overflow:hidden}
  #mapPane{grid-row:1} #rzM{grid-row:2} #kraeftePane{grid-row:3}
  #etbPane{grid-row:1} #rzETB{grid-row:2} #hbPane{grid-row:3}
  .rc-top,.rc-bot{display:flex;flex-direction:column;min-height:0;overflow:hidden;background:var(--panel)}
  body.resizing{cursor:inherit;user-select:none}
  body.resizing iframe,body.resizing #map{pointer-events:none}
  .col{background:var(--panel);overflow:hidden;display:flex;flex-direction:column;min-height:0}
  .bl{border-left:1px solid var(--line)} .br{border-right:1px solid var(--line)} .bt{border-top:1px solid var(--line)}
  .col h2{font-size:11px;text-transform:uppercase;letter-spacing:1.5px;color:var(--ink-dim);margin:0;padding:9px 14px 6px;display:flex;justify-content:space-between;align-items:center;gap:6px}
  .col h2 .h2act{background:var(--panel-2);border:1px solid var(--line);color:var(--ink-dim);border-radius:5px;font-size:11px;padding:3px 7px;font-weight:600;letter-spacing:0;text-transform:none}
  .col h2 .h2act:hover{border-color:var(--accent);color:var(--ink)}
  .scroll{overflow:auto;flex:1;min-height:0}
  .scroll::-webkit-scrollbar{width:8px;height:8px}
  .scroll::-webkit-scrollbar-thumb{background:var(--line);border-radius:4px}
  .palcol{min-width:0}

  #map{width:100%;height:100%;background:#0a0d12}
  .leaflet-container{background:#0a0d12;font-family:var(--sans)}
  .map-wrap{position:relative;min-height:0}
  .map-toolbar{position:absolute;top:10px;right:10px;left:auto;z-index:600;display:flex;gap:6px;background:rgba(22,27,34,.92);padding:6px;border-radius:8px;border:1px solid var(--line);flex-wrap:wrap;justify-content:flex-end;max-width:calc(100% - 20px)}
  .map-toolbar button{background:var(--panel-2);border:1px solid var(--line);color:var(--ink);height:34px;min-width:34px;padding:0 8px;border-radius:6px;font-size:15px;display:flex;align-items:center;justify-content:center;gap:6px}
  .map-toolbar button.active{background:var(--accent);border-color:var(--accent)}
  .map-toolbar .tglbtn{font-size:13px;font-weight:600}
  .map-search{display:flex;align-items:center;gap:0;margin-left:4px}
  .map-search input{background:var(--panel-2);border:1px solid var(--line);color:var(--ink);height:34px;padding:0 8px;border-radius:6px 0 0 6px;font-size:13px;width:180px}
  .map-search input:focus{outline:none;border-color:var(--accent)}
  .map-search button{background:var(--panel-2);border:1px solid var(--line);border-left:none;color:var(--ink);height:34px;width:36px;border-radius:0 6px 6px 0;font-size:14px}
  .map-search button:hover{border-color:var(--accent)}
  .search-results{position:absolute;top:56px;right:10px;left:auto;z-index:650;background:var(--panel);border:1px solid var(--line);border-radius:8px;max-width:340px;max-height:240px;overflow:auto;display:none}
  /* Tabellenansicht (statt Karte) */
  #tableView{position:absolute;inset:0;background:var(--bg);display:none;flex-direction:column;overflow:hidden;z-index:400}
  #mapPane.show-table #tableView{display:flex}
  #mapPane.show-table #map,#mapPane.show-table .search-results{visibility:hidden}
  .tv-scroll{flex:1;overflow:auto;padding:48px 14px 14px}
  .tv-abschnitt{margin-bottom:18px}
  .tv-abschnitt h3{font-size:13px;color:#4a90d9;margin:0 0 6px;font-weight:800}
  table.tv{width:100%;border-collapse:collapse;font-size:12px;table-layout:fixed}
  table.tv th,table.tv td{border:1px solid var(--line);padding:6px 8px;vertical-align:top}
  table.tv th{background:var(--panel-2);font-size:10px;text-transform:uppercase;letter-spacing:.5px;color:var(--ink-dim);text-align:left}
  table.tv td.rowlab{background:var(--panel-2);font-weight:700;width:92px;white-space:nowrap}
  table.tv col.c1{width:92px}
  table.tv .thick-l,table.tv td.thick-l{border-left:3px solid var(--ink-dim)}
  table.tv td.tv-merge{vertical-align:top}
  table.tv td.tv-merge .tv-cell-in{height:100%;min-height:78px}
  .ak-figs{display:flex;flex-wrap:wrap;align-items:center;gap:2px}
  .ak-figs .grp{display:flex;gap:2px;margin-right:8px}
  .ak-figs svg{width:12px;height:20px}
  .ak-figs .fig{cursor:pointer;line-height:0;border-radius:3px;padding:1px}
  .ak-figs .fig:hover{background:rgba(230,57,70,.25);outline:1px solid var(--crit)}
  .kfz-figs svg{width:20px;height:18px}
  .ak-num{font-family:var(--mono);color:var(--ink-dim);margin-left:6px;font-size:11px}
  .kfz-num{font-family:var(--mono);font-size:15px;font-weight:700}
  .tv-cell-in{width:100%;background:var(--panel-2);border:1px solid var(--line);color:var(--ink);border-radius:4px;font-size:12px;font-family:var(--sans);padding:4px 6px;resize:vertical;min-height:26px}
  .tv-cell-in:focus{outline:none;border-color:var(--accent)}
  .tv-bet{font-family:var(--mono);color:var(--accent-2);font-weight:800;margin-bottom:4px;font-size:16px}
  .ak-total{font-family:var(--mono);font-weight:800;font-size:16px;color:var(--ink);margin-right:8px;min-width:18px;text-align:right}
  .search-results.show{display:block}
  .search-results .sr{padding:8px 12px;font-size:12px;border-bottom:1px solid var(--line);cursor:pointer}
  .search-results .sr:hover{background:var(--panel-2)}
  .search-results .sr:last-child{border-bottom:none}
  /* Handlungsbedarf-Frame */
  #hbPane h2{color:var(--accent-2)}
  #hbPane.flash{animation:hbflash .9s ease}
  @keyframes hbflash{0%{box-shadow:inset 0 0 0 2px var(--accent-2)}100%{box-shadow:inset 0 0 0 0 transparent}}
  .hb-item{padding:9px 14px;border-bottom:1px solid var(--line);display:flex;flex-direction:column;gap:7px}
  .hb-item .hb-head{display:flex;gap:10px;align-items:flex-start}
  .hb-item .hb-ic{color:var(--accent-2);font-size:15px;line-height:1.3}
  .hb-item .hb-body{flex:1;min-width:0}
  .hb-item .hb-ts{font-family:var(--mono);font-size:11px;color:var(--ink-dim)}
  .hb-item .hb-txt{font-size:13px;margin-top:1px}
  .hb-decision{display:flex;gap:8px;align-items:flex-end}
  .hb-cmt{flex:1;background:var(--panel-2);border:1px solid var(--line);color:var(--ink);border-radius:6px;font-size:12px;font-family:var(--sans);padding:6px 8px;resize:vertical;min-height:34px}
  .hb-cmt:focus{outline:none;border-color:var(--info)}
  .hb-ro{font-size:11px;color:var(--ink-dim);font-style:italic}
  .hb-item .hb-done{background:var(--ok);border:none;color:#06210d;border-radius:5px;font-size:11px;font-weight:700;padding:8px 12px;white-space:nowrap}
  .hb-item .hb-done:hover{filter:brightness(1.1)}
  .hb-item .hb-done:disabled{opacity:.4;cursor:not-allowed}
  .entry .decision{margin-top:4px;padding:4px 8px;border-left:3px solid var(--info);background:rgba(74,144,217,.12);color:#7fb3e6;font-size:12px;border-radius:0 4px 4px 0}
  .entry .prio-btn{background:none;border:1px solid var(--line);color:var(--ink-dim);border-radius:4px;font-size:12px;line-height:1;padding:2px 5px;margin-right:2px;cursor:pointer;flex:0 0 auto;align-self:flex-start}
  .entry .prio-btn:hover{border-color:var(--accent-2);color:var(--accent-2)}
  .entry .prio-btn.on{background:rgba(244,163,0,.2);border-color:var(--accent-2);color:var(--accent-2)}
  .map-hint{position:absolute;bottom:12px;left:50%;transform:translateX(-50%);z-index:600;background:rgba(22,27,34,.92);border:1px solid var(--accent);padding:6px 14px;border-radius:20px;font-size:12px;font-family:var(--mono);color:var(--accent-2);display:none}
  .map-hint.show{display:block}

  .cat{padding:2px 12px 8px}
  .cat-title{font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--ink-dim);margin:8px 0 6px;display:flex;justify-content:space-between;align-items:center}
  .cat-title .addbtn{background:none;border:1px solid var(--line);color:var(--ink-dim);border-radius:4px;font-size:12px;line-height:1;padding:2px 6px}
  .cat-title .addbtn:hover{border-color:var(--ok);color:var(--ok)}
  .sym-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:6px}
  .sym{position:relative;aspect-ratio:1;border:1px solid var(--line);border-radius:6px;background:var(--panel-2);display:flex;align-items:center;justify-content:center;padding:4px;transition:.12s}
  .sym:hover{border-color:var(--accent);transform:translateY(-2px)}
  .sym svg{width:100%;height:100%;pointer-events:none}
  .sym .del{position:absolute;top:-6px;right:-6px;width:16px;height:16px;border-radius:50%;background:var(--crit);color:#fff;border:none;font-size:10px;line-height:1;display:none;align-items:center;justify-content:center;padding:0}
  .palcol.editing .sym .del{display:flex}
  .palcol.editing .sym .del.builtin{background:var(--ink-dim)}

  .kraefte{width:100%;border-collapse:collapse;font-size:12px}
  .kraefte th{position:sticky;top:0;background:var(--panel-2);text-align:left;padding:7px 9px;font-size:10px;text-transform:uppercase;letter-spacing:.5px;color:var(--ink-dim);border-bottom:1px solid var(--line);white-space:nowrap}
  .kraefte td{padding:6px 9px;border-bottom:1px solid var(--line);white-space:nowrap}
  .kraefte tr:hover td{background:var(--panel-2)}
  .kraefte .rn{font-weight:700;font-family:var(--mono)}
  .kraefte .ea{color:var(--ink-dim)}
  .kraefte input{background:transparent;border:1px solid transparent;color:var(--ink);font-size:12px;padding:3px 4px;border-radius:4px;width:100%;min-width:70px}
  .kraefte input:hover{border-color:var(--line)}
  .kraefte input:focus{outline:none;border-color:var(--accent);background:var(--panel)}
  .kraefte .staerke input{font-family:var(--mono);width:92px;min-width:92px}
  .kraefte .dur{font-family:var(--mono);color:var(--accent-2)}
  .kraefte select{background:var(--panel);border:1px solid var(--line);color:var(--ink);border-radius:4px;font-size:11px;padding:2px}
  .kraefte .rem{background:none;border:none;color:var(--ink-dim);font-size:14px}
  .kraefte .rem:hover{color:var(--crit)}
  .kraefte tr.st-green td{background:rgba(63,185,80,.16)}
  .kraefte tr.st-green:hover td{background:rgba(63,185,80,.24)}
  .kraefte tr.st-yellow td{background:rgba(244,194,13,.16)}
  .kraefte tr.st-yellow:hover td{background:rgba(244,194,13,.24)}
  .kraefte tr.st-red td{background:rgba(230,57,70,.18)}
  .kraefte tr.st-red:hover td{background:rgba(230,57,70,.26)}

  .log-add{display:flex;gap:6px;padding:8px 14px;border-bottom:1px solid var(--line)}
  .log-add input{flex:1;background:var(--panel-2);border:1px solid var(--line);color:var(--ink);padding:7px 9px;border-radius:6px;font-size:13px}
  .log-add input:focus{outline:none;border-color:var(--accent)}
  .log-add select{background:var(--panel-2);border:1px solid var(--line);color:var(--ink);border-radius:6px;font-size:12px;padding:0 4px}
  .log-add button{background:var(--accent);border:none;color:#fff;width:34px;border-radius:6px;font-size:16px;font-weight:700}
  .entry{padding:8px 14px;border-bottom:1px solid var(--line);font-size:13px;display:flex;gap:9px}
  .entry .ts{font-family:var(--mono);font-size:11px;color:var(--ink-dim);white-space:nowrap;padding-top:1px}
  .entry .by{font-family:var(--mono);font-size:11px;color:#4a90d9;white-space:nowrap;padding-top:1px;font-weight:700}
  .entry .txt{flex:1}
  .tag{display:inline-block;font-size:9px;font-weight:700;padding:1px 6px;border-radius:8px;margin-right:6px;text-transform:uppercase;letter-spacing:.5px;vertical-align:1px}
  .tag-info{background:rgba(74,144,217,.2);color:var(--info)}
  .tag-lage{background:rgba(244,163,0,.2);color:var(--warn)}
  .tag-befehl{background:rgba(230,57,70,.2);color:var(--accent)}
  .tag-meldung{background:rgba(63,185,80,.2);color:var(--ok)}

  .tac-marker{background:transparent;border:none}
  .tac-marker .wrap{position:relative;width:44px;height:44px}
  .tac-marker svg{width:44px;height:44px;filter:drop-shadow(0 2px 4px rgba(0,0,0,.6))}
  .tac-marker .lbl{position:absolute;top:44px;left:50%;transform:translateX(-50%);white-space:nowrap;font-size:11px;font-weight:700;font-family:var(--mono);background:rgba(13,17,23,.85);padding:1px 5px;border-radius:3px;color:var(--ink)}
  .empty{padding:20px 14px;color:var(--ink-dim);font-size:12px;text-align:center}
  .leaflet-popup-content-wrapper{background:var(--panel-2);color:var(--ink);border:1px solid var(--line);border-radius:8px}
  .leaflet-popup-tip{background:var(--panel-2)}
  .pop h4{margin:0 0 6px;font-size:13px}
  .pop .pop-sec{margin:9px 0 2px;padding-top:8px;border-top:1px solid var(--line);font-size:11px;font-weight:700;color:var(--ink);text-transform:uppercase;letter-spacing:.5px;display:flex;justify-content:space-between;align-items:baseline;gap:8px}
  .pop .pop-cur{font-weight:600;text-transform:none;letter-spacing:0;color:#4a90d9;font-family:var(--mono);font-size:11px}
  .pop label{font-size:11px;color:var(--ink-dim);display:block;margin:6px 0 2px}
  .pop input,.pop select{width:100%;background:var(--panel);border:1px solid var(--line);color:var(--ink);padding:5px;border-radius:5px;font-size:12px}
  .pop .row{display:flex;gap:6px;margin-top:8px}
  .pop button{flex:1;border:none;border-radius:5px;padding:6px;font-size:12px;font-weight:600}
  .pop .del{background:var(--crit);color:#fff}
  .pop .sv{background:var(--ok);color:#06210d}

  /* Modal für Zeichen hinzufügen */
  .modal-bg{position:fixed;inset:0;background:rgba(0,0,0,.55);z-index:2000;display:none;align-items:center;justify-content:center}
  .modal-bg.show{display:flex}
  .modal{background:var(--panel);border:1px solid var(--line);border-radius:12px;width:380px;max-width:92vw;padding:20px}
  .modal h3{margin:0 0 14px;font-size:15px}
  .modal .fld{margin-bottom:11px}
  .modal .fld label{display:block;font-size:11px;color:var(--ink-dim);margin-bottom:4px}
  .modal .fld input,.modal .fld select{width:100%;background:var(--panel-2);border:1px solid var(--line);color:var(--ink);padding:8px;border-radius:6px;font-size:13px}
  .modal .fld input:focus,.modal .fld select:focus{outline:none;border-color:var(--accent)}
  .swatches{display:flex;gap:6px;flex-wrap:wrap}
  .swatch{width:28px;height:28px;border-radius:6px;border:2px solid transparent;cursor:pointer}
  .swatch.sel{border-color:var(--ink)}
  .preview-box{display:flex;align-items:center;gap:14px;background:var(--panel-2);border:1px dashed var(--line);border-radius:8px;padding:12px;margin:12px 0}
  .preview-box svg{width:52px;height:52px}
  .modal-actions{display:flex;gap:8px;margin-top:6px}
  .modal-actions button{flex:1;border:none;border-radius:6px;padding:10px;font-weight:700;font-size:13px}
  .modal-actions .ok{background:var(--accent);color:#fff}
  .modal-actions .cancel{background:var(--panel-2);border:1px solid var(--line);color:var(--ink)}
  .chk{display:flex;align-items:center;gap:8px;font-size:12px;color:var(--ink-dim)}
  .chk input{width:auto}

  #viewConfig{display:none;height:100%;overflow:auto;padding:22px 28px}
  #viewConfig.active{display:block}
  #viewEinsatz.hidden{display:none}
  .cfg-head{max-width:1100px;margin:0 auto 18px}
  .cfg-head h1{font-size:20px;margin:0 0 4px}
  .cfg-head p{color:var(--ink-dim);font-size:13px;margin:0}
  .cfg-grid{max-width:1100px;margin:0 auto}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:16px}
  .card h3{margin:0 0 12px;font-size:13px;text-transform:uppercase;letter-spacing:1px;color:var(--ink-dim);display:flex;justify-content:space-between;align-items:center}
  .card h3 button{background:var(--accent);border:none;color:#fff;border-radius:6px;font-size:12px;padding:6px 12px;font-weight:600}
  .cfg-list .grp{margin-bottom:20px}
  .cfg-list .grp-h{display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid var(--line);padding-bottom:6px;margin-bottom:10px}
  .cfg-list .grp-h b{font-size:13px} .cfg-list .grp-h span{font-size:11px;color:var(--ink-dim)}
  .el-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:10px}
  .el-card{display:flex;align-items:center;gap:10px;background:var(--panel-2);border:1px solid var(--line);border-radius:8px;padding:9px}
  .el-card svg{width:38px;height:38px;flex:0 0 38px}
  .el-card .n{font-size:12px;font-weight:600;line-height:1.2}
  .el-card .k{font-size:10px;color:var(--ink-dim);font-family:var(--mono)}
  .el-card .x{margin-left:auto;background:none;border:none;color:var(--ink-dim);font-size:15px}
  .el-card .x:hover{color:var(--crit)}
  .el-card.builtin .x{color:#3a4150}
  .tagline{font-size:10px;color:var(--ink-dim);margin-left:auto}
  /* ===== Grafischer Einsatzplan ===== */
  #viewPlan{display:none;height:100%;min-height:0;flex-direction:column}
  #viewPlan.active{display:flex}
  /* Status-Übersicht (im mittleren Bereich, umschaltbar wie die Tabelle) */
  #statusView{position:absolute;inset:0;background:var(--bg);display:none;flex-direction:column;overflow:hidden;z-index:400}
  #mapPane.show-status #statusView{display:flex}
  .status-head{display:flex;align-items:center;gap:10px;padding:10px 16px;border-bottom:1px solid var(--line);background:var(--panel)}
  .status-head h2{margin:0;font-size:15px;flex:0 0 auto}
  .status-head #statusConfigBtn{margin-left:auto}
  .status-scroll{flex:1;overflow:auto;padding:0}
  .status-hint{margin-left:auto;font-size:11px;color:var(--ink-dim)}
  .status-block{margin:0 0 18px}
  .status-block-hd{display:flex;align-items:center;gap:10px;padding:8px 14px;background:#1b2330;border-top:1px solid var(--line);border-bottom:1px solid var(--line);font-weight:700;color:#7fb3e6;position:sticky;left:0}
  .status-block-hd .ea-cols-btn{margin-left:auto}
  table.status{border-collapse:collapse;width:max-content;min-width:100%;font-size:12px}
  table.status th,table.status td{border:1px solid var(--line);padding:0;text-align:center;white-space:nowrap}
  /* Spaltenüberschriften senkrecht (von unten nach oben), damit die Spalten schmal bleiben */
  table.status thead th.colhd{position:sticky;top:0;background:var(--panel-2);z-index:2;height:150px;width:34px;min-width:34px;max-width:34px;padding:6px 0;font-weight:600;vertical-align:bottom}
  table.status thead th.colhd>span{writing-mode:vertical-rl;transform:rotate(180deg);display:inline-block;white-space:nowrap;font-size:11px;line-height:1.1;max-height:140px;overflow:hidden}
  table.status thead th.rowhd,table.status thead th.sonsthd{position:sticky;top:0;background:var(--panel-2);z-index:3;padding:7px 8px;font-weight:600;vertical-align:bottom}
  table.status th.rowhd,table.status td.rowhd{position:sticky;left:0;background:var(--panel-2);z-index:1;text-align:left;padding:7px 10px;min-width:180px;font-weight:600}
  table.status thead th.rowhd{z-index:4}
  table.status tr.ea-row td.rowhd,table.status tr.ea-row th.rowhd{background:#1b2330;color:#7fb3e6}
  table.status .ea-toggle{cursor:pointer;display:inline-block;width:16px;color:#7fb3e6;font-size:11px}
  table.status .ea-toggle-empty{display:inline-block;width:16px}
  table.status .ua-count{color:var(--ink-dim);font-weight:400;font-size:11px}
  table.status tr.ua-row td.rowhd{padding-left:24px;color:var(--ink)}
  table.status td.stcell{cursor:pointer;width:34px;min-width:34px;max-width:34px;height:32px;color:var(--ink-dim);font-size:11px}
  table.status td.stcell:hover{background:var(--panel-2)}
  table.status td.stcell.done{background:rgba(63,185,80,.18);color:#3fb950;font-family:var(--mono);font-weight:700;font-size:10px}
  table.status td.stcell.ro{cursor:not-allowed;opacity:.6}
  table.status td.stcell.auto{cursor:default;color:var(--ink-dim);font-style:italic}
  table.status td.stcell.auto:hover{background:transparent}
  table.status td.stcell.auto.done{background:rgba(63,185,80,.10);color:#7fce93;font-style:normal}
  table.status td.sonst{min-width:150px;padding:0}
  table.status td.sonst textarea{width:100%;min-height:30px;background:transparent;border:none;color:var(--ink);font-size:12px;font-family:var(--sans);padding:5px 7px;resize:vertical}
  .status-config{position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);width:340px;max-height:80%;overflow:auto;background:var(--panel);border:1px solid var(--accent);border-radius:10px;box-shadow:0 12px 40px rgba(0,0,0,.6);padding:14px;z-index:600}
  .status-config.hidden{display:none}
  .sc-head{font-size:13px;font-weight:600;margin-bottom:8px}
  .sc-note{color:var(--ink-dim);font-weight:400;font-size:11px}
  .sc-list{display:flex;flex-direction:column;gap:2px;max-height:340px;overflow:auto}
  .sc-item{display:flex;align-items:center;gap:8px;padding:5px 6px;border-radius:6px;cursor:pointer;font-size:12px}
  .sc-item:hover{background:var(--panel-2)}
  .sc-item input{accent-color:var(--accent)}
  .sc-actions{margin-top:10px;text-align:right}
  .plan-scroll{min-width:900px;padding:20px 24px 60px;flex:1;overflow:auto}
  /* Befehl-Seite */
  #viewBefehl{display:none;height:100%;min-height:0}
  #viewBefehl.active{display:block}
  .befehl-wrap{display:grid;grid-template-columns:3fr 1fr;height:100%;min-height:0}
  .befehl-viewer{position:relative;min-width:0;min-height:0;background:#20242b;border-right:1px solid var(--line);display:flex}
  .befehl-viewer iframe{width:100%;height:100%;border:none;background:#fff}
  .befehl-empty{margin:auto;text-align:center;color:var(--ink-dim);padding:30px}
  .befehl-empty p{margin:6px 0}
  .befehl-docx{width:100%;height:100%;overflow:auto;background:#fff;color:#111;padding:34px 46px;font-family:Georgia,'Times New Roman',serif;font-size:15px;line-height:1.6}
  .befehl-docx h1,.befehl-docx h2,.befehl-docx h3{font-family:Arial,sans-serif;line-height:1.3}
  .befehl-docx p{margin:0 0 10px}
  .befehl-docx table{border-collapse:collapse;margin:10px 0}
  .befehl-docx td,.befehl-docx th{border:1px solid #999;padding:4px 8px}
  .befehl-docx img{max-width:100%}
  .xlsx-view table{border-collapse:collapse;margin:0 0 6px;font-family:Arial,sans-serif;font-size:13px}
  .xlsx-view td,.xlsx-view th{border:1px solid #bbb;padding:3px 7px;color:#111}
  .xlsx-view h3{color:#111}
  .befehl-tree{display:flex;flex-direction:column;min-height:0;min-width:0;background:var(--panel)}
  .befehl-tree-hd{display:flex;align-items:center;gap:6px;padding:8px 10px;border-bottom:1px solid var(--line);font-size:12px}
  .befehl-tree-hd #befehlFolderName{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--ink-dim);font-family:var(--mono)}
  .befehl-tree-body{flex:1;overflow:auto;padding:6px 4px;font-size:13px}
  .bt-node{user-select:none}
  .bt-row{display:flex;align-items:center;gap:5px;padding:3px 6px;border-radius:5px;cursor:pointer;white-space:nowrap}
  .bt-row:hover{background:var(--panel-2)}
  .bt-row.active{background:rgba(74,144,217,.2);color:#7fb3e6}
  .bt-ic{width:16px;text-align:center;flex:0 0 auto}
  .bt-name{overflow:hidden;text-overflow:ellipsis}
  .bt-children{margin-left:14px;border-left:1px solid var(--line);padding-left:4px}
  .bt-badge{margin-left:auto;font-size:9px;color:var(--ink-dim);font-family:var(--mono)}
  .plan-toolbar{display:flex;align-items:center;gap:14px;flex:0 0 auto;padding:12px 24px;background:var(--bg);border-bottom:1px solid var(--line)}
  .plan-hint{font-size:12px;color:var(--ink-dim)}
  .plan-box{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:10px 12px}
  .plan-top{display:grid;grid-template-columns:1.1fr .7fr 1.6fr;gap:14px;align-items:start}
  .pl-title{font-size:12px;font-weight:800;margin-bottom:8px}
  .pl-title.accent,.pl-k.accent{color:#4a90d9}
  .pl-row{display:flex;gap:6px;font-size:12px;padding:3px 0;border-bottom:1px solid rgba(42,49,64,.5)}
  .pl-k{color:var(--ink-dim);white-space:nowrap;min-width:78px}
  .pl-v{flex:1;color:var(--ink);min-height:16px;outline:none;cursor:text}
  .pl-v:focus{background:var(--panel-2)}
  .pl-v.big{font-weight:700;font-size:14px;min-height:40px}
  .stab-grid{display:grid;grid-template-columns:1fr 1fr;gap:0 16px}
  .head-meta .pl-v{border-bottom:1px dashed transparent}
  .plan-connector{position:relative;height:34px;margin:6px 0 2px}
  .pl-vline{position:absolute;left:50%;top:0;width:2px;height:16px;background:var(--line)}
  .pl-hline{position:absolute;top:16px;height:2px;background:var(--line);left:5%;right:5%}
  .plan-eas{display:flex;gap:14px;align-items:flex-start;flex-wrap:nowrap;overflow-x:auto;padding-bottom:10px}
  .ea-card{position:relative;flex:0 0 260px;background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:10px 12px}
  .ea-card::before{content:"";position:absolute;top:-18px;left:50%;width:2px;height:18px;background:var(--line)}
  .ea-card .ea-title{display:flex;align-items:center;gap:6px;font-weight:800;font-size:12px;margin-bottom:8px}
  .ea-card .ea-title .lab{color:#4a90d9}
  .ea-card .ea-title .rem{margin-left:auto;background:none;border:none;color:var(--ink-dim);font-size:14px;cursor:pointer}
  .ea-card .ea-title .rem:hover{color:var(--crit)}
  .ea-sub{font-size:11px;color:var(--ink-dim);text-transform:uppercase;letter-spacing:.5px;margin:8px 0 3px}
  .ua-sub-hd{font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--ink-dim);margin:12px 0 6px;border-top:1px solid var(--line);padding-top:8px}
  .ua-list{display:flex;flex-direction:column;gap:10px}
  .ua-card{background:var(--panel-2);border:1px solid var(--line);border-left:3px solid var(--info);border-radius:8px;padding:8px 10px}
  .ua-card .ea-title{margin-bottom:6px}
  .ua-card .ea-title .lab.ua{color:var(--info)}
  .ua-card .rem{background:none;border:none;color:var(--ink-dim);font-size:13px;cursor:pointer}
  .ua-card .rem:hover{color:var(--crit)}
  .stw{display:inline-flex;align-items:center;gap:2px;font-family:var(--mono)}
  .stw .sx{width:26px;background:var(--panel-2);border:1px solid var(--line);color:var(--ink);border-radius:4px;text-align:center;font-size:12px;padding:2px 0;font-family:var(--mono)}
  .stw .sx:focus{outline:none;border-color:var(--accent);background:var(--panel)}
  .stw .sep{color:var(--ink-dim);font-weight:700}
  .stw .sges{min-width:24px;text-align:center;color:var(--accent-2);font-weight:700}
  .ua-card .ua-st .sx{width:24px}
  .ea-addua{background:var(--panel-2);border:1px solid var(--line);color:var(--ink-dim);border-radius:5px;font-size:11px;padding:3px 8px;margin-top:8px;cursor:pointer}
  .ea-addua:hover{border-color:var(--ok);color:var(--ok)}
  .ea-kanal{margin-top:10px;text-align:center;font-size:11px;color:var(--ink-dim);border-top:1px solid var(--line);padding-top:6px}
  .ea-kanal .pl-v{display:inline-block;min-width:60px;border-bottom:1px dashed var(--line)}

  /* ===== Rollen-Overlay ===== */
  #roleOverlay{position:fixed;inset:0;z-index:5000;background:rgba(9,12,18,.92);display:flex;align-items:center;justify-content:center;backdrop-filter:blur(4px)}
  #roleOverlay.hidden{display:none}
  .role-card{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:26px 28px;width:380px;max-width:92vw}
  .role-brand{display:flex;align-items:center;gap:10px;font-weight:800;letter-spacing:.5px;font-size:18px}
  .role-brand .dot{width:12px;height:12px;border-radius:2px;background:var(--accent);box-shadow:0 0 10px var(--accent)}
  .role-sub{color:var(--ink-dim);font-size:13px;margin:8px 0 18px}
  .role-lab{font-size:11px;color:var(--ink-dim);display:block;margin-bottom:5px}
  #roleSelect{width:100%;background:var(--panel-2);border:1px solid var(--line);color:var(--ink);padding:10px;border-radius:8px;font-size:14px}
  #roleSelect:focus{outline:none;border-color:var(--accent)}
  .role-conn{font-size:12px;font-family:var(--mono);margin:12px 0;padding:7px 10px;border-radius:6px;background:var(--panel-2);border:1px solid var(--line);color:var(--warn)}
  .role-conn.ok{color:var(--ok);border-color:rgba(63,185,80,.4)}
  .role-conn.err{color:var(--crit);border-color:rgba(230,57,70,.4)}
  .role-go{width:100%;background:var(--accent);border:none;color:#fff;padding:12px;border-radius:8px;font-weight:700;font-size:14px;margin-top:4px}
  .role-go:disabled{opacity:.5;cursor:not-allowed}
  .role-note{font-size:11px;color:var(--ink-dim);margin:16px 0 0;line-height:1.5}
  .role-badge{font-family:var(--mono);font-size:12px;background:var(--panel-2);border:1px solid var(--line);padding:6px 12px;border-radius:6px;color:#4a90d9}
  .role-badge.ea{color:var(--accent-2)}
  body.readonly-plan .ea-card [contenteditable],
  body.readonly-plan .ea-card .sx,
  body.readonly-plan .ea-card .ea-addua,
  body.readonly-plan .ea-card .rem{pointer-events:none;opacity:.55}
  .ea-card.mine{border-color:var(--accent-2);box-shadow:0 0 0 1px rgba(244,163,0,.3)}
  .ea-card.mine [contenteditable],.ea-card.mine .sx,.ea-card.mine .ea-addua,.ea-card.mine .rem{pointer-events:auto;opacity:1}
  #planAddEA.hidden-role{display:none}
  .net-disc{position:fixed;bottom:14px;left:50%;transform:translateX(-50%);z-index:4000;background:var(--crit);color:#fff;font-size:13px;padding:8px 16px;border-radius:20px;display:none}
  .net-disc.show{display:block}
</style>
</head>
<body>
<!-- ===== Rollen-/Verbindungs-Overlay ===== -->
<div id="roleOverlay">
  <div class="role-card">
    <div class="role-brand"><span class="dot"></span> LAGEFÜHRUNG</div>
    <p class="role-sub">Mehrplatz-Betrieb – bitte Arbeitsplatz-Rolle wählen.</p>
    <label class="role-lab">Rolle / Führungsebene</label>
    <select id="roleSelect"></select>
    <div id="roleConnState" class="role-conn">Verbindung wird aufgebaut…</div>
    <button id="roleGo" class="role-go">Arbeitsplatz starten</button>
    <p class="role-note">Führungsgruppe sieht alle Eingaben und das gesamte Einsatztagebuch. Ein Einsatzabschnitt bearbeitet nur seinen Abschnitt und sieht die übrigen nur lesend.</p>
  </div>
</div>

<div class="app">
  <header>
    <div class="brand"><span class="dot"></span> LAGEFÜHRUNG <small>Einsatz- und Lagedarstellung</small></div>
    <div class="op-meta">
      <span>Einsatz: <b id="opName" contenteditable="true" spellcheck="false" title="Zum Ändern klicken">Übungslage Dorsten</b></span>
      <span>Abschnitt: <b id="opSection" contenteditable="true" spellcheck="false" title="Zum Ändern klicken">EA 1</b></span>
    </div>
    <div class="tabs">
      <button class="tab active" id="tabEinsatz">Lage</button>
      <button class="tab" id="tabBefehl">Befehl</button>
      <button class="tab" id="tabPlan">Grafischer Einsatzplan</button>
      <button class="tab" id="tabConfig">Grundkonfiguration</button>
    </div>
    <div class="spacer"></div>
    <button class="hbtn" id="btnStart" style="background:var(--ok);border-color:var(--ok);color:#06210d;font-weight:700">▶ Einsatzbeginn</button>
    <button class="hbtn" id="btnExport">Lage exportieren</button>
    <button class="hbtn" id="btnClear">Zurücksetzen</button>
    <div class="role-badge" id="roleBadge" title="Angemeldete Rolle">–</div>
    <div class="clock" id="clock">--:--:--</div>
  </header>

  <div class="content">
  <div class="view" id="viewEinsatz">
    <!-- Palette (einklappbar) -->
    <div class="col br palcol" id="palcol">
      <h2>Taktische Zeichen
        <button class="h2act" id="palEdit">Bearbeiten</button>
      </h2>
      <div class="scroll" id="palette"></div>
    </div>
    <div class="resizer-col" id="rzL" title="Breite ziehen"></div>

    <div class="center" id="centerCol">
      <div class="col map-wrap" id="mapPane">
        <div class="map-toolbar">
          <button id="viewToggle" class="tglbtn" title="Zwischen Karte und Tabelle wechseln">▦ Tabelle</button>
          <button id="statusToggle" class="tglbtn" title="Status-Übersicht ein-/ausblenden">📋 Status</button>
          <button id="palToggle" class="tglbtn" title="Zeichen-Menü ein-/ausblenden">☰ Zeichen</button>
          <button id="tPan" class="active" title="Verschieben">✋</button>
          <button id="tMeasure" title="Strecke messen">📏</button>
          <button id="tArea" title="Gefahrenbereich zeichnen">⬡</button>
          <button id="tLabel" title="Textmarke setzen">🏷️</button>
          <button id="tDelete" title="Zeichen entfernen (anklicken)">🗑️</button>
          <button id="mapCustomLoad" class="tglbtn" title="Eigenes Kartenmaterial laden (JPG/PDF)">🗺️+ Material</button>
          <button id="mapBaseToggle" class="tglbtn" title="Online-Karte / Eigenes Material umschalten" style="display:none">🌐 Online</button>
          <div class="map-search">
            <input id="mapSearch" placeholder="Ort suchen (z. B. Stadt)…" />
            <button id="mapSearchBtn" title="Suchen">🔍</button>
          </div>
        </div>
        <div class="map-hint" id="mapHint"></div>
        <div class="search-results" id="searchResults"></div>
        <div id="map"></div>
        <div id="tableView"><div class="tv-scroll" id="tableViewBody"></div></div>
        <div id="statusView">
          <div class="status-head">
            <h2>Status-Übersicht</h2>
            <span class="status-hint">Spalten je Einsatzabschnitt über „⚙ Spalten“ festlegen (Führungsgruppe)</span>
          </div>
          <div class="status-scroll" id="statusBody"></div>
          <div class="status-config hidden" id="statusConfig">
            <div class="sc-head" id="statusConfigTitle">Spalten wählen</div>
            <div class="sc-list" id="statusConfigList"></div>
            <div class="sc-actions"><button id="statusConfigDone" class="role-go" style="width:auto;padding:8px 16px">Fertig</button></div>
          </div>
        </div>
      </div>
      <div class="resizer-row" id="rzM" title="Höhe ziehen"></div>
      <div class="col bt" id="kraeftePane">
        <h2>Eingesetzte Kräfte
          <span style="display:flex;align-items:center;gap:6px;margin-left:auto">
            <span class="tagline" id="kraefteCount"></span>
            <select id="levelSelect" class="h2act" style="cursor:pointer"></select>
          </span>
        </h2>
        <div class="scroll">
          <table class="kraefte">
            <thead><tr>
              <th>Einsatzabschnitt</th><th>Unterabschnitt</th><th>Rufname</th>
              <th>Status</th><th>Auftrag</th><th>Stärke</th><th>Beteiligte</th><th>Dienstbeginn</th><th>Einsatzdauer</th><th></th>
            </tr></thead>
            <tbody id="kraefteBody"></tbody>
          </table>
        </div>
      </div>
    </div>
    <div class="resizer-col" id="rzR" title="Breite ziehen"></div>

    <div class="col bl" id="rightCol">
      <div class="rc-top" id="etbPane">
        <h2>Einsatztagebuch (ETB)
          <button class="h2act" id="etbPdf" title="Einsatztagebuch als PDF speichern" style="margin-left:auto">🖨 PDF</button>
        </h2>
        <div class="log-add">
          <select id="logType">
            <option value="meldung">Meldung</option>
            <option value="lage">Lage</option>
            <option value="befehl">Befehl</option>
            <option value="info">Info</option>
          </select>
          <input id="logInput" placeholder="Eintrag erfassen…" />
          <button id="logBtn">+</button>
        </div>
        <div class="scroll" id="log"></div>
      </div>
      <div class="resizer-row" id="rzETB" title="Höhe ziehen"></div>
      <div class="rc-bot" id="hbPane">
        <h2>Handlungsbedarf / zu entscheiden <span class="tagline" id="hbCount"></span></h2>
        <div class="scroll" id="handlungsbedarf"></div>
      </div>
    </div>

  </div>
  </div>

  <!-- ============ BEFEHL (externe Dateien) ============ -->
  <div class="view" id="viewBefehl">
    <div class="befehl-wrap">
      <div class="befehl-viewer" id="befehlViewer">
        <div class="befehl-empty" id="befehlEmpty">
          <div>
            <p>Wähle rechts einen Ordner auf der Netzablage und öffne eine Datei.</p>
            <p style="font-size:12px;color:var(--ink-dim)">PDF wird hier angezeigt · Word (.docx) und Excel (.xlsx/.xlsm) werden vereinfacht dargestellt.</p>
          </div>
        </div>
        <iframe id="befehlFrame" title="Dateianzeige" style="display:none"></iframe>
        <div class="befehl-docx" id="befehlDocx" style="display:none"></div>
      </div>
      <div class="befehl-tree">
        <div class="befehl-tree-hd">
          <span id="befehlFolderName">Kein Ordner</span>
          <button class="h2act" id="befehlPick" title="Ordner auf der Netzablage wählen">📁 Ordner</button>
          <button class="h2act" id="befehlReload" title="Aktualisieren">⟳</button>
        </div>
        <div class="befehl-tree-body" id="befehlTree">
          <div class="empty" style="padding:14px">Noch kein Ordner gewählt.</div>
        </div>
      </div>
    </div>
  </div>

  <!-- ============ GRAFISCHER EINSATZPLAN ============ -->
  <div class="view" id="viewPlan">
    <div class="plan-toolbar">
      <button class="hbtn" id="planAddEA">+ Einsatzabschnitt</button>
      <button class="hbtn" id="planPdf" title="Grafischen Einsatzplan als PDF speichern">🖨 PDF</button>
      <span class="plan-hint">Felder direkt anklicken und ausfüllen · „+ UA" fügt einen Unterabschnitt hinzu</span>
    </div>
    <div class="plan-scroll">

      <!-- Kopfzeile -->
      <div class="plan-top">
        <div class="plan-box head-meta">
          <div class="pl-row"><span class="pl-k">Dienststelle:</span><span class="pl-v" contenteditable="true"></span></div>
          <div class="pl-row"><span class="pl-k">Aktenzeichen:</span><span class="pl-v" contenteditable="true"></span></div>
          <div class="pl-row"><span class="pl-k">Anlass:</span><span class="pl-v" contenteditable="true" data-bind="anlass"></span></div>
          <div class="pl-row"><span class="pl-k">Blatt Nr.:</span><span class="pl-v" contenteditable="true"></span></div>
        </div>
        <div class="plan-box head-pf">
          <div class="pl-title accent">Polizeiführer:</div>
          <div class="pl-v big" contenteditable="true" data-bind="pf"></div>
        </div>
        <div class="plan-box head-stab">
          <div class="pl-title accent">Führungsstab / Führungsgruppe</div>
          <div class="stab-grid">
            <div class="pl-row"><span class="pl-k">Leiter:</span><span class="pl-v" contenteditable="true"></span></div>
            <div class="pl-row"><span class="pl-k accent">Kräfte:</span><span class="pl-v" contenteditable="true"></span></div>
            <div class="pl-row"><span class="pl-k">Rufname:</span><span class="pl-v" contenteditable="true"></span></div>
            <div class="pl-row"><span class="pl-k">MOZ:</span><span class="pl-v" contenteditable="true"></span></div>
            <div class="pl-row"><span class="pl-k">Telefon:</span><span class="pl-v" contenteditable="true"></span></div>
            <div class="pl-row"><span class="pl-k">EOZ:</span><span class="pl-v" contenteditable="true"></span></div>
            <div class="pl-row"><span class="pl-k">Mobiltel.:</span><span class="pl-v" contenteditable="true"></span></div>
            <div class="pl-row"><span class="pl-k">Kanal:</span><span class="pl-v" contenteditable="true"></span></div>
          </div>
        </div>
      </div>

      <div class="plan-connector"><div class="pl-vline"></div><div class="pl-hline" id="planHLine"></div></div>

      <!-- Einsatzabschnitte -->
      <div class="plan-eas" id="planEAs"></div>
    </div>
  </div>

  <div class="view" id="viewConfig">
    <div class="cfg-head">
      <h1>Grundkonfiguration – Taktische Elemente</h1>
      <p>Übersicht aller taktischen Zeichen. Zeichen lassen sich auch direkt im Palette-Menü der Lageansicht ergänzen und entfernen.</p>
    </div>
    <div class="cfg-grid">
      <div class="card">
        <h3>Vorhandene Zeichen <button id="cfgAddOpen">+ Zeichen hinzufügen</button></h3>
        <div class="cfg-list" id="cfgList"></div>
      </div>
    </div>
  </div>
  </div>
</div>

<!-- Modal: Zeichen hinzufügen -->
<div class="modal-bg" id="modalBg">
  <div class="modal">
    <h3 id="modalTitle">Neues taktisches Zeichen</h3>
    <div class="fld"><label>Bezeichnung</label><input id="mName" placeholder="z. B. Wechsellader"></div>
    <div class="fld"><label>Kategorie</label><select id="mCat"></select></div>
    <div class="fld"><label>Kürzel (max. 4 Zeichen, optional)</label><input id="mKuerzel" maxlength="4" placeholder="z. B. WLF"></div>
    <div class="fld"><label>Form</label>
      <select id="mShape">
        <option value="unit">Einheit (Rechteck)</option>
        <option value="stelle">Führungsstelle</option>
        <option value="point">Objekt (Kreis)</option>
        <option value="diamond">Gefahr (Raute)</option>
        <option value="factory">Objekt – Fabrik</option>
        <option value="house">Objekt – Haus</option>
        <option value="bank">Objekt – Bank</option>
        <option value="car">Objekt – Fahrzeug</option>
        <option value="people">Objekt – Sammelstelle (Personen)</option>
      </select>
    </div>
    <div class="fld"><label>Rahmenfarbe</label><div class="swatches" id="mSwatches"></div></div>
    <label class="chk"><input type="checkbox" id="mMobile"> Mobiles Einsatzmittel (erscheint in Kräfte-Tabelle)</label>
    <label class="chk" style="margin-top:6px"><input type="checkbox" id="mNumber"> Individuell nummerieren (z. B. ZP 1, ZP 2 …)</label>
    <div class="preview-box"><div id="mPreview"></div><div style="font-size:12px;color:var(--ink-dim)">Vorschau</div></div>
    <div class="modal-actions">
      <button class="cancel" id="mCancel">Abbrechen</button>
      <button class="ok" id="mAdd">Hinzufügen</button>
    </div>
  </div>
</div>

<div class="net-disc" id="netDisc">Verbindung zum Server verloren – Wiederverbindung…</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.js"></script>
<script>
/* ===== Farb-Grundwerte ===== */
const C={zoll:'#1f6feb', rd:'#f4820a', fuehrung:'#3fb950', thw:'#1f6feb', pol:'#2f6df0', gefahr:'#f4c20d', gebaeude:'#8b98a8', grau:'#8b98a8'};
const PALETTE_COLORS=['#1f6feb','#e63946','#f4820a','#3fb950','#f4c20d','#2f6df0','#8b98a8','#9d4edd'];

/* ===== SVG-Bausteine ===== */
function txt(t,color,size){return \`<text x="50" y="\${58}" font-family="monospace" font-size="\${size||24}" font-weight="800" text-anchor="middle" fill="\${color}">\${t}</text>\`;}
function unitFrame(fill,inner){return \`<svg viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg"><rect x="12" y="30" width="76" height="40" rx="3" fill="\${fill}" stroke="#0d1117" stroke-width="3"/>\${inner}</svg>\`;}
function stelleFrame(fill,inner){return \`<svg viewBox="0 0 100 100"><path d="M12 28 h76 v44 h-76 z" fill="\${fill}" stroke="#0d1117" stroke-width="3"/><path d="M12 28 l14 12 l-14 12 z" fill="#0d1117"/>\${inner}</svg>\`;}
function pointSym(fill,inner){return \`<svg viewBox="0 0 100 100"><circle cx="50" cy="50" r="30" fill="\${fill}" stroke="#0d1117" stroke-width="3"/>\${inner}</svg>\`;}
function diamond(fill,inner){return \`<svg viewBox="0 0 100 100"><path d="M50 16 L84 50 L50 84 L16 50 Z" fill="\${fill}" stroke="#0d1117" stroke-width="3"/>\${inner}</svg>\`;}
// Objektzeichen: Kreis-Rahmen (Objekt) mit Piktogramm innen
function objectFrame(fill,pic){return \`<svg viewBox="0 0 100 100"><circle cx="50" cy="50" r="32" fill="\${fill}" stroke="#0d1117" stroke-width="3"/>\${pic}</svg>\`;}
const PIC={
  factory:\`<g fill="#0d1117"><path d="M32 62 V46 l10 6 V46 l10 6 V40 h8 v22 z"/><rect x="34" y="64" width="34" height="4"/><rect x="60" y="34" width="5" height="10"/></g>\`,
  house:\`<g fill="#0d1117"><path d="M50 34 L70 50 H62 V66 H38 V50 H30 Z"/><rect x="46" y="54" width="8" height="12" fill="\${'#8b98a8'}"/></g>\`,
  bank:\`<g fill="#0d1117"><path d="M34 44 L50 36 L66 44 Z"/><rect x="36" y="46" width="4" height="16"/><rect x="48" y="46" width="4" height="16"/><rect x="60" y="46" width="4" height="16"/><rect x="32" y="63" width="36" height="4"/></g>\`,
  car:\`<g fill="#0d1117"><path d="M32 56 l4 -10 h28 l4 10 v6 h-6 a4 4 0 0 1 -8 0 h-8 a4 4 0 0 1 -8 0 h-6 z"/><path d="M40 48 h20 l2 6 h-24 z" fill="\${'#8b98a8'}"/></g>\`,
  people:\`<g fill="#fff"><circle cx="35" cy="42" r="6"/><rect x="29" y="50" width="12" height="16" rx="3"/><circle cx="50" cy="40" r="6"/><rect x="44" y="48" width="12" height="18" rx="3"/><circle cx="65" cy="42" r="6"/><rect x="59" y="50" width="12" height="16" rx="3"/></g>\`,
  cp:\`<g fill="#fff"><rect x="46" y="30" width="8" height="40"/><path d="M54 32 h16 v14 h-16 z"/></g>\`, // Kontrollpunkt: Schlagbaum/Fahne
  pc:\`<g fill="#0d1117"><rect x="34" y="38" width="32" height="22" rx="2"/><rect x="38" y="42" width="24" height="14" fill="\${'#8b98a8'}"/><rect x="44" y="62" width="12" height="4"/></g>\`,
};
function shapeSvg(shape,fill,kuerzel,ink){
  const inner=kuerzel?txt(kuerzel,ink||'#fff'):'';
  switch(shape){
    case 'unit':return unitFrame(fill,inner);
    case 'stelle':return stelleFrame(fill,inner);
    case 'point':return pointSym(fill,inner);
    case 'diamond':return diamond(fill,inner);
    case 'factory':return objectFrame(fill,PIC.factory);
    case 'house':return objectFrame(fill,PIC.house);
    case 'bank':return objectFrame(fill,PIC.bank);
    case 'car':return objectFrame(fill,PIC.car);
    case 'people':return objectFrame(fill,PIC.people);
    case 'checkpoint':return objectFrame(fill,PIC.cp);
    case 'pc':return objectFrame(fill,PIC.pc);
    default:return unitFrame(fill,inner);
  }
}
function svgOf(it){
  // nummerierte Zeichen zeigen Kürzel+laufende Nummer als Text (bei Raute/Objekt zusätzlich)
  return shapeSvg(it.shape,it.fill,it.kuerzel,it.ink);
}

/* ===== Katalog ===== */
let CATALOG=[
  {cat:'Führung', items:[
    {id:'el',  name:'Einsatzleitung',      shape:'stelle', fill:C.fuehrung, kuerzel:'EL', ink:'#06210d', mobile:false, builtin:true},
    {id:'ea',  name:'Einsatzabschnitt',    shape:'stelle', fill:C.fuehrung, kuerzel:'EA', ink:'#06210d', mobile:false, builtin:true},
    {id:'brt', name:'Bereitstellungsraum', shape:'diamond',fill:C.fuehrung, kuerzel:'BR', ink:'#06210d', mobile:false, builtin:true},
  ]},
  {cat:'Zoll', items:[
    {id:'zuz', name:'Zugriffszug (ZUZ)',       shape:'unit', fill:C.zoll, kuerzel:'ZUZ', ink:'#fff', mobile:true, builtin:true},
    {id:'oez', name:'Observationseinheit (OEZ)',shape:'unit', fill:C.zoll, kuerzel:'OEZ', ink:'#fff', mobile:true, builtin:true},
    {id:'ea_z',name:'Einsatzabschnitt (EA)',   shape:'unit', fill:C.zoll, kuerzel:'EA', ink:'#fff', mobile:true, builtin:true},
    {id:'zfhr',name:'Zoll-Führungsstelle',     shape:'stelle',fill:C.zoll,kuerzel:'ZF', ink:'#fff', mobile:false,builtin:true},
    {id:'kp',  name:'Kontrollpunkt',           shape:'checkpoint', fill:C.zoll, kuerzel:'', ink:'#fff', mobile:false, builtin:true},
    {id:'ds',  name:'Datenstelle',             shape:'pc', fill:C.grau, kuerzel:'', ink:'#fff', mobile:false, builtin:true},
    {id:'ss',  name:'Sammelstelle',            shape:'people', fill:C.zoll, kuerzel:'', ink:'#fff', mobile:false, builtin:true},
  ]},
  {cat:'Rettungsdienst', items:[
    {id:'rtw', name:'Rettungswagen (RTW)',  shape:'unit', fill:'#fff', kuerzel:'RTW', ink:C.rd, mobile:true, builtin:true},
    {id:'nef', name:'Notarzt (NEF)',        shape:'unit', fill:'#fff', kuerzel:'NA',  ink:C.rd, mobile:true, builtin:true},
  ]},
  {cat:'THW / Technik', items:[
    {id:'thw', name:'THW-Einheit',          shape:'unit', fill:C.thw, kuerzel:'TZ', ink:'#fff', mobile:true, builtin:true},
    {id:'strom',name:'Notstrom',            shape:'point',fill:C.thw, kuerzel:'~',  ink:'#fff', mobile:false,builtin:true},
  ]},
  {cat:'Polizei', items:[
    {id:'pol', name:'Polizeikräfte',        shape:'unit', fill:C.pol, kuerzel:'PO', ink:'#fff', mobile:true, builtin:true},
  ]},
  {cat:'Gefahren & Objekte', items:[
    {id:'firma',  name:'Firma',              shape:'factory', fill:C.grau,   kuerzel:'', ink:'#fff', mobile:false, builtin:true},
    {id:'wohnung',name:'Privatwohnung',      shape:'house',   fill:C.grau,   kuerzel:'', ink:'#fff', mobile:false, builtin:true},
    {id:'bank',   name:'Bank',               shape:'bank',    fill:'#3fb950',kuerzel:'', ink:'#fff', mobile:false, builtin:true},
    {id:'stb',    name:'Steuerberater',      shape:'point',   fill:C.grau,   kuerzel:'StB', ink:'#0d1117', mobile:false, builtin:true},
    {id:'fzg',    name:'Fahrzeug',           shape:'car',     fill:C.grau,   kuerzel:'', ink:'#fff', mobile:false, builtin:true},
    {id:'zp_g',   name:'Zielperson (Gefahr)',shape:'diamond', fill:C.gefahr, kuerzel:'ZP', ink:'#0d1117', mobile:false, builtin:true, number:true},
    {id:'zp',     name:'Zielperson',         shape:'point',   fill:'#9d4edd',kuerzel:'ZP', ink:'#fff', mobile:false, builtin:true, number:true},
    {id:'gefahr', name:'Gefahrenstelle',     shape:'diamond', fill:C.gefahr, kuerzel:'!', ink:'#0d1117', mobile:false, builtin:true},
  ]},
];
function findItem(tid){for(const g of CATALOG)for(const it of g.items)if(it.id===tid)return it;}

/* ===== State ===== */
const S={markers:[],log:[],units:[],seq:1};
/* Gliederung: Führungsgruppe -> Einsatzabschnitte (EA) -> Unterabschnitte (UA)
   Jede Kraft bekommt Felder u.ea / u.ua (Freitext), deren Auswahllisten aus PLAN gespeist werden. */
const PLAN={eas:[
  {id:'ea1',name:'EA 1',fields:{},uas:[]},
  {id:'ea2',name:'EA 2',fields:{},uas:[]},
], statusCols:['moz','eoz','ds_begonnen','zp_angetroffen','zp_festgenommen','objekt_verlassen','entlassen']};
let planSeq=3;

const STATUS_CATALOG=[
  ['moz','MOZ'],['eoz','EOZ'],['ds_begonnen','DS begonnen'],
  ['zp_angetroffen','ZP angetroffen'],['zp_festgenommen','ZP festgenommen'],
  ['zp_ed','ZP ED-Behandlung'],['zp_haftrichter','ZP Haftrichter'],['zp_vernehmung','ZP Vernehmung'],
  ['tueroeffnung','Türöffnung'],['it_gefunden','IT gefunden'],['df_vor_ort','DF vor Ort'],
  ['barmittelfund','Barmittelfund'],['wertgegenstaende','Wertgegenstände'],['vam_vor_ort','VAM vor Ort'],
  ['vern_begonnen','Vernehmung begonnen'],['vern_abgeschlossen','Vernehmung abgeschlossen'],
  ['durchs_begonnen','Durchsuchung begonnen'],['durchs_abgeschlossen','Durchsuchung abgeschlossen'],
  ['fahrzeuge_durchsucht','Fahrzeuge durchsucht'],['objekt_verlassen','Objekt verlassen'],
  ['bereitstellungsraum','Bereitstellungsraum'],['entlassen','entlassen']
];
function statusLabel(key){const f=STATUS_CATALOG.find(x=>x[0]===key);return f?f[1]:key;}

/* ===== Mehrplatz-Synchronisation =====
   Rolle: 'fg' (Führungsgruppe, sieht/bearbeitet alles) oder EA-Name (nur eigener Abschnitt).
   Modell: Der Client sendet nach jeder lokalen Änderung den kompletten serialisierbaren
   Zustand an den Server; der Server spiegelt ihn an alle. Eingehender Zustand ersetzt den
   lokalen und rendert neu. Einfach und für Stabsbetrieb robust (last-write-wins). */
const NET={ ws:null, role:null, connected:false, applying:false, pushTimer:null, ready:false, singleplayer:false };

function serializeState(){
  return {
    op:{ name:opName.textContent.trim(), section:opSection.textContent.trim(), anlass:'', pf:'' },
    plan:{ eas:PLAN.eas.map(e=>({id:e.id,name:e.name,fields:e.fields||{},staerke:e.staerke||{h:0,g:0,m:0},
             uas:(e.uas||[]).map(u=>({id:u.id,name:u.name,fields:u.fields||{},staerke:u.staerke||{h:0,g:0,m:0}}))})), statusCols:(PLAN.statusCols||[]).slice() },
    units:S.units.map(u=>serUnit(u)),
    markers:S.markers.filter(m=>!S.units.includes(m)).map(m=>serUnit(m)),
    log:S.log,
    opStarted, opStartTime:opStartTime?opStartTime.getTime():null,
    seq:S.seq, planSeq
  };
}
function serUnit(u){const p=u.marker?u.marker.getLatLng():{lat:u._lat,lng:u._lng};
  return {id:u.id,type:u.type,name:u.name,label:u.label,call:u.call,status:u.status,
    ea:u.ea,ua:u.ua,auftrag:u.auftrag,s_f:u.s_f,s_u:u.s_u,s_m:u.s_m,beteiligte:u.beteiligte||0,
    start:u.start,lat:p.lat,lng:p.lng,mobile:S.units.includes(u)};}

function applyState(st){
  if(!st)return;
  NET.applying=true;
  // Kopf
  if(st.op){opName.textContent=st.op.name||'Einsatz';opSection.textContent=st.op.section||'';}
  // Plan
  PLAN.eas=(st.plan&&st.plan.eas||[]).map(e=>({id:e.id,name:e.name,fields:e.fields||{},staerke:e.staerke||{h:0,g:0,m:0},
    uas:(e.uas||[]).map(u=>({id:u.id,name:u.name,fields:u.fields||{},staerke:u.staerke||{h:0,g:0,m:0}}))}));
  if(st.plan&&Array.isArray(st.plan.statusCols))PLAN.statusCols=st.plan.statusCols.slice();
  planSeq=st.planSeq||planSeq;
  // Einsatzbeginn
  opStarted=!!st.opStarted; opStartTime=st.opStartTime?new Date(st.opStartTime):null;
  syncStartButton();
  // Kräfte + Marker als Leaflet neu projizieren (Diff nach id)
  const wanted=new Map();
  (st.units||[]).forEach(u=>wanted.set(u.id,{...u,mobile:true}));
  (st.markers||[]).forEach(u=>wanted.set(u.id,{...u,mobile:false}));
  // vorhandene entfernen, die nicht mehr da sind
  [...S.markers].forEach(rec=>{ if(!wanted.has(rec.id)){ if(rec.marker)map.removeLayer(rec.marker); }});
  const newMarkers=[],newUnits=[];
  wanted.forEach(d=>{
    let rec=S.markers.find(r=>r.id===d.id);
    const it=findItem(d.type)||{shape:'point',fill:'#8b98a8',kuerzel:'',ink:'#fff'};
    if(!rec){
      const marker=mapMarker([d.lat,d.lng],{draggable:true,icon:makeIcon(svgOf(it),d.call||d.label)}).addTo(map);
      rec={id:d.id,marker};
      marker.on('click',()=>{if(mode==='delete'){delMarker(rec.id);}else{openPopup(rec);}});
      marker.on('dragend',()=>{pushState();});
    }else{
      rec.marker.setLatLng([d.lat,d.lng]);
    }
    Object.assign(rec,{type:d.type,name:d.name,label:d.label,call:d.call,status:d.status,ea:d.ea,ua:d.ua,
      auftrag:d.auftrag,s_f:d.s_f,s_u:d.s_u,s_m:d.s_m,beteiligte:d.beteiligte||0,start:d.start,_lat:d.lat,_lng:d.lng});
    rec.marker.setIcon(makeIcon(svgOf(it),rec.call||rec.label));
    newMarkers.push(rec); if(d.mobile)newUnits.push(rec);
  });
  S.markers=newMarkers; S.units=newUnits; S.seq=st.seq||S.seq;
  // ETB
  S.log=(st.log||[]).map(e=>({id:e.id||("L"+(logSeq++)+"_"+Math.random().toString(36).slice(2,7)),ts:e.ts,by:e.by||"",type:e.type,text:e.text,prio:!!e.prio,done:!!e.done,decision:e.decision||null}));
  NET.applying=false;
  renderKraefte();renderLog();
  if(document.getElementById('viewPlan').classList.contains('active'))renderPlan();
  applyRolePermissions();
  if(window.__afterFirstState){window.__afterFirstState();}
}

function pushState(){
  if(NET.applying||!NET.connected)return;
  clearTimeout(NET.pushTimer);
  NET.pushTimer=setTimeout(()=>{
    try{NET.ws.send(JSON.stringify({t:'mut',op:'full',payload:{state:serializeState()}}));}catch(e){}
  },120);
}

function connectNET(role,onState){
  NET.role=role;
  // Einzelplatzbetrieb: Datei direkt geöffnet (file://) oder kein Host -> nicht verbinden
  if(location.protocol==='file:' || !location.host){
    NET.connected=false; NET.singleplayer=true;
    onState&&onState('err');
    return;
  }
  const proto=location.protocol==='https:'?'wss':'ws';
  const url=proto+'://'+location.host+'/ws'; // Cloudflare Worker: WebSocket-Route
  try{NET.ws=new WebSocket(url);}catch(e){onState&&onState('err');return;}
  NET.ws.onopen=()=>{NET.connected=true;NET.ws.send(JSON.stringify({t:'hello',role}));onState&&onState('ok');document.getElementById('netDisc').classList.remove('show');};
  NET.ws.onmessage=(ev)=>{try{const m=JSON.parse(ev.data);
    if(m.t==='state'){applyStateFromServer(m.state);}
    else if(m.t==='denied'){flashDenied();}
  }catch(e){}};
  NET.ws.onclose=()=>{NET.connected=false;
    if(NET.singleplayer)return;
    document.getElementById('netDisc').classList.add('show');setTimeout(()=>connectNET(role),1500);};
  NET.ws.onerror=()=>{onState&&onState('err');};
}
// Server schickt {op:'full'}-Zustände als komplettes Dokument
function applyStateFromServer(serverState){
  // serverState hat dieselbe Form wie serializeState() (im 'full'-Modell in payload gespiegelt)
  applyState(serverState.__full||serverState);
}
function flashDenied(){const b=document.getElementById('netDisc');b.textContent='Diese Änderung ist deiner Rolle nicht erlaubt.';b.style.background='var(--warn)';b.style.color='#3a2a00';b.classList.add('show');
  setTimeout(()=>{b.classList.remove('show');b.textContent='Verbindung zum Server verloren – Wiederverbindung…';b.style.background='';b.style.color='';},2500);}

function planEANames(){return PLAN.eas.map(e=>e.name).filter(Boolean);}
function planUANamesFor(eaName){const e=PLAN.eas.find(x=>x.name===eaName);return e?e.uas.map(u=>u.name).filter(Boolean):[];}
function allUANames(){const s=[];PLAN.eas.forEach(e=>e.uas.forEach(u=>{if(u.name)s.push(u.name);}));return s;}
const STATUS={1:{t:'einsatzbereit',c:'#3fb950'},2:{t:'Anmarsch',c:'#f4a300'},3:{t:'Kräftesammelstelle',c:'#f4c20d'},4:{t:'Bereitstellungsraum',c:'#4a90d9'},5:{t:'im Auftrag',c:'#e63946'},6:{t:'Marsch',c:'#9d4edd'},7:{t:'Ausfall',c:'#8b98a8'},8:{t:'Entlassung',c:'#5a6270'}};

/* ===== Karte ===== */
let map;
const MAP_OK = (typeof L !== 'undefined' && L && L.map);
let onlineLayer=null, customLayer=null, mapIsCustom=false, customBounds=null;
const GEO_CENTER=[51.660,6.964], GEO_ZOOM=14;
if(MAP_OK){
  try{
    map=L.map('map',{zoomControl:true,attributionControl:false,maxZoom:22}).setView(GEO_CENTER,GEO_ZOOM);
    // Farbige Kartenansicht (CARTO Voyager)
    onlineLayer=L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png',{maxZoom:20,subdomains:'abcd'});
    onlineLayer.addTo(map);
    L.control.attribution({prefix:false}).addAttribution('© OpenStreetMap · CARTO').addTo(map);
  }catch(e){ map=null; }
}
if(!map){
  const el=document.getElementById('map');
  if(el)el.innerHTML='<div style="position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:#8b98a8;font-size:13px;text-align:center;padding:20px">Kartendarstellung nicht verfügbar<br>(keine Internetverbindung beim Öffnen).<br>Eigenes Kartenmaterial oder alle übrigen Funktionen sind nutzbar.</div>';
  const noop=()=>fake; const fake={
    on:noop,off:noop,setView:noop,invalidateSize:noop,getCenter:()=>({lat:51.660,lng:6.964}),
    getZoom:()=>14,removeLayer:noop,addLayer:noop,addControl:noop,hasLayer:()=>false,fitBounds:noop,
    doubleClickZoom:{disable:noop,enable:noop}
  };
  map=fake;
  window.__mapDisabled=true;
}
function mapMarker(latlng,opts){
  if(window.__mapDisabled){ // Dummy-Marker ohne Kartenbezug
    let ll={lat:(latlng[0]!=null?latlng[0]:latlng.lat),lng:(latlng[1]!=null?latlng[1]:latlng.lng)};
    const m={_ll:ll,on:()=>m,addTo:()=>m,setLatLng:(x)=>{m._ll={lat:x[0]!=null?x[0]:x.lat,lng:x[1]!=null?x[1]:x.lng};return m;},
      getLatLng:()=>m._ll,setIcon:()=>m,bindPopup:()=>m,openPopup:()=>m,closePopup:()=>m};
    return m;
  }
  return L.marker(latlng,opts);
}
let mode='pan',measurePts=[],measureLine=null,areaPts=[],areaPoly=null;
function setMode(m){mode=m;document.querySelectorAll('.map-toolbar button').forEach(b=>{if(b.id!=='palToggle')b.classList.remove('active')});
  const b={pan:'tPan',measure:'tMeasure',area:'tArea',label:'tLabel',delete:'tDelete'};document.getElementById(b[m]).classList.add('active');
  const hint=document.getElementById('mapHint');const H={measure:'Punkte klicken – Doppelklick beendet die Messung',area:'Eckpunkte klicken – Doppelklick schließt den Bereich',label:'Auf Karte klicken, um Textmarke zu setzen',delete:'Zeichen anklicken, um es zu entfernen'};
  if(H[m]){hint.textContent=H[m];hint.classList.add('show')}else hint.classList.remove('show');
  document.getElementById('map').style.cursor=(m==='delete')?'crosshair':'';
  measurePts=[];areaPts=[];if(measureLine){map.removeLayer(measureLine);measureLine=null}}
tPan.onclick=()=>setMode('pan');tMeasure.onclick=()=>setMode('measure');tArea.onclick=()=>setMode('area');tLabel.onclick=()=>setMode('label');
tDelete.onclick=()=>setMode(mode==='delete'?'pan':'delete');
map.on('click',e=>{
  if(mode==='measure'){measurePts.push(e.latlng);if(measureLine)map.removeLayer(measureLine);
    measureLine=L.polyline(measurePts,{color:'#f4a300',weight:2,dashArray:'6 4'}).addTo(map);
    if(measurePts.length>1){let d=0;for(let i=1;i<measurePts.length;i++)d+=measurePts[i-1].distanceTo(measurePts[i]);
      measureLine.bindTooltip((d<1000?d.toFixed(0)+' m':(d/1000).toFixed(2)+' km'),{permanent:true,direction:'top'}).openTooltip();}}
  else if(mode==='area'){areaPts.push(e.latlng);if(areaPoly)map.removeLayer(areaPoly);
    areaPoly=L.polygon(areaPts,{color:'#e63946',fillColor:'#e63946',fillOpacity:.15,weight:2}).addTo(map);}
  else if(mode==='label'){const t=prompt('Text der Marke:');if(t&&!window.__mapDisabled){L.marker(e.latlng,{icon:L.divIcon({className:'tac-marker',html:\`<div class="lbl" style="position:static">\${esc(t)}</div>\`})}).addTo(map);}setMode('pan');}
});
map.on('dblclick',()=>{if(mode==='measure'||mode==='area')setMode('pan');});map.doubleClickZoom.disable();

/* ===== Größenveränderbare Frames (Resizer) ===== */
const viewEl=document.getElementById('viewEinsatz');
function setVar(name,val){viewEl.style.setProperty(name,val);}
function dragResize(handle,onMove){
  handle.addEventListener('mousedown',(e)=>{
    e.preventDefault();document.body.classList.add('resizing');
    const move=(ev)=>{onMove(ev);};
    const up=()=>{document.body.classList.remove('resizing');window.removeEventListener('mousemove',move);window.removeEventListener('mouseup',up);setTimeout(()=>map.invalidateSize(),50);};
    window.addEventListener('mousemove',move);window.addEventListener('mouseup',up);
  });
}
// Linke Palette-Breite
dragResize(document.getElementById('rzL'),(ev)=>{
  const left=viewEl.getBoundingClientRect().left;const w=Math.min(500,Math.max(120,ev.clientX-left));
  setVar('--pal',w+'px');
});
// Rechte Spalte-Breite (von rechts gezogen)
dragResize(document.getElementById('rzR'),(ev)=>{
  const right=viewEl.getBoundingClientRect().right;const w=Math.min(640,Math.max(220,right-ev.clientX));
  setVar('--right',w+'px');
});
// Mitte: Karte/Kräfte-Höhe
dragResize(document.getElementById('rzM'),(ev)=>{
  const cen=document.getElementById('centerCol').getBoundingClientRect();
  const top=Math.min(cen.height-120,Math.max(120,ev.clientY-cen.top));
  setVar('--maprow',top+'px');setVar('--krow','1fr');
});
// Rechts: ETB/Handlungsbedarf-Höhe
dragResize(document.getElementById('rzETB'),(ev)=>{
  const rc=document.getElementById('rightCol').getBoundingClientRect();
  const top=Math.min(rc.height-100,Math.max(100,ev.clientY-rc.top));
  setVar('--etbrow',top+'px');setVar('--hbrow','1fr');
});

/* ===== Ortssuche (OpenStreetMap Nominatim) ===== */
const searchBox=document.getElementById('mapSearch');
const searchResults=document.getElementById('searchResults');
async function doSearch(){
  const q=searchBox.value.trim();if(!q)return;
  searchResults.innerHTML='<div class="sr">Suche läuft…</div>';searchResults.classList.add('show');
  try{
    const r=await fetch('https://nominatim.openstreetmap.org/search?format=json&limit=6&q='+encodeURIComponent(q),{headers:{'Accept-Language':'de'}});
    const list=await r.json();
    if(!list.length){searchResults.innerHTML='<div class="sr">Kein Treffer.</div>';return;}
    searchResults.innerHTML='';
    list.forEach(item=>{
      const d=document.createElement('div');d.className='sr';d.textContent=item.display_name;
      d.onclick=()=>{map.setView([+item.lat,+item.lon],14);searchResults.classList.remove('show');searchBox.value='';};
      searchResults.appendChild(d);
    });
  }catch(e){searchResults.innerHTML='<div class="sr">Suche nicht verfügbar (keine Internetverbindung?).</div>';}
}
document.getElementById('mapSearchBtn').onclick=doSearch;
searchBox.addEventListener('keydown',e=>{if(e.key==='Enter'){e.preventDefault();doSearch();}});
document.addEventListener('click',e=>{if(!e.target.closest('.map-search')&&!e.target.closest('.search-results'))searchResults.classList.remove('show');});

/* ===== Eigenes Kartenmaterial (JPG/PDF) als zoombare Bild-Karte ===== */
const mapCustomBtn=document.getElementById('mapCustomLoad');
const mapBaseToggleBtn=document.getElementById('mapBaseToggle');
let hiddenFileInput=null;
if(mapCustomBtn){
  mapCustomBtn.onclick=()=>{
    if(window.__mapDisabled){alert('Karte ist nicht verfügbar (Leaflet nicht geladen). Eigenes Material kann daher nicht angezeigt werden.');return;}
    if(!hiddenFileInput){
      hiddenFileInput=document.createElement('input');hiddenFileInput.type='file';
      hiddenFileInput.accept='image/jpeg,image/png,image/*,application/pdf,.jpg,.jpeg,.png,.pdf';
      hiddenFileInput.style.display='none';document.body.appendChild(hiddenFileInput);
      hiddenFileInput.onchange=()=>{const f=hiddenFileInput.files[0];if(f)loadCustomMap(f);hiddenFileInput.value='';};
    }
    hiddenFileInput.click();
  };
}
async function loadCustomMap(file){
  try{
    let dataUrl, w, h;
    if(file.type==='application/pdf' || /\\.pdf$/i.test(file.name)){
      const r=await pdfFirstPageToImage(file); dataUrl=r.url; w=r.w; h=r.h;
    } else {
      const r=await imageFileToData(file); dataUrl=r.url; w=r.w; h=r.h;
    }
    setCustomMap(dataUrl,w,h);
    addLog('info',\`Eigenes Kartenmaterial geladen: \${file.name}\`);
  }catch(e){
    alert('Kartenmaterial konnte nicht geladen werden: '+(e&&e.message||e));
  }
}
function imageFileToData(file){
  return new Promise((resolve,reject)=>{
    const rd=new FileReader();
    rd.onload=()=>{ const img=new Image(); img.onload=()=>resolve({url:rd.result,w:img.naturalWidth,h:img.naturalHeight}); img.onerror=reject; img.src=rd.result; };
    rd.onerror=reject; rd.readAsDataURL(file);
  });
}
function loadPdfJs(){
  if(window.pdfjsLib)return Promise.resolve(window.pdfjsLib);
  return new Promise((resolve,reject)=>{
    const s=document.createElement('script');
    s.src='https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.min.js';
    s.onload=()=>{ try{window.pdfjsLib.GlobalWorkerOptions.workerSrc='https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js';}catch(e){} resolve(window.pdfjsLib); };
    s.onerror=()=>reject(new Error('PDF-Bibliothek konnte nicht geladen werden (Internet nötig).'));
    document.head.appendChild(s);
  });
}
async function pdfFirstPageToImage(file){
  const pdfjsLib=await loadPdfJs();
  const buf=await file.arrayBuffer();
  const pdf=await pdfjsLib.getDocument({data:buf}).promise;
  const page=await pdf.getPage(1);
  const scale=2; const viewport=page.getViewport({scale});
  const canvas=document.createElement('canvas');canvas.width=viewport.width;canvas.height=viewport.height;
  await page.render({canvasContext:canvas.getContext('2d'),viewport}).promise;
  return {url:canvas.toDataURL('image/png'),w:canvas.width,h:canvas.height};
}
function setCustomMap(dataUrl,w,h){
  // Bild in synthetische lat/lng-Bounds legen, damit Marker (lat/lng) weiter funktionieren.
  // Wir bilden das Bild auf ein Rechteck um GEO_CENTER ab (Seitenverhältnis erhalten).
  const aspect=w/h; const latSpan=0.08; const lngSpan=latSpan*aspect;
  const south=GEO_CENTER[0]-latSpan/2, north=GEO_CENTER[0]+latSpan/2;
  const west=GEO_CENTER[1]-lngSpan/2, east=GEO_CENTER[1]+lngSpan/2;
  customBounds=[[south,west],[north,east]];
  if(customLayer)map.removeLayer(customLayer);
  customLayer=L.imageOverlay(dataUrl,customBounds);
  // aktivieren
  mapIsCustom=true; applyBaseLayer();
  map.fitBounds(customBounds);
  mapBaseToggleBtn.style.display='';
  updateBaseToggleLabel();
}
function applyBaseLayer(){
  if(window.__mapDisabled)return;
  if(mapIsCustom){
    if(onlineLayer&&map.hasLayer(onlineLayer))map.removeLayer(onlineLayer);
    if(customLayer&&!map.hasLayer(customLayer))customLayer.addTo(map);
    customLayer&&customLayer.bringToBack();
  } else {
    if(customLayer&&map.hasLayer(customLayer))map.removeLayer(customLayer);
    if(onlineLayer&&!map.hasLayer(onlineLayer))onlineLayer.addTo(map);
  }
}
function updateBaseToggleLabel(){
  if(!mapBaseToggleBtn)return;
  mapBaseToggleBtn.textContent=mapIsCustom?'🌐 Online-Karte':'🗺️ Eigenes Material';
  mapBaseToggleBtn.title=mapIsCustom?'Zur Online-Karte wechseln':'Zum eigenen Kartenmaterial wechseln';
}
if(mapBaseToggleBtn){
  mapBaseToggleBtn.onclick=()=>{
    if(!customLayer){return;}
    mapIsCustom=!mapIsCustom; applyBaseLayer(); updateBaseToggleLabel();
    if(mapIsCustom&&customBounds)map.fitBounds(customBounds);
    setTimeout(()=>map.invalidateSize(),50);
  };
}

/* ===== Palette ein-/ausblenden ===== */
let palOpen=false;
function togglePalette(force){palOpen=(force!==undefined)?force:!palOpen;
  document.getElementById('viewEinsatz').classList.toggle('pal-open',palOpen);
  setTimeout(()=>map.invalidateSize(),200);}
palToggle.onclick=()=>togglePalette();

/* ===== Karte / Tabelle umschalten ===== */
let tableMode=false;
document.getElementById('viewToggle').onclick=()=>{
  tableMode=!tableMode;
  const pane=document.getElementById('mapPane');
  if(tableMode && statusOpen){ statusOpen=false; pane.classList.remove('show-status'); document.getElementById('statusToggle').textContent='📋 Status'; }
  pane.classList.toggle('show-table',tableMode);
  document.getElementById('viewToggle').textContent=tableMode?'🗺 Karte':'▦ Tabelle';
  if(tableMode)renderTableView(); else setTimeout(()=>map.invalidateSize(),60);
};
// Männchen-SVG (einfaches Personensymbol)
/* Männchen-Symbol in gegebener Farbe; failed=true zeigt ein rotes X darüber */
function figSvg(color,failed){
  const x=failed?'<path d="M1 1 L11 19 M11 1 L1 19" stroke="#e63946" stroke-width="2.2" fill="none"/>':'';
  return \`<svg viewBox="0 0 12 20"><circle cx="6" cy="4" r="3" fill="\${color}"/><path d="M2 19 v-6 a4 4 0 0 1 8 0 v6 z" fill="\${color}"/>\${x}</svg>\`;
}
function kfzSvg(failed){
  const x=failed?'<path d="M2 3 L18 15 M18 3 L2 15" stroke="#e63946" stroke-width="2.4" fill="none"/>':'';
  return \`<svg viewBox="0 0 20 18"><rect x="1" y="7" width="18" height="6" rx="1.5" fill="#8b98a8"/><path d="M4 7 l2-3 h6 l2 3 z" fill="#8b98a8"/><circle cx="6" cy="14" r="2.2" fill="#2a3140"/><circle cx="14" cy="14" r="2.2" fill="#2a3140"/>\${x}</svg>\`;
}
const RANK_COLOR={h:'#f4c20d',g:'#8b98a8',m:'#4a90d9'}; // höher=gelb, gehoben=grau, mittel=blau
const RANK_LABEL={h:'höherer Dienst',g:'gehobener Dienst',m:'mittlerer Dienst'};
/* AK-Symbole: aktive (farbig, nach Laufbahn) + ausgefallene (mit X), 5er-Gruppierung übergreifend */
function akFigures(node){
  const st=node.staerke||{h:0,g:0,m:0};
  const aus=node.fields||{};
  const items=[]; // {rank, failed}
  ['h','g','m'].forEach(r=>{ for(let i=0;i<(+st[r]||0);i++) items.push({rank:r,failed:false}); });
  ['h','g','m'].forEach(r=>{ const a=+aus['ak_aus_'+r]||0; for(let i=0;i<a;i++) items.push({rank:r,failed:true}); });
  if(!items.length)return '<div class="ak-figs"><span class="ak-total">0</span></div>';
  const active=(+st.h||0)+(+st.g||0)+(+st.m||0);
  const failed=(+aus.ak_aus_h||0)+(+aus.ak_aus_g||0)+(+aus.ak_aus_m||0);
  let out=\`<div class="ak-figs"><span class="ak-total" title="Anzahl AK gesamt">\${active}</span>\`;let i=0;
  while(i<items.length){
    out+='<div class="grp">';
    for(let j=0;j<5 && i<items.length;j++,i++){
      const it=items[i];
      out+=\`<span class="fig ak-fig" data-node="\${node.id}" data-rank="\${it.rank}" data-failed="\${it.failed?1:0}" title="\${RANK_LABEL[it.rank]} – klicken für Ausfall">\${figSvg(RANK_COLOR[it.rank],it.failed)}</span>\`;
    }
    out+='</div>';
  }
  out+=\`\${failed?\`<span class="ak-num">(+\${failed} Ausfall)</span>\`:''}</div>\`;
  return out;
}
/* KFZ-Symbole: gesamt = kfz (aus Plan); die letzten kfz_aus sind ausgefallen (mit X) */
function kfzFigures(node){
  const total=parseInt((node.fields&&node.fields.kfz)||'0')||0;
  const aus=+((node.fields&&node.fields.kfz_aus)||0);
  if(total===0)return '<div class="ak-figs"><span class="ak-total">0</span></div>';
  let out=\`<div class="ak-figs kfz-figs"><span class="ak-total" title="Anzahl Fahrzeuge aktiv">\${total-aus}</span>\`;let i=0;
  while(i<total){
    out+='<div class="grp">';
    for(let j=0;j<5 && i<total;j++,i++){
      const failed=i>=(total-aus);
      out+=\`<span class="fig kfz-fig" data-node="\${node.id}" data-idx="\${i}" data-failed="\${failed?1:0}" title="KFZ – klicken für Ausfall">\${kfzSvg(failed)}</span>\`;
    }
    out+='</div>';
  }
  out+=\`\${aus?\`<span class="ak-num">(+\${aus} Ausfall)</span>\`:''}</div>\`;
  return out;
}
// Freitext-/Zahlfelder pro Abschnitt liegen in node.fields unter tv_*-Schlüsseln
function tvCell(node,key,ph){
  node.fields=node.fields||{};
  const v=node.fields[key]||'';
  return \`<textarea class="tv-cell-in" data-tvnode="\${node.id}" data-tvkey="\${key}" rows="1" placeholder="\${ph||''}">\${esc(v)}</textarea>\`;
}
function abschnittTable(node,list){
  const bet=sumBeteiligte(list); // fixe Anzahl Beteiligte aus Eingesetzte Kräfte
  const owns={AK:akFigures(node), KFZ:kfzFigures(node), Ausstattung:tvCell(node,'tv_own_Ausstattung','Ausstattung…')};
  const rows=[['AK','AK'],['KFZ','KFZ'],['Ausstattung','Ausstattung']];
  let body='';
  rows.forEach(([lab,rk],idx)=>{
    const betPrefix=(rk==='AK')?\`<div class="tv-bet" title="Beteiligte aus Eingesetzte Kräfte">Beteiligte: \${bet}</div>\`:'';
    // OWi/SV und Sonstiges: je EIN zusammengefasstes Feld über alle drei Zeilen (nur in der ersten Zeile ausgeben)
    const merged=(idx===0)?\`
      <td class="thick-l tv-merge" rowspan="3">\${tvCell(node,'tv_owi','OWi / SV…')}</td>
      <td class="tv-merge" rowspan="3">\${tvCell(node,'tv_sonst','Sonstiges…')}</td>\`:'';
    body+=\`<tr>
      <td class="rowlab">\${lab}</td>
      <td>\${owns[rk]}</td>
      <td class="thick-l">\${betPrefix}\${tvCell(node,'tv_other_'+rk)}</td>\${merged}
    </tr>\`;
  });
  return \`<div class="tv-abschnitt"><h3>\${esc(node.name)}</h3>
    <table class="tv"><colgroup><col class="c1"><col><col><col><col></colgroup>
      <thead><tr><th></th><th>eigene Kräfte</th><th class="thick-l">andere Kräfte</th><th class="thick-l">OWi/SV</th><th>Sonstiges</th></tr></thead>
      <tbody>\${body}</tbody>
    </table></div>\`;
}
function renderTableView(){
  const box=document.getElementById('tableViewBody');if(!box)return;
  const US=visibleUnits();
  let eas;
  if(roleIsFG() && level==='fg'){ eas=PLAN.eas.slice(); }
  else { const only=roleIsFG()?level:NET.role; eas=PLAN.eas.filter(e=>e.name===only); }
  if(!eas.length){box.innerHTML='<div class="empty">Keine Einsatzabschnitte vorhanden. Im „Grafischen Einsatzplan“ anlegen.</div>';return;}
  let html='';
  eas.forEach(ea=>{
    const eaList=US.filter(u=>(u.ea||'')===ea.name);
    html+=abschnittTable(ea,eaList);
    (ea.uas||[]).forEach(ua=>{
      const uaList=eaList.filter(u=>(u.ua||'')===ua.name);
      html+=abschnittTable(ua,uaList);
    });
  });
  box.innerHTML=html;
  // Freitext-Bindings
  box.querySelectorAll('[data-tvnode]').forEach(inp=>{
    inp.addEventListener('change',()=>{
      const node=findPlanNode(inp.dataset.tvnode);
      if(node){node.fields=node.fields||{};node.fields[inp.dataset.tvkey]=inp.value;pushState();}
    });
  });
  // AK-Ausfall-Klicks
  box.querySelectorAll('.ak-fig').forEach(el=>{
    el.onclick=()=>toggleAkAusfall(el.dataset.node,el.dataset.rank,el.dataset.failed==='1');
  });
  // KFZ-Ausfall-Klicks
  box.querySelectorAll('.kfz-fig').forEach(el=>{
    el.onclick=()=>toggleKfzAusfall(el.dataset.node,el.dataset.failed==='1');
  });
}
function toggleAkAusfall(nodeId,rank,wasFailed){
  const node=findPlanNode(nodeId);if(!node)return;
  node.staerke=node.staerke||{h:0,g:0,m:0};node.fields=node.fields||{};
  const ak='ak_aus_'+rank;const cur=+node.fields[ak]||0;
  if(!wasFailed){
    // aktives Symbol -> Ausfall: Stärke -1, Ausfallzähler +1
    if((+node.staerke[rank]||0)<=0)return;
    node.staerke[rank]=(+node.staerke[rank]||0)-1;
    node.fields[ak]=cur+1;
    addLog('lage',\`Ausfall \${node.name}: 1 \${RANK_LABEL[rank]} (Stärke \${rankShort(rank)} → \${node.staerke[rank]})\`,true);
  }else{
    // Ausfall zurücknehmen: Stärke +1, Ausfallzähler -1
    if(cur<=0)return;
    node.staerke[rank]=(+node.staerke[rank]||0)+1;
    node.fields[ak]=cur-1;
    addLog('lage',\`Ausfall zurückgenommen \${node.name}: 1 \${RANK_LABEL[rank]} (Stärke \${rankShort(rank)} → \${node.staerke[rank]})\`,true);
  }
  renderKraefte(); // aktualisiert Tabelle + Eingesetzte Kräfte
  if(document.getElementById('viewPlan').classList.contains('active'))renderPlan();
  pushState();
}
function rankShort(r){return r==='h'?'höh.':r==='g'?'geh.':'mittl.';}
function toggleKfzAusfall(nodeId,wasFailed){
  const node=findPlanNode(nodeId);if(!node)return;
  node.fields=node.fields||{};
  const total=parseInt(node.fields.kfz||'0')||0;
  let aus=+node.fields.kfz_aus||0;
  if(!wasFailed){ if(aus>=total)return; aus++; addLog('lage',\`Ausfall \${node.name}: 1 KFZ (aktiv \${total-aus} von \${total})\`,true); }
  else{ if(aus<=0)return; aus--; addLog('lage',\`Ausfall zurückgenommen \${node.name}: 1 KFZ (aktiv \${total-aus} von \${total})\`,true); }
  node.fields.kfz_aus=aus;
  renderKraefte();
  if(document.getElementById('viewPlan').classList.contains('active'))renderPlan();
  pushState();
}
function findPlanNode(id){
  for(const e of PLAN.eas){ if(e.id===id)return e; for(const u of (e.uas||[])) if(u.id===id)return u; }
  return null;
}

/* ===== Status-Übersicht (im mittleren Bereich, umschaltbar wie die Tabelle) ===== */
let statusOpen=false;
let statusExpanded={};
function toggleStatus(force){
  statusOpen=(force!==undefined)?force:!statusOpen;
  const pane=document.getElementById('mapPane');
  if(statusOpen){
    if(tableMode){ tableMode=false; pane.classList.remove('show-table'); document.getElementById('viewToggle').textContent='▦ Tabelle'; }
    pane.classList.add('show-status');
    renderStatus();
  } else {
    pane.classList.remove('show-status');
    setTimeout(()=>{ if(!tableMode && map && map.invalidateSize) map.invalidateSize(); },60);
  }
  document.getElementById('statusToggle').textContent=statusOpen?'🗺 Karte':'📋 Status';
}
document.getElementById('statusToggle').onclick=()=>toggleStatus();

// Darf die aktuelle Rolle diesen Abschnitt (EA-Name) abhaken?
function canEditStatus(eaName){ return roleIsFG() || NET.role===eaName; }

function eaStatusCols(ea){
  const c=ea.fields&&ea.fields.statusCols;
  if(Array.isArray(c))return c;
  return (PLAN.statusCols||[]).slice(); // Vorgabe für neue/ohne eigene Auswahl
}
function renderStatus(){
  const body=document.getElementById('statusBody');
  // Sichtbare EA (FG: alle; EA: nur eigener)
  let eas;
  if(roleIsFG()) eas=PLAN.eas.slice();
  else eas=PLAN.eas.filter(e=>e.name===NET.role);
  if(!eas.length){ body.innerHTML='<div class="empty" style="padding:20px">Keine Einsatzabschnitte vorhanden. Im „Grafischen Einsatzplan“ anlegen.</div>'; return; }
  // Pro EA ein eigener Block mit eigener Kopfzeile
  let html='';
  eas.forEach(ea=>{
    const cols=eaStatusCols(ea);
    const uas=ea.uas||[];
    const open=!!statusExpanded[ea.id];
    const canCfg=roleIsFG();
    const head='<tr><th class="rowhd">Abschnitt</th>'+cols.map(k=>\`<th class="colhd"><span>\${esc(statusLabel(k))}</span></th>\`).join('')+'<th class="sonsthd">Sonstige</th></tr>';
    let rows=statusRow(ea,'ea',ea.name,cols,uas.length,open);
    if(open) uas.forEach(ua=>{ rows+=statusRow(ua,'ua',ea.name,cols,0,false); });
    const cfgBtn=canCfg?\`<button class="h2act ea-cols-btn" data-eacols="\${ea.id}" title="Spalten für \${esc(ea.name)} festlegen">⚙ Spalten</button>\`:'';
    html+=\`<div class="status-block" data-block="\${ea.id}">
      <div class="status-block-hd"><span>\${esc(ea.name)}</span>\${cfgBtn}</div>
      <table class="status"><thead>\${head}</thead><tbody>\${rows}</tbody></table>
    </div>\`;
  });
  body.innerHTML=html;
  // EA auf-/zuklappen
  body.querySelectorAll('.ea-toggle').forEach(el=>{
    el.onclick=(e)=>{ e.stopPropagation(); const id=el.dataset.ea; statusExpanded[id]=!statusExpanded[id]; renderStatus(); };
  });
  // Spalten pro EA konfigurieren
  body.querySelectorAll('.ea-cols-btn').forEach(b=>{
    b.onclick=()=>openEaColsConfig(b.dataset.eacols);
  });
  // Klicks zum Abhaken; automatische EA-Zellen sind nicht klickbar
  body.querySelectorAll('td.stcell').forEach(td=>{
    if(td.classList.contains('ro')||td.classList.contains('auto'))return;
    td.onclick=()=>toggleStatusCell(td.dataset.node,td.dataset.key);
  });
  // Sonstige-Freitext
  body.querySelectorAll('textarea[data-sonst]').forEach(t=>{
    t.onchange=()=>{ const n=findPlanNode(t.dataset.sonst); if(n){n.fields=n.fields||{};n.fields.status_sonst=t.value;pushState();} };
  });
}
function statusRow(node,kind,eaName,cols,uaCount,open){
  node.fields=node.fields||{};
  let label;
  const eaAuto = (kind==='ea' && uaCount>0); // EA mit UAs -> Spalten automatisch aus UAs
  if(kind==='ea'){
    const toggle=uaCount>0
      ? \`<span class="ea-toggle" data-ea="\${node.id}" title="Unterabschnitte ein-/ausklappen">\${open?'▾':'▸'}</span>\`
      : \`<span class="ea-toggle-empty"></span>\`;
    label = uaCount>0
      ? \`\${toggle}<span class="ea-sum">Gesamt (\${uaCount} UA)</span>\`
      : \`\${toggle}\${esc(node.name)}\`;
  } else {
    label=\`↳ \${esc(node.name||'(UA)')}\`;
  }
  const ro=!canEditStatus(eaName);
  const cells=cols.map(k=>{
    if(eaAuto){
      // automatisch: abgehakt, wenn ALLE UAs diese Spalte gesetzt haben; Zeitstempel = spätester
      const uas=node.uas||[];
      const stamps=uas.map(u=>(u.fields&&u.fields['st_'+k])||'');
      const allDone = uas.length>0 && stamps.every(s=>!!s);
      const latest = allDone ? stamps.slice().sort().pop() : '';
      const cls='stcell auto'+(allDone?' done':'');
      return \`<td class="\${cls}" title="\${esc(statusLabel(k))} – automatisch (alle UA abgehakt)">\${allDone?('✓ '+esc(latest)):''}</td>\`;
    }
    const val=node.fields['st_'+k]||'';
    const cls='stcell'+(val?' done':'')+(ro?' ro':'');
    return \`<td class="\${cls}" data-node="\${node.id}" data-key="\${k}" title="\${esc(statusLabel(k))} – klicken zum Ab-/Anhaken">\${val?('✓ '+esc(val)):''}</td>\`;
  }).join('');
  const sonst=\`<td class="sonst"><textarea data-sonst="\${node.id}" rows="1" \${ro?'readonly':''} placeholder="…">\${esc(node.fields.status_sonst||'')}</textarea></td>\`;
  return \`<tr class="\${kind}-row"><td class="rowhd">\${label}</td>\${cells}\${sonst}</tr>\`;
}
function toggleStatusCell(nodeId,key){
  const node=findPlanNode(nodeId);if(!node)return;
  // Rollen-Check: EA darf nur eigenen Abschnitt; ermitteln, zu welchem EA der Knoten gehört
  const eaName=eaNameOfNode(nodeId);
  if(!canEditStatus(eaName))return;
  node.fields=node.fields||{};
  const k='st_'+key;
  if(node.fields[k]){ delete node.fields[k]; addLog('lage',\`Status „\${statusLabel(key)}“ zurückgenommen – \${statusPathLabel(nodeId)}\`); }
  else { node.fields[k]=nowHM(); addLog('lage',\`Status „\${statusLabel(key)}“ erledigt – \${statusPathLabel(nodeId)}\`); }
  renderStatus();
  pushState();
}
function eaNameOfNode(nodeId){
  for(const e of PLAN.eas){ if(e.id===nodeId)return e.name; for(const u of (e.uas||[])) if(u.id===nodeId)return e.name; }
  return null;
}
function statusPathLabel(nodeId){
  for(const e of PLAN.eas){
    if(e.id===nodeId)return e.name;
    for(const u of (e.uas||[])) if(u.id===nodeId)return e.name+' / '+(u.name||'UA');
  }
  return '';
}

/* Spalten-Konfiguration (nur Führungsgruppe) */
function openEaColsConfig(eaId){
  if(!roleIsFG()){ flashInfo('Nur die Führungsgruppe kann die Spalten festlegen.'); return; }
  const ea=PLAN.eas.find(e=>e.id===eaId); if(!ea)return;
  ea.fields=ea.fields||{};
  const box=document.getElementById('statusConfig');
  const list=document.getElementById('statusConfigList');
  document.getElementById('statusConfigTitle').textContent='Spalten für '+ea.name;
  const sel=new Set(eaStatusCols(ea));
  list.innerHTML=STATUS_CATALOG.map(([k,lab])=>\`<label class="sc-item"><input type="checkbox" data-k="\${k}" \${sel.has(k)?'checked':''}> \${esc(lab)}</label>\`).join('');
  box.classList.remove('hidden');
  list.querySelectorAll('input[data-k]').forEach(cb=>{
    cb.onchange=()=>{
      const k=cb.dataset.k;
      let cols=eaStatusCols(ea);
      if(cb.checked){ if(!cols.includes(k))cols=cols.concat([k]); }
      else { cols=cols.filter(x=>x!==k); }
      // Reihenfolge nach Katalog sortieren, pro EA speichern
      ea.fields.statusCols=STATUS_CATALOG.map(x=>x[0]).filter(x=>cols.includes(x));
      renderStatus();pushState();
    };
  });
}
document.getElementById('statusConfigDone').onclick=()=>document.getElementById('statusConfig').classList.add('hidden');

let palEditing=false;
palEdit.onclick=()=>{palEditing=!palEditing;document.getElementById('palcol').classList.toggle('editing',palEditing);
  palEdit.textContent=palEditing?'Fertig':'Bearbeiten';};

/* ===== Palette render ===== */
function renderPalette(){const pal=document.getElementById('palette');pal.innerHTML='';
  CATALOG.forEach(group=>{
    const wrap=document.createElement('div');wrap.className='cat';
    const head=document.createElement('div');head.className='cat-title';
    head.innerHTML=\`<span>\${esc(group.cat)}</span>\`;
    const add=document.createElement('button');add.className='addbtn';add.textContent='+';add.title='Zeichen zu '+group.cat+' hinzufügen';
    add.onclick=()=>openModal(group.cat);head.appendChild(add);
    wrap.appendChild(head);
    const grid=document.createElement('div');grid.className='sym-grid';
    group.items.forEach(it=>{
      const el=document.createElement('div');el.className='sym';el.title=it.name;el.innerHTML=svgOf(it);
      el.onclick=()=>{if(!palEditing)placeSymbol(it);};
      const del=document.createElement('button');del.className='del'+(it.builtin?' builtin':'');del.textContent='✕';del.title='Entfernen';
      del.onclick=(ev)=>{ev.stopPropagation();removeSign(group,it);};
      el.appendChild(del);grid.appendChild(el);
    });
    wrap.appendChild(grid);pal.appendChild(wrap);});}
function removeSign(group,it){
  group.items.splice(group.items.indexOf(it),1);
  renderPalette();renderCfgList();
}

/* ===== Zeichen setzen ===== */
let callCounters={},numberCounters={};
function defaultCall(it){callCounters[it.id]=(callCounters[it.id]||0)+1;return (it.kuerzel||'EM')+' '+String(callCounters[it.id]).padStart(2,'0');}
function makeIcon(svg,label){if(window.__mapDisabled)return null;return L.divIcon({className:'tac-marker',iconSize:[44,44],iconAnchor:[22,22],html:\`<div class="wrap">\${svg}<div class="lbl">\${esc(label)}</div></div>\`});}
function placeSymbol(it){
  const c=map.getCenter();const id='m'+(S.seq++);
  let label=it.name.replace(/\\s*\\(.*\\)/,'').split('/')[0];
  if(it.number){numberCounters[it.id]=(numberCounters[it.id]||0)+1;label=(it.kuerzel||label)+' '+numberCounters[it.id];}
  const call=it.mobile?defaultCall(it):'';
  const shownLabel=call||label;
  const marker=mapMarker(c,{draggable:true,icon:makeIcon(svgOf(it),shownLabel)}).addTo(map);
  const rec={id,type:it.id,name:it.name,label:shownLabel,call,status:3,ea:(PLAN.eas[0]&&PLAN.eas[0].name)||'',ua:'',auftrag:'',s_f:0,s_u:0,s_m:0,beteiligte:0,start:nowHM(),marker};
  if(it.mobile){S.units.push(rec);renderKraefte();}
  marker.on('click',()=>{if(mode==='delete'){delMarker(rec.id);}else{openPopup(rec);}});
  marker.on('dragend',()=>{pushState();});
  S.markers.push(rec);
  addLog('meldung',\`\${it.name}\${it.number?' '+label.replace(/\\D+/,'').trim():''} auf Lage gesetzt\`);
  pushState();
}
function eaOptionsHTML(sel){
  const names=planEANames();
  let o='<option value="">– Einsatzabschnitt –</option>';
  o+=names.map(n=>\`<option value="\${esc(n)}" \${sel===n?'selected':''}>\${esc(n)}</option>\`).join('');
  // EA, der (noch) nicht im Plan ist, aber am Datensatz hängt
  if(sel&&!names.includes(sel))o+=\`<option value="\${esc(sel)}" selected>\${esc(sel)} (nicht im Plan)</option>\`;
  return o;
}
function uaOptionsHTML(eaName,sel){
  const names=planUANamesFor(eaName);
  let o='<option value="">– kein Unterabschnitt –</option>';
  o+=names.map(n=>\`<option value="\${esc(n)}" \${sel===n?'selected':''}>\${esc(n)}</option>\`).join('');
  if(sel&&!names.includes(sel))o+=\`<option value="\${esc(sel)}" selected>\${esc(sel)} (nicht im Plan)</option>\`;
  return o;
}
window.popEAChange=id=>{const rec=S.markers.find(m=>m.id===id);const ea=document.getElementById('pea').value;
  const uaSel=document.getElementById('pua');
  const keep=(ea===rec.ea)?rec.ua:'';
  if(uaSel)uaSel.innerHTML=uaOptionsHTML(ea,keep);
  popUpdateCur();};
window.popUpdateCur=()=>{const ea=document.getElementById('pea');const ua=document.getElementById('pua');const cur=document.getElementById('pcur');
  if(!ea||!cur)return;const e=ea.value,u=ua?ua.value:'';cur.textContent=e?(e+(u?' / '+u:'')):'nicht zugeordnet';};
function openPopup(rec){
  const isUnit=S.units.includes(rec);
  const statOpts=Object.entries(STATUS).map(([k,v])=>\`<option value="\${k}" \${rec.status==k?'selected':''}>Status \${k} – \${v.t}</option>\`).join('');
  const ziel=rec.ea?(rec.ea+(rec.ua?' / '+rec.ua:'')):'nicht zugeordnet';
  rec.marker.bindPopup(\`<div class="pop"><h4>\${esc(rec.name)}</h4>
    <label>Funkrufname / Bezeichnung</label><input id="pc" value="\${esc(rec.call||rec.label)}"/>
    <div class="pop-sec">Zuordnung <span class="pop-cur" id="pcur">\${esc(ziel)}</span></div>
    <label>Einsatzabschnitt</label><select id="pea" onchange="popEAChange('\${rec.id}')">\${eaOptionsHTML(rec.ea)}</select>
    <label>Unterabschnitt</label><select id="pua" onchange="popUpdateCur()">\${uaOptionsHTML(rec.ea,rec.ua)}</select>
    \${isUnit?\`<label>Status</label><select id="ps">\${statOpts}</select>\`:''}
    <div class="row"><button class="sv" onclick="savePopup('\${rec.id}')">Speichern</button>
    <button class="del" onclick="delMarker('\${rec.id}')">Entfernen</button></div></div>\`,{minWidth:230}).openPopup();
}
window.savePopup=id=>{const rec=S.markers.find(m=>m.id===id);const v=document.getElementById('pc').value.trim();
  if(v){rec.call=v;rec.label=v}const ps=document.getElementById('ps');
  if(ps){const nv=+ps.value;if(nv!==rec.status){rec.status=nv;addLog('meldung',\`\${rec.call||rec.name}: Status \${nv} – \${STATUS[nv].t}\`);}}
  const pea=document.getElementById('pea'),pua=document.getElementById('pua');
  if(pea){const oldEA=rec.ea,oldUA=rec.ua;rec.ea=pea.value;rec.ua=pua?pua.value:'';
    if(rec.ea!==oldEA||rec.ua!==oldUA){
      const ziel=rec.ea?(rec.ea+(rec.ua?' / '+rec.ua:'')):'(ohne Zuordnung)';
      addLog('meldung',\`\${rec.call||rec.name}: zugeordnet zu \${ziel}\`);}}
  rec.marker.setIcon(makeIcon(svgOf(findItem(rec.type)),rec.call||rec.label));rec.marker.closePopup();renderKraefte();
  addLog('info',\`\${rec.call||rec.name}: aktualisiert\`);pushState();};
window.delMarker=id=>{const i=S.markers.findIndex(m=>m.id===id);const rec=S.markers[i];if(!rec)return;
  map.removeLayer(rec.marker);S.markers.splice(i,1);const ui=S.units.indexOf(rec);if(ui>=0)S.units.splice(ui,1);
  renderKraefte();addLog('info',\`\${rec.call||rec.name}: von Lage entfernt\`);pushState();};

/* ===== Führungsebene ===== */
// level: 'fg' = Führungsgruppe (nur EA, kumuliert, aufklappbar)
//        oder ein EA-Name (zeigt diesen EA + seine UA)
let level='fg';
let expandedEA={}; // EA-Name -> aufgeklappt?
function buildLevelSelect(){
  const sel=document.getElementById('levelSelect');if(!sel)return;
  if(!roleIsFG()){
    // EA-Arbeitsplatz: nur die eigene Ebene, kein Umschalten
    level=NET.role;
    sel.innerHTML=\`<option value="\${esc(NET.role)}">Ebene: \${esc(NET.role)}</option>\`;
    sel.value=NET.role;sel.disabled=true;sel.onchange=null;return;
  }
  sel.disabled=false;
  const prev=level;
  sel.innerHTML='<option value="fg">Ebene: Führungsgruppe</option>'+
    PLAN.eas.filter(e=>e.name).map(e=>\`<option value="\${esc(e.name)}">Ebene: \${esc(e.name)}</option>\`).join('');
  if(prev!=='fg'&&!planEANames().includes(prev))level='fg';
  sel.value=level;
  sel.onchange=()=>{level=sel.value;renderKraefte();};
}
function sumStaerke(list){const f=list.reduce((a,u)=>a+u.s_f,0),un=list.reduce((a,u)=>a+u.s_u,0),m=list.reduce((a,u)=>a+u.s_m,0);
  return{f,u:un,m,ges:f+un+m};}
function sumBeteiligte(list){return list.reduce((a,u)=>a+(+u.beteiligte||0),0);}

/* ===== Kräfte-Tabelle ===== */
function statusClass(s){
  if([1,3,4,8].includes(s))return 'st-green';
  if([2,5,6].includes(s))return 'st-yellow';
  if(s===7)return 'st-red';
  return '';
}
const COLSPAN=10;
function unitRow(u){
  const statOpts=Object.entries(STATUS).map(([k,v])=>\`<option value="\${k}" \${u.status==k?'selected':''}>\${k} · \${v.t}</option>\`).join('');
  const tr=document.createElement('tr');
  tr.className=statusClass(u.status);tr.dataset.row=u.id;
  tr.innerHTML=\`
    <td class="ea"><input value="\${esc(u.ea)}" list="dlEA" data-f="ea"></td>
    <td class="ea"><input value="\${esc(u.ua)}" list="dlUA-\${esc(u.ea)}" data-f="ua" placeholder="–"></td>
    <td class="rn"><input value="\${esc(u.call||u.label)}" data-f="call"></td>
    <td><select data-f="status">\${statOpts}</select></td>
    <td><input value="\${esc(u.auftrag)}" data-f="auftrag" placeholder="Auftrag…"></td>
    <td class="staerke"><input value="\${u.s_f}/\${u.s_u}/\${u.s_m}//\${u.s_f+u.s_u+u.s_m}" data-f="staerke"></td>
    <td><input value="\${+u.beteiligte||0}" data-f="beteiligte" style="width:52px;min-width:52px;text-align:center"></td>
    <td class="dur"><input value="\${esc(u.start)}" data-f="start" style="width:64px;min-width:64px"></td>
    <td class="dur" data-dur="\${u.id}">\${durationSince(u.start)}</td>
    <td><button class="rem" title="Aus Einsatz nehmen">✕</button></td>\`;
  tr.querySelectorAll('[data-f]').forEach(inp=>inp.addEventListener('change',()=>applyField(u,inp.dataset.f,inp.value,inp)));
  tr.querySelector('.rem').onclick=()=>{if(u.marker){map.removeLayer(u.marker);const mi=S.markers.indexOf(u);if(mi>=0)S.markers.splice(mi,1);}
    S.units.splice(S.units.indexOf(u),1);renderKraefte();addLog('info',\`\${u.call||u.name}: aus Einsatz genommen\`);};
  return tr;
}
function planEAObj(name){return PLAN.eas.find(e=>e.name===name);}
function planUAObj(eaName,uaName){const e=planEAObj(eaName);return e?e.uas.find(u=>u.name===uaName):null;}
function staerkeCellHTML(obj){
  // editierbares x/x/x//xx-Feld für ein Plan-Objekt (EA oder UA); Summe automatisch
  const s=(obj&&obj.staerke)||{h:0,g:0,m:0};const ges=(+s.h||0)+(+s.g||0)+(+s.m||0);
  return \`<span class="stw"><input class="sx" data-sp="h" value="\${+s.h||0}" title="höherer Dienst"><span class="sep">/</span>\`+
    \`<input class="sx" data-sp="g" value="\${+s.g||0}" title="gehobener Dienst"><span class="sep">/</span>\`+
    \`<input class="sx" data-sp="m" value="\${+s.m||0}" title="mittlerer Dienst"><span class="sep">//</span>\`+
    \`<span class="sges">\${ges}</span></span>\`;
}
function bindStaerkeCell(td,obj){
  if(!obj)return;obj.staerke=obj.staerke||{h:0,g:0,m:0};
  td.querySelectorAll('.sx').forEach(inp=>{
    inp.addEventListener('change',()=>{obj.staerke[inp.dataset.sp]=Math.max(0,parseInt(inp.value)||0);inp.value=obj.staerke[inp.dataset.sp];
      td.querySelector('.sges').textContent=(+obj.staerke.h||0)+(+obj.staerke.g||0)+(+obj.staerke.m||0);renderPlan();pushState();});
    inp.addEventListener('keydown',e=>{if(e.key==='Enter'){e.preventDefault();inp.blur();}});
  });
}
function eaSummaryRow(eaName,list,noToggle){
  const bet=sumBeteiligte(list);
  const open=!!expandedEA[eaName];
  const eaObj=planEAObj(eaName);
  const tr=document.createElement('tr');tr.style.fontWeight='700';
  tr.style.background='var(--panel-2)';
  const marker=noToggle?'':(open?'▾ ':'▸ ');
  tr.innerHTML=\`
    <td colspan="2"><span style="color:#4a90d9">\${marker}\${esc(eaName||'(ohne EA)')}</span></td>
    <td colspan="3" style="color:var(--ink-dim);font-weight:400">\${list.length} Einheit\${list.length===1?'':'en'}</td>
    <td class="staerke">\${eaObj?staerkeCellHTML(eaObj):(()=>{const st=sumStaerke(list);return \`<span style="font-family:var(--mono)">\${st.f}/\${st.u}/\${st.m}//\${st.ges}</span>\`;})()}</td>
    <td style="text-align:center;font-family:var(--mono)">\${bet}</td>
    <td colspan="3"></td>\`;
  const stCell=tr.querySelector('.staerke');if(eaObj)bindStaerkeCell(stCell,eaObj);
  // Klapp-Toggle nur auf dem Namensbereich, damit Stärke-Eingabe nicht klappt
  if(!noToggle){const nameCell=tr.children[0];nameCell.style.cursor='pointer';nameCell.onclick=()=>{expandedEA[eaName]=!open;renderKraefte();};}
  return tr;
}
function renderDatalists(){
  let dl=document.getElementById('kraefteDatalists');
  if(!dl){dl=document.createElement('div');dl.id='kraefteDatalists';document.body.appendChild(dl);}
  let html=\`<datalist id="dlEA">\${planEANames().map(n=>\`<option value="\${esc(n)}">\`).join('')}</datalist>\`;
  PLAN.eas.forEach(e=>{html+=\`<datalist id="dlUA-\${esc(e.name)}">\${e.uas.map(u=>\`<option value="\${esc(u.name)}">\`).join('')}</datalist>\`;});
  // generische UA-Liste (falls EA leer)
  html+=\`<datalist id="dlUA-">\${allUANames().map(n=>\`<option value="\${esc(n)}">\`).join('')}</datalist>\`;
  dl.innerHTML=html;
}
function uaSubRow(eaName,uaName,list){
  const bet=sumBeteiligte(list);
  const uaObj=planUAObj(eaName,uaName);
  const tr=document.createElement('tr');tr.style.background='rgba(28,35,48,.5)';
  tr.innerHTML=\`
    <td></td>
    <td style="color:#8b98a8;font-weight:600">└ \${esc(uaName)}</td>
    <td colspan="3" style="color:var(--ink-dim)">\${list.length} Einheit\${list.length===1?'':'en'}</td>
    <td class="staerke">\${uaObj?staerkeCellHTML(uaObj):(()=>{const st=sumStaerke(list);return \`<span style="font-family:var(--mono);color:var(--ink-dim)">\${st.f}/\${st.u}/\${st.m}//\${st.ges}</span>\`;})()}</td>
    <td style="text-align:center;font-family:var(--mono);color:var(--ink-dim)">\${bet}</td>
    <td colspan="3"></td>\`;
  const stCell=tr.querySelector('.staerke');if(uaObj)bindStaerkeCell(stCell,uaObj);
  return tr;
}
function renderKraefte(){
  const tb=document.getElementById('kraefteBody');
  buildLevelSelect();renderDatalists();
  if(typeof tableMode!=='undefined' && tableMode)renderTableView();
  const US=visibleUnits();
  document.getElementById('kraefteCount').textContent=US.length?US.length+' Einheiten':'';
  tb.innerHTML='';

  if(level==='fg'){
    // Alle EA aus dem Einsatzplan zeigen (auch ohne Kräfte), danach EA, die nur an Kräften hängen
    const eaOrder=[...planEANames()];
    US.forEach(u=>{const k=u.ea||'(ohne EA)';if(!eaOrder.includes(k))eaOrder.push(k);});
    if(!eaOrder.length){tb.innerHTML=\`<tr><td colspan="\${COLSPAN}" class="empty">Keine Einsatzabschnitte angelegt. Im „Grafischen Einsatzplan“ anlegen.</td></tr>\`;return;}
    eaOrder.forEach(k=>{
      const list=US.filter(u=>(u.ea||'(ohne EA)')===k);
      tb.appendChild(eaSummaryRow(k,list));
      if(expandedEA[k]){
        const planEA=PLAN.eas.find(e=>e.name===k);
        const uaNames=planEA?planEA.uas.map(u=>u.name).filter(Boolean):[];
        const extra=[];list.forEach(u=>{const un=u.ua||'';if(un&&!uaNames.includes(un)&&!extra.includes(un))extra.push(un);});
        const allUA=[...uaNames,...extra];
        allUA.forEach(un=>{
          const sub=list.filter(u=>(u.ua||'')===un);
          tb.appendChild(uaSubRow(k,un,sub));
          sub.forEach(u=>tb.appendChild(unitRow(u)));
        });
        list.filter(u=>!(u.ua||'')).forEach(u=>tb.appendChild(unitRow(u)));
      }
    });
  } else {
    const list=US.filter(u=>(u.ea||'')===level);
    const planEA=PLAN.eas.find(e=>e.name===level);
    const uaNames=planEA?planEA.uas.map(u=>u.name).filter(Boolean):[];
    const extra=[];list.forEach(u=>{const un=u.ua||'';if(un&&!uaNames.includes(un)&&!extra.includes(un))extra.push(un);});
    const allUA=[...uaNames,...extra];
    tb.appendChild(eaSummaryRow(level,list,true));
    allUA.forEach(un=>{
      const sub=list.filter(u=>(u.ua||'')===un);
      tb.appendChild(uaSubRow(level,un,sub));
      sub.forEach(u=>tb.appendChild(unitRow(u)));
    });
    list.filter(u=>!(u.ua||'')).forEach(u=>tb.appendChild(unitRow(u)));
  }
}
function applyField(u,f,val,inp){
  const rn=u.call||u.label||u.name;
  if(f==='status'){const nv=+val;if(nv!==u.status){u.status=nv;addLog('meldung',\`\${rn}: Status \${nv} – \${STATUS[nv].t}\`);}
    const row=inp.closest('tr');if(row){row.classList.remove('st-green','st-yellow','st-red');const c=statusClass(nv);if(c)row.classList.add(c);}}
  else if(f==='staerke'){const m=val.match(/(\\d+)\\D+(\\d+)\\D+(\\d+)/);if(m){u.s_f=+m[1];u.s_u=+m[2];u.s_m=+m[3];}inp.value=\`\${u.s_f}/\${u.s_u}/\${u.s_m}//\${u.s_f+u.s_u+u.s_m}\`;renderKraefte();}
  else if(f==='beteiligte'){u.beteiligte=Math.max(0,parseInt(val)||0);inp.value=u.beteiligte;renderKraefte();}
  else if(f==='call'){u.call=val;if(u.marker)u.marker.setIcon(makeIcon(svgOf(findItem(u.type)),val));}
  else if(f==='auftrag'){const old=u.auftrag;u.auftrag=val;if(val&&val!==old)addLog('meldung',\`\${rn}: Auftrag – \${val}\`);}
  else if(f==='ea'){u.ea=val;renderKraefte();}
  else if(f==='ua'){u.ua=val;}
  else{u[f]=val;}
  pushState();
}
function durationSince(hm){const p=hm.split(':').map(Number);if(p.length<2||isNaN(p[0]))return '–';
  const now=new Date();let start=new Date();start.setHours(p[0],p[1],0,0);
  let diff=(now-start)/60000;if(diff<0)diff+=1440;
  const H=Math.floor(diff/60),M=Math.floor(diff%60);return String(H).padStart(2,'0')+':'+String(M).padStart(2,'0')+' h';}
setInterval(()=>{S.units.forEach(u=>{const c=document.querySelector(\`[data-dur="\${u.id}"]\`);if(c)c.textContent=durationSince(u.start);});},15000);

/* ===== ETB ===== */
function nowHM(){const d=new Date();return String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0');}
let opStarted=false; // erst nach "Einsatzbeginn" werden automatische Aktionen protokolliert
let logSeq=1;
function roleLabel(){return roleIsFG()?'FüGrp':(NET.role||'—');}
function addLog(type,text,force){if(!opStarted&&!force)return;S.log.unshift({id:'L'+Date.now()+'_'+(logSeq++),ts:nowHM(),by:roleLabel(),type,text,prio:false,done:false});renderLog();pushState();}
function renderLog(){const box=document.getElementById('log');
  const L=visibleLog();
  const fg=roleIsFG();
  if(!L.length){box.innerHTML='<div class="empty">'+(opStarted?'Einsatztagebuch ist leer.':'Vorbereitungsphase – Protokollierung startet mit „Einsatzbeginn“.')+'</div>';renderHandlungsbedarf();return;}
  const names={meldung:'Meldung',lage:'Lage',befehl:'Befehl',info:'Info'};
  box.innerHTML=L.map(e=>{
    const prioBtn=fg?\`<button class="prio-btn \${e.prio&&!e.done?'on':''}" data-prio="\${e.id}" title="Als Handlungsbedarf markieren">\${e.prio&&!e.done?'★':'☆'}</button>\`:'';
    const decision=(e.decision)?\`<div class="decision">↳ Entscheidung \${e.decision.ts}\${e.decision.text?': '+esc(e.decision.text):' (ohne Kommentar)'}</div>\`:'';
    const by=e.by?\`<span class="by" title="Meldungsersteller">\${esc(e.by)}</span>\`:'';
    return \`<div class="entry">\${prioBtn}<span class="ts">\${e.ts}</span>\${by}<span class="txt"><span class="tag tag-\${e.type}">\${names[e.type]}</span>\${esc(e.text)}\${decision}</span></div>\`;
  }).join('');
  if(fg)box.querySelectorAll('[data-prio]').forEach(b=>b.onclick=()=>togglePrio(b.dataset.prio));
  renderHandlungsbedarf();
}
function togglePrio(id){const e=S.log.find(x=>x.id===id);if(!e)return;e.prio=!e.prio;if(e.prio)e.done=false;renderLog();pushState();
  if(e.prio)openHandlungsbedarf();}
function openHandlungsbedarf(){
  // Frame sichtbar/größer machen: falls sehr klein gezogen, mindestens hälftig öffnen
  const view=document.getElementById('viewEinsatz');
  const rc=document.getElementById('rightCol').getBoundingClientRect();
  const cur=getComputedStyle(view).getPropertyValue('--etbrow').trim();
  // Wenn der untere Bereich sehr klein ist, ETB/Handlungsbedarf auf ~50/50 setzen
  const hb=document.getElementById('hbPane');
  const hbH=hb?hb.getBoundingClientRect().height:0;
  if(hbH<120){ view.style.setProperty('--etbrow','1fr'); view.style.setProperty('--hbrow','1fr'); }
  const box=document.getElementById('handlungsbedarf');
  if(box){box.scrollTop=0;}
  if(hb){hb.classList.remove('flash');void hb.offsetWidth;hb.classList.add('flash');}
}
function markDone(id){
  const e=S.log.find(x=>x.id===id);if(!e)return;
  const ta=document.getElementById('hb-cmt-'+id);
  const comment=ta?ta.value.trim():'';
  e.done=true;
  e.decision={text:comment,ts:nowHM()}; // Entscheidung mit Zeitstempel an die Meldung hängen
  const suffix=comment?\` – \${comment}\`:'';
  addLog('info',\`Entscheidung zu: \${e.text}\${suffix}\`,true);
  renderLog();pushState();
}
function renderHandlungsbedarf(){
  const box=document.getElementById('handlungsbedarf');if(!box)return;
  const fg=roleIsFG();
  // offene, priorisierte Einträge (rollen-sichtbar)
  const src=visibleLog().filter(e=>e.prio&&!e.done);
  document.getElementById('hbCount').textContent=src.length?src.length+' offen':'';
  if(!src.length){box.innerHTML='<div class="empty">Kein offener Handlungsbedarf.'+(fg?' Im ETB einen Eintrag mit „Bedarf“ markieren.':'')+'</div>';return;}
  box.innerHTML=src.map(e=>\`<div class="hb-item">
    <div class="hb-head"><span class="hb-ic">⚑</span>
      <div class="hb-body"><div class="hb-ts">\${e.ts}</div><div class="hb-txt">\${esc(e.text)}</div></div></div>
    \${fg?\`<div class="hb-decision">
      <textarea id="hb-cmt-\${e.id}" class="hb-cmt" rows="2" placeholder="Entscheidung / Kommentar eingeben…">\${esc(e.decision&&e.decision.text||'')}</textarea>
      <button class="hb-done" data-done="\${e.id}" title="Entscheidung übernehmen und erledigen">erledigt</button>
    </div>\`:'<div class="hb-decision"><div class="hb-ro">Entscheidung durch Führungsgruppe</div></div>'}
  </div>\`).join('');
  if(fg)box.querySelectorAll('[data-done]').forEach(b=>b.onclick=()=>markDone(b.dataset.done));
}
logBtn.onclick=submitLog;logInput.addEventListener('keydown',e=>{if(e.key==='Enter')submitLog();});
function submitLog(){const v=logInput.value.trim();if(!v)return;addLog(logType.value,v,true);logInput.value='';}

/* ===== Header ===== */
btnClear.onclick=()=>{if(NET.role&&NET.role!=='fg'){flashInfo('Zurücksetzen darf nur die Führungsgruppe.');return;}
  if(!confirm('Gesamte Lage zurücksetzen?'))return;
  S.markers.forEach(m=>map.removeLayer(m.marker));S.markers=[];S.units=[];S.log=[];callCounters={};numberCounters={};S.seq=1;renderKraefte();renderLog();pushState();};
/* ===== Einsatztagebuch als PDF ===== */
/* Offline-tauglich ohne Fremdbibliothek: baut ein druckfertiges HTML-Dokument
   und öffnet den Druckdialog des Browsers („Als PDF speichern"). */
function etbPdf(auto){
  const einsatz=opName.textContent.trim()||'Einsatz';
  const abschnitt=opSection.textContent.trim();
  const rolle=roleIsFG()?'Führungsgruppe':(NET.role||'Einzelplatz');
  const now=new Date();
  const stamp=now.toLocaleString('de-DE');
  const beginn=opStartTime?opStartTime.toLocaleString('de-DE'):'—';
  // Bei Einsatzende gesamtes ETB, sonst rollen-sichtbares
  const L=roleIsFG()?S.log:visibleLog();
  const names={meldung:'Meldung',lage:'Lage',befehl:'Befehl',info:'Info'};
  const rows=[...L].reverse().map(e=>{ // chronologisch (älteste zuerst)
    const dec=e.decision?\`<div class="dec">↳ Entscheidung \${e.decision.ts}\${e.decision.text?': '+escHtml(e.decision.text):''}</div>\`:'';
    const prio=e.prio?' ⚑':'';
    return \`<tr>
      <td class="z">\${e.ts}</td>
      <td class="by">\${escHtml(e.by||'')}</td>
      <td class="ty ty-\${e.type}">\${names[e.type]||e.type}</td>
      <td>\${escHtml(e.text)}\${prio}\${dec}</td>
    </tr>\`;
  }).join('');
  const doc=\`<!DOCTYPE html><html lang="de"><head><meta charset="utf-8">
<title>Einsatztagebuch – \${escHtml(einsatz)}</title>
<style>
  *{box-sizing:border-box}
  body{font-family:Arial,Helvetica,sans-serif;color:#111;margin:24px;font-size:12px}
  h1{font-size:20px;margin:0 0 4px}
  .meta{font-size:11px;color:#333;margin-bottom:14px;line-height:1.6}
  .meta b{color:#000}
  table{width:100%;border-collapse:collapse}
  th,td{border:1px solid #999;padding:5px 7px;vertical-align:top;text-align:left}
  th{background:#eee;font-size:11px;text-transform:uppercase;letter-spacing:.5px}
  td.z{white-space:nowrap;font-family:monospace;width:70px}
  td.ty{white-space:nowrap;width:70px;font-weight:bold}
  td.by{white-space:nowrap;width:64px;font-family:monospace;color:#333}
  .ty-lage{color:#8a5a00}.ty-befehl{color:#8a0000}.ty-meldung{color:#003a8a}.ty-info{color:#333}
  .dec{margin-top:3px;padding:3px 6px;border-left:3px solid #4a90d9;background:#eef4fb;color:#204a72}
  .foot{margin-top:16px;font-size:10px;color:#666;border-top:1px solid #ccc;padding-top:6px}
  @media print{body{margin:12mm}}
</style></head><body>
  <h1>Einsatztagebuch</h1>
  <div class="meta">
    <b>Einsatz:</b> \${escHtml(einsatz)}\${abschnitt?\` &nbsp;·&nbsp; <b>Abschnitt:</b> \${escHtml(abschnitt)}\`:''}<br>
    <b>Einsatzbeginn:</b> \${beginn} &nbsp;·&nbsp; <b>Stand:</b> \${stamp}\${auto?' (automatisch bei Einsatzende)':''}<br>
    <b>Erstellt durch:</b> \${escHtml(rolle)} &nbsp;·&nbsp; <b>Einträge:</b> \${L.length}
  </div>
  <table>
    <thead><tr><th>Uhrzeit</th><th>Ersteller</th><th>Art</th><th>Eintrag / Entscheidung</th></tr></thead>
    <tbody>\${rows||'<tr><td colspan="4">Keine Einträge.</td></tr>'}</tbody>
  </table>
  <div class="foot">Einsatzführung / Lagedarstellung – Einsatztagebuch, erzeugt am \${stamp}.</div>
</body></html>\`;
  const w=window.open('','_blank');
  if(!w){ if(!auto)alert('Bitte Popups für diese Seite erlauben, um das PDF zu erzeugen.'); return; }
  w.document.open();w.document.write(doc);w.document.close();
  // nach dem Rendern Druckdialog öffnen (Nutzer wählt „Als PDF speichern")
  const go=()=>{ try{w.focus();w.print();}catch(e){} };
  if(w.document.readyState==='complete')setTimeout(go,300); else w.onload=()=>setTimeout(go,300);
  addLog('info','Einsatztagebuch als PDF erzeugt'+(auto?' (automatisch bei Einsatzende)':''));
}
function escHtml(s){return String(s==null?'':s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));}
document.getElementById('etbPdf').onclick=()=>etbPdf(false);

function planPdf(){
  const src=document.querySelector('#viewPlan .plan-scroll');
  if(!src){ alert('Einsatzplan nicht gefunden.'); return; }
  // Klon erstellen und aufbereiten
  const clone=src.cloneNode(true);
  // Werkzeugleiste (Buttons) und Entfernen-Schaltflächen raus
  clone.querySelectorAll('.plan-toolbar').forEach(el=>el.remove());
  clone.querySelectorAll('.rem, .ua-add, .ea-add, button').forEach(el=>el.remove());
  // contenteditable-Felder in reinen Text umwandeln (Cursor/Attribute weg)
  clone.querySelectorAll('[contenteditable]').forEach(el=>{ el.removeAttribute('contenteditable'); });
  const einsatz=opName.textContent.trim()||'Einsatz';
  const abschnitt=opSection.textContent.trim();
  const stamp=new Date().toLocaleString('de-DE');
  const doc=\`<!DOCTYPE html><html lang="de"><head><meta charset="utf-8">
<title>Grafischer Einsatzplan – \${escHtml(einsatz)}</title>
<style>
  *{box-sizing:border-box}
  body{font-family:Arial,Helvetica,sans-serif;color:#111;margin:16px;font-size:12px;background:#fff}
  h1{font-size:19px;margin:0 0 4px}
  .meta{font-size:11px;color:#333;margin:0 0 14px;line-height:1.6}
  .meta b{color:#000}
  .plan-scroll{min-width:0;padding:0}
  .plan-top{display:grid;grid-template-columns:1.1fr .7fr 1.6fr;gap:12px;align-items:start;margin-bottom:14px}
  .plan-box{background:#fff;border:1px solid #999;border-radius:6px;padding:9px 11px}
  .pl-title{font-size:12px;font-weight:800;margin-bottom:8px}
  .pl-title.accent,.pl-k.accent,.ea-title .lab{color:#1a4e8a}
  .pl-row{display:flex;gap:6px;font-size:12px;padding:3px 0;border-bottom:1px solid #ddd}
  .pl-k{color:#555;white-space:nowrap;min-width:78px}
  .pl-v{flex:1;color:#111;min-height:14px}
  .pl-v.big{font-weight:700;font-size:14px}
  .stab-grid{display:grid;grid-template-columns:1fr 1fr;gap:0 16px}
  .plan-connector{display:none}
  .plan-eas{display:flex;gap:12px;align-items:flex-start;flex-wrap:wrap}
  .ea-card{position:relative;flex:0 0 250px;background:#fff;border:1px solid #999;border-radius:6px;padding:9px 11px;page-break-inside:avoid;break-inside:avoid}
  .ea-card .ea-title,.ua-card .ea-title{display:flex;align-items:center;gap:6px;font-weight:800;font-size:12px;margin-bottom:8px}
  .ea-card .ea-title .lab{color:#1a4e8a}
  .ea-sub{font-size:11px;color:#555;text-transform:uppercase;letter-spacing:.5px;margin:8px 0 3px}
  .ua-sub-hd{font-size:10px;text-transform:uppercase;letter-spacing:1px;color:#555;margin:12px 0 6px;border-top:1px solid #bbb;padding-top:8px}
  .ua-list{display:flex;flex-direction:column;gap:8px}
  .ua-card{background:#f4f6f9;border:1px solid #bbb;border-left:3px solid #4a90d9;border-radius:6px;padding:7px 9px}
  .ua-card .ea-title .lab.ua{color:#2f6ca8}
  .stw{display:inline-flex;align-items:center;gap:2px;font-family:monospace}
  input,textarea{border:none;background:transparent;color:#111;font:inherit;padding:0;resize:none;width:100%}
  @media print{ body{margin:10mm} .ea-card{flex-basis:230px} }
</style></head><body>
  <h1>Grafischer Einsatzplan</h1>
  <div class="meta"><b>Einsatz:</b> \${escHtml(einsatz)}\${abschnitt?\` &nbsp;·&nbsp; <b>Abschnitt:</b> \${escHtml(abschnitt)}\`:''} &nbsp;·&nbsp; <b>Stand:</b> \${stamp}</div>
  \${clone.outerHTML}
</body></html>\`;
  const w=window.open('','_blank');
  if(!w){ alert('Bitte Popups für diese Seite erlauben, um das PDF zu erzeugen.'); return; }
  w.document.open();w.document.write(doc);w.document.close();
  const go=()=>{ try{w.focus();w.print();}catch(e){} };
  if(w.document.readyState==='complete')setTimeout(go,300); else w.onload=()=>setTimeout(go,300);
  addLog('info','Grafischer Einsatzplan als PDF erzeugt');
}
document.getElementById('planPdf').onclick=()=>planPdf();

btnExport.onclick=()=>{
  const data={einsatz:opName.textContent,zeit:new Date().toISOString(),
    kraefte:S.units.map(u=>({ea:u.ea,ua:u.ua,rufname:u.call,status:u.status,auftrag:u.auftrag,staerke:\`\${u.s_f}/\${u.s_u}/\${u.s_m}//\${u.s_f+u.s_u+u.s_m}\`,beteiligte:+u.beteiligte||0,dienstbeginn:u.start,pos:u.marker.getLatLng()})),
    gliederung:PLAN.eas.map(e=>({ea:e.name,felder:e.fields,staerke:e.staerke||{h:0,g:0,m:0},unterabschnitte:e.uas.map(u=>({name:u.name,staerke:u.staerke||{h:0,g:0,m:0}}))})),
    lage:S.markers.map(m=>({typ:m.name,bez:m.call||m.label,pos:m.marker.getLatLng()})),etb:S.log};
  const blob=new Blob([JSON.stringify(data,null,2)],{type:'application/json'});
  const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download='lage_export_'+Date.now()+'.json';a.click();addLog('info','Lage exportiert');};

/* ===== Tabs ===== */
tabEinsatz.onclick=()=>switchView('einsatz');tabConfig.onclick=()=>switchView('config');tabPlan.onclick=()=>switchView('plan');
document.getElementById('tabBefehl').onclick=()=>switchView('befehl');
function switchView(v){
  document.getElementById('viewEinsatz').classList.toggle('hidden',v!=='einsatz');
  document.getElementById('viewConfig').classList.toggle('active',v==='config');
  document.getElementById('viewPlan').classList.toggle('active',v==='plan');
  document.getElementById('viewBefehl').classList.toggle('active',v==='befehl');
  tabEinsatz.classList.toggle('active',v==='einsatz');
  document.getElementById('tabBefehl').classList.toggle('active',v==='befehl');
  tabConfig.classList.toggle('active',v==='config');
  tabPlan.classList.toggle('active',v==='plan');
  if(v==='einsatz')setTimeout(()=>map.invalidateSize(),50);
  if(v==='plan')renderPlan();
}

/* ===== Befehl: externe Dateien (Netzablage) ===== */
const BEFEHL={ dir:null, activeRow:null, mammothLoading:null, xlsxLoading:null };
const fsApiOK = ('showDirectoryPicker' in window);

document.getElementById('befehlPick').onclick=async ()=>{
  if(!fsApiOK){ befehlNotice('Dieser Browser unterstützt den Ordnerzugriff nicht. Bitte Chrome oder Edge verwenden.'); return; }
  try{
    const dir=await window.showDirectoryPicker();
    BEFEHL.dir=dir;
    document.getElementById('befehlFolderName').textContent=dir.name;
    await buildBefehlTree();
  }catch(e){ /* abgebrochen */ }
};
document.getElementById('befehlReload').onclick=()=>{ if(BEFEHL.dir)buildBefehlTree(); };

function befehlNotice(txt){
  const tree=document.getElementById('befehlTree');
  tree.innerHTML=\`<div class="empty" style="padding:14px">\${esc(txt)}</div>\`;
}
async function buildBefehlTree(){
  const tree=document.getElementById('befehlTree');
  tree.innerHTML='<div class="empty" style="padding:14px">Lese Ordner…</div>';
  try{
    const root=await readDirNode(BEFEHL.dir);
    tree.innerHTML='';
    tree.appendChild(renderTreeNode(root,true));
  }catch(e){ befehlNotice('Ordner konnte nicht gelesen werden.'); }
}
// rekursiv Verzeichnis einlesen (nur relevante Dateitypen zeigen)
async function readDirNode(handle){
  const node={name:handle.name,handle,dir:true,children:[]};
  const ents=[];
  for await (const [name,h] of handle.entries()) ents.push([name,h]);
  ents.sort((a,b)=>{ const ad=a[1].kind==='directory',bd=b[1].kind==='directory'; if(ad!==bd)return ad?-1:1; return a[0].localeCompare(b[0],'de'); });
  for(const [name,h] of ents){
    if(h.kind==='directory'){ node.children.push(await readDirNode(h)); }
    else{ const ext=name.split('.').pop().toLowerCase();
      if(['pdf','docx','doc','txt','xlsx','xlsm'].includes(ext)) node.children.push({name,handle:h,dir:false,ext}); }
  }
  return node;
}
function renderTreeNode(node,open){
  const el=document.createElement('div');el.className='bt-node';
  if(node.dir){
    const row=document.createElement('div');row.className='bt-row';
    const kids=document.createElement('div');kids.className='bt-children';kids.style.display=open?'block':'none';
    row.innerHTML=\`<span class="bt-ic">\${open?'📂':'📁'}</span><span class="bt-name">\${esc(node.name)}</span>\`;
    row.onclick=()=>{const vis=kids.style.display!=='none';kids.style.display=vis?'none':'block';row.querySelector('.bt-ic').textContent=vis?'📁':'📂';};
    el.appendChild(row);
    node.children.forEach(c=>kids.appendChild(renderTreeNode(c,false)));
    el.appendChild(kids);
  }else{
    const row=document.createElement('div');row.className='bt-row';
    const icon=node.ext==='pdf'?'📕':(node.ext==='txt'?'📄':((node.ext==='xlsx'||node.ext==='xlsm')?'📊':'📘'));
    row.innerHTML=\`<span class="bt-ic">\${icon}</span><span class="bt-name">\${esc(node.name)}</span><span class="bt-badge">\${esc(node.ext.toUpperCase())}</span>\`;
    row.onclick=()=>{ if(BEFEHL.activeRow)BEFEHL.activeRow.classList.remove('active'); row.classList.add('active');BEFEHL.activeRow=row; openBefehlFile(node); };
    el.appendChild(row);
  }
  return el;
}
function befehlShow(which){ // 'empty' | 'frame' | 'docx'
  document.getElementById('befehlEmpty').style.display=which==='empty'?'flex':'none';
  document.getElementById('befehlFrame').style.display=which==='frame'?'block':'none';
  document.getElementById('befehlDocx').style.display=which==='docx'?'block':'none';
}
async function openBefehlFile(node){
  try{
    const file=await node.handle.getFile();
    if(node.ext==='pdf'){
      const url=URL.createObjectURL(file);
      document.getElementById('befehlFrame').src=url;
      befehlShow('frame');
    } else if(node.ext==='txt'){
      const txt=await file.text();
      const box=document.getElementById('befehlDocx');
      box.innerHTML='<pre style="white-space:pre-wrap;font-family:monospace;font-size:13px">'+esc(txt)+'</pre>';
      befehlShow('docx');
    } else if(node.ext==='docx'){
      await showDocx(file);
    } else if(node.ext==='xlsx' || node.ext==='xlsm'){
      await showXlsx(file);
    } else if(node.ext==='doc'){
      const box=document.getElementById('befehlDocx');
      box.innerHTML='<p><b>Älteres Word-Format (.doc)</b> kann im Browser nicht dargestellt werden.<br>Bitte die Datei in .docx umwandeln oder extern öffnen.</p>';
      befehlShow('docx');
    }
    addLog('info',\`Dokument geöffnet: \${node.name}\`);
  }catch(e){
    const box=document.getElementById('befehlDocx');box.innerHTML='<p>Datei konnte nicht geöffnet werden.</p>';befehlShow('docx');
  }
}
// Excel (.xlsx/.xlsm) als Tabellenvorschau – SheetJS bei Bedarf laden (Makros werden NICHT ausgeführt)
function loadSheetJS(){
  if(window.XLSX)return Promise.resolve(window.XLSX);
  if(BEFEHL.xlsxLoading)return BEFEHL.xlsxLoading;
  BEFEHL.xlsxLoading=new Promise((resolve,reject)=>{
    const s=document.createElement('script');
    s.src='https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js';
    s.onload=()=>resolve(window.XLSX);
    s.onerror=()=>reject(new Error('xlsx load failed'));
    document.head.appendChild(s);
  });
  return BEFEHL.xlsxLoading;
}
async function showXlsx(file){
  const box=document.getElementById('befehlDocx');
  box.innerHTML='<p style="color:#666">Tabelle wird geladen…</p>';befehlShow('docx');
  try{
    const XLSX=await loadSheetJS();
    const buf=await file.arrayBuffer();
    const wb=XLSX.read(buf,{type:'array'});
    let html='';
    wb.SheetNames.forEach((name,i)=>{
      const ws=wb.Sheets[name];
      const tbl=XLSX.utils.sheet_to_html(ws,{header:'',footer:''});
      html+=\`<h3 style="font-family:Arial,sans-serif;margin:\${i?'22px':'2px'} 0 8px">\${esc(name)}</h3>\`+tbl;
    });
    box.innerHTML='<div class="xlsx-view">'+(html||'<p>(leere Arbeitsmappe)</p>')+'</div>';
  }catch(e){
    box.innerHTML='<p><b>Tabellen-Anzeige nicht verfügbar.</b><br>Für die Darstellung von .xlsx/.xlsm wird einmalig eine Internetverbindung benötigt (SheetJS). Ohne Internet kann die Tabelle hier nicht angezeigt werden.</p>';
  }
}
// Word (.docx) vereinfacht anzeigen – mammoth.js bei Bedarf laden
function loadMammoth(){
  if(window.mammoth)return Promise.resolve(window.mammoth);
  if(BEFEHL.mammothLoading)return BEFEHL.mammothLoading;
  BEFEHL.mammothLoading=new Promise((resolve,reject)=>{
    const s=document.createElement('script');
    s.src='https://cdnjs.cloudflare.com/ajax/libs/mammoth/1.6.0/mammoth.browser.min.js';
    s.onload=()=>resolve(window.mammoth);
    s.onerror=()=>reject(new Error('mammoth load failed'));
    document.head.appendChild(s);
  });
  return BEFEHL.mammothLoading;
}
async function showDocx(file){
  const box=document.getElementById('befehlDocx');
  box.innerHTML='<p style="color:#666">Word-Dokument wird geladen…</p>';befehlShow('docx');
  try{
    const mammoth=await loadMammoth();
    const buf=await file.arrayBuffer();
    const res=await mammoth.convertToHtml({arrayBuffer:buf});
    box.innerHTML=res.value||'<p>(leeres Dokument)</p>';
  }catch(e){
    box.innerHTML='<p><b>Word-Anzeige nicht verfügbar.</b><br>Für die vereinfachte Darstellung von .docx wird einmalig eine Internetverbindung benötigt (mammoth.js). Ohne Internet kann das Dokument hier nicht angezeigt werden.</p>';
  }
}
// Word (.docx) vereinfacht anzeigen – mammoth.js bei Bedarf laden
function loadMammoth(){
  if(window.mammoth)return Promise.resolve(window.mammoth);
  if(BEFEHL.mammothLoading)return BEFEHL.mammothLoading;
  BEFEHL.mammothLoading=new Promise((resolve,reject)=>{
    const s=document.createElement('script');
    s.src='https://cdnjs.cloudflare.com/ajax/libs/mammoth/1.6.0/mammoth.browser.min.js';
    s.onload=()=>resolve(window.mammoth);
    s.onerror=()=>reject(new Error('mammoth load failed'));
    document.head.appendChild(s);
  });
  return BEFEHL.mammothLoading;
}
async function showDocx(file){
  const box=document.getElementById('befehlDocx');
  box.innerHTML='<p style="color:#666">Word-Dokument wird geladen…</p>';befehlShow('docx');
  try{
    const mammoth=await loadMammoth();
    const buf=await file.arrayBuffer();
    const res=await mammoth.convertToHtml({arrayBuffer:buf});
    box.innerHTML=res.value||'<p>(leeres Dokument)</p>';
  }catch(e){
    box.innerHTML='<p><b>Word-Anzeige nicht verfügbar.</b><br>Für die vereinfachte Darstellung von .docx wird einmalig eine Internetverbindung benötigt (mammoth.js). Ohne Internet kann das Dokument hier nicht angezeigt werden.</p>';
  }
}

/* ===== Modal: Zeichen hinzufügen ===== */
let mColor=PALETTE_COLORS[0];
function fillModalCats(preselect){const s=document.getElementById('mCat');
  s.innerHTML=CATALOG.map(g=>\`<option \${g.cat===preselect?'selected':''}>\${esc(g.cat)}</option>\`).join('');}
function initModal(){
  const sw=document.getElementById('mSwatches');sw.innerHTML='';
  PALETTE_COLORS.forEach(c=>{const d=document.createElement('div');d.className='swatch'+(c===mColor?' sel':'');d.style.background=c;
    d.onclick=()=>{mColor=c;[...sw.children].forEach(x=>x.classList.remove('sel'));d.classList.add('sel');updateModalPreview();};sw.appendChild(d);});
  ['mName','mKuerzel','mShape'].forEach(id=>document.getElementById(id).addEventListener('input',updateModalPreview));
  document.getElementById('mAdd').onclick=addElement;
  document.getElementById('mCancel').onclick=closeModal;
  document.getElementById('modalBg').onclick=e=>{if(e.target.id==='modalBg')closeModal();};
  document.getElementById('cfgAddOpen').onclick=()=>openModal();
}
function inkFor(color){return (color==='#f4c20d'||color==='#fff')?'#0d1117':'#fff';}
function updateModalPreview(){const shape=document.getElementById('mShape').value;const k=document.getElementById('mKuerzel').value.toUpperCase();
  document.getElementById('mPreview').innerHTML=shapeSvg(shape,mColor,k,inkFor(mColor));}
function openModal(preselectCat){fillModalCats(preselectCat);updateModalPreview();document.getElementById('modalBg').classList.add('show');document.getElementById('mName').focus();}
function closeModal(){document.getElementById('modalBg').classList.remove('show');
  document.getElementById('mName').value='';document.getElementById('mKuerzel').value='';
  document.getElementById('mMobile').checked=false;document.getElementById('mNumber').checked=false;}
function addElement(){
  const name=document.getElementById('mName').value.trim();const cat=document.getElementById('mCat').value;
  const kuerzel=document.getElementById('mKuerzel').value.toUpperCase().trim();const shape=document.getElementById('mShape').value;
  const mobile=document.getElementById('mMobile').checked;const number=document.getElementById('mNumber').checked;
  if(!name){alert('Bitte eine Bezeichnung angeben.');return;}
  const item={id:'u'+Date.now(),name,shape,fill:mColor,kuerzel,ink:inkFor(mColor),mobile,number,builtin:false};
  let grp=CATALOG.find(g=>g.cat===cat);if(!grp){grp={cat,items:[]};CATALOG.push(grp);}
  grp.items.push(item);
  closeModal();renderPalette();renderCfgList();
}

/* ===== Konfig-Liste ===== */
function renderCfgList(){const box=document.getElementById('cfgList');box.innerHTML='';
  CATALOG.forEach(g=>{const grp=document.createElement('div');grp.className='grp';
    grp.innerHTML=\`<div class="grp-h"><b>\${esc(g.cat)}</b><span>\${g.items.length} Zeichen</span></div>\`;
    const grid=document.createElement('div');grid.className='el-grid';
    g.items.forEach(it=>{const card=document.createElement('div');card.className='el-card'+(it.builtin?' builtin':'');
      const flags=[it.mobile?'mobil':'',it.number?'nummeriert':''].filter(Boolean).join(' · ');
      card.innerHTML=\`\${svgOf(it)}<div><div class="n">\${esc(it.name)}</div><div class="k">\${esc(it.kuerzel||it.shape)}\${flags?' · '+flags:''}</div></div><button class="x" title="Löschen">✕</button>\`;
      card.querySelector('.x').onclick=()=>{g.items.splice(g.items.indexOf(it),1);renderCfgList();renderPalette();};
      grid.appendChild(card);});
    grp.appendChild(grid);box.appendChild(grp);});}

/* ===== Uhr & util ===== */
setInterval(()=>{clock.textContent=new Date().toLocaleTimeString('de-DE');},1000);
function esc(s){return String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}

/* ===== Einsatzbeginn ===== */
let opStartTime=null;
function syncStartButton(){
  if(opStarted){
    btnStart.textContent='● Einsatz läuft'+(opStartTime?' seit '+String(opStartTime.getHours()).padStart(2,'0')+':'+String(opStartTime.getMinutes()).padStart(2,'0'):'');
    btnStart.style.background='var(--crit)';btnStart.style.borderColor='var(--crit)';btnStart.style.color='#fff';
  } else {
    btnStart.textContent='▶ Einsatzbeginn';
    btnStart.style.background='var(--ok)';btnStart.style.borderColor='var(--ok)';btnStart.style.color='#06210d';
  }
}
btnStart.onclick=()=>{
  if(NET.role&&NET.role!=='fg'){flashInfo('Einsatzbeginn/-ende steuert die Führungsgruppe.');return;}
  if(!opStarted){
    opStarted=true;opStartTime=new Date();syncStartButton();
    addLog('lage','— EINSATZBEGINN — Protokollierung aktiviert',true);pushState();
  } else {
    if(!confirm('Einsatz beenden? Weitere Aktionen werden dann nicht mehr protokolliert.\\nDas Einsatztagebuch wird automatisch als PDF geöffnet (zum Speichern).'))return;
    addLog('lage','— EINSATZENDE — Protokollierung beendet',true);
    opStarted=false;syncStartButton();pushState();
    // Einsatztagebuch automatisch als PDF (gesamtes ETB)
    setTimeout(()=>etbPdf(true),200);
  }
};
function flashInfo(txt){const b=document.getElementById('netDisc');b.textContent=txt;b.style.background='var(--info)';b.style.color='#fff';b.classList.add('show');
  setTimeout(()=>{b.classList.remove('show');b.textContent='Verbindung zum Server verloren – Wiederverbindung…';b.style.background='';b.style.color='';},2500);}

/* ===== Editierbare Kopf-Felder (Einsatz / Abschnitt) ===== */
opName.addEventListener('blur',()=>{const v=opName.textContent.trim()||'Einsatz';opName.textContent=v;addLog('info',\`Einsatzbezeichnung: \${v}\`);pushState();});
opSection.addEventListener('blur',()=>{const v=opSection.textContent.trim();opSection.textContent=v;if(v)addLog('info',\`Abschnitt: \${v}\`);pushState();});
[opName,opSection].forEach(el=>el.addEventListener('keydown',e=>{if(e.key==='Enter'){e.preventDefault();el.blur();}}));

/* ===== Grafischer Einsatzplan ===== */
/* Stärke-Widget im Format x/x/x//xx (höherer/gehobener/mittlerer Dienst // Summe).
   Die drei x sind manuell, die Summe nach // wird automatisch berechnet. */
function staerkeWidget(obj){
  obj.staerke=obj.staerke||{h:0,g:0,m:0};
  const s=obj.staerke;const ges=(+s.h||0)+(+s.g||0)+(+s.m||0);
  const wrap=document.createElement('span');wrap.className='stw';
  wrap.innerHTML=\`<input class="sx" data-p="h" value="\${+s.h||0}" title="höherer Dienst"><span class="sep">/</span>\`+
    \`<input class="sx" data-p="g" value="\${+s.g||0}" title="gehobener Dienst"><span class="sep">/</span>\`+
    \`<input class="sx" data-p="m" value="\${+s.m||0}" title="mittlerer Dienst"><span class="sep">//</span>\`+
    \`<span class="sges">\${ges}</span>\`;
  wrap.querySelectorAll('.sx').forEach(inp=>{
    inp.addEventListener('change',()=>{s[inp.dataset.p]=Math.max(0,parseInt(inp.value)||0);inp.value=s[inp.dataset.p];
      wrap.querySelector('.sges').textContent=(+s.h||0)+(+s.g||0)+(+s.m||0);renderKraefte();pushState();});
    inp.addEventListener('keydown',e=>{if(e.key==='Enter'){e.preventDefault();inp.blur();}});
  });
  return wrap;
}
function planCardFields(node,idAttr){
  // node = EA- oder UA-Objekt mit .fields; erzeugt die identische Feldstruktur (ohne Fax)
  node.fields=node.fields||{};
  const f=node.fields;
  const row=(k,lab)=>\`<div class="pl-row"><span class="pl-k">\${lab}:</span><span class="pl-v" contenteditable="true" data-node="\${idAttr}" data-fk="\${k}">\${esc(f[k]||'')}</span></div>\`;
  return \`
    \${row('fuehrer','Führer')}
    \${row('rufname','Rufname')}
    \${row('telefon','Telefon')}
    \${row('mobil','Mobiltel.')}
    <div class="ea-sub">Führungsgruppe</div>
    \${row('fg_rufname','Rufname')}
    \${row('fg_telefon','Telefon')}
    \${row('fg_mobil','Mobiltel.')}
    <div class="ea-sub">Kräfte / Dienststelle</div>
    <div class="pl-row"><span class="pl-k">Stärke:</span><span class="pl-v staerke-slot" data-slot="\${idAttr}"></span></div>
    <div class="pl-row"><span class="pl-k">Fahrzeuge (KFZ):</span><span class="pl-v" contenteditable="true" data-node="\${idAttr}" data-fk="kfz">\${esc(f.kfz||'')}</span></div>
    \${row('kraefte','Dienststelle')}
    \${row('moz','MOZ')}
    \${row('eoz','EOZ')}
    \${row('fem','FEM')}
    \${row('auftraege','Einzelaufträge')}
    <div class="ea-kanal">◄ Kanal: <span class="pl-v" contenteditable="true" data-node="\${idAttr}" data-fk="kanal">\${esc(f.kanal||'')}</span></div>\`;
}
function bindPlanFields(card,node,onName,extraOnField){
  card.querySelectorAll('[data-fk]').forEach(el=>{
    el.addEventListener('blur',()=>{
      const key=el.dataset.fk;const val=el.textContent.trim();
      if(key==='name'){onName(el,val);}
      else{node.fields=node.fields||{};node.fields[key]=val;if(extraOnField)extraOnField();}
      pushState();
    });
    el.addEventListener('keydown',e=>{if(e.key==='Enter'&&el.dataset.fk!=='auftraege'){e.preventDefault();el.blur();}});
  });
}
function renderPlan(){
  const box=document.getElementById('planEAs');box.innerHTML='';
  PLAN.eas.forEach(ea=>{
    const card=document.createElement('div');card.className='ea-card';
    card.innerHTML=\`
      <div class="ea-title"><span class="lab">EA:</span>
        <span class="pl-v" contenteditable="true" data-node="\${ea.id}" data-fk="name" style="flex:1;font-weight:800">\${esc(ea.name)}</span>
        <button class="rem" title="Einsatzabschnitt entfernen">✕</button></div>
      \${planCardFields(ea,ea.id)}
      <div class="ua-sub-hd">Unterabschnitte</div>
      <div class="ua-list" data-ualist="\${ea.id}"></div>
      <button class="ea-addua">+ Unterabschnitt</button>\`;
    bindPlanFields(card,ea,(el,val)=>{const old=ea.name;ea.name=val||old;el.textContent=ea.name;
      if(old!==ea.name)S.units.forEach(u=>{if(u.ea===old)u.ea=ea.name;});renderKraefte();});
    card.querySelector('.rem').onclick=()=>{if(!confirm(\`\${ea.name} entfernen?\`))return;PLAN.eas.splice(PLAN.eas.indexOf(ea),1);renderPlan();renderKraefte();pushState();};
    card.querySelector('.ea-addua').onclick=()=>{ea.uas.push({id:'ua'+(planSeq++),name:'UA '+(ea.uas.length+1),fields:{},staerke:{h:0,g:0,m:0}});renderPlan();renderKraefte();pushState();};
    box.appendChild(card);
    // EA-Stärkewidget
    const eaSlot=card.querySelector(\`.staerke-slot[data-slot="\${ea.id}"]\`);
    if(eaSlot)eaSlot.appendChild(staerkeWidget(ea));
    // Unterabschnitte als eigene Karten mit gleicher Feldstruktur
    const ul=card.querySelector(\`[data-ualist="\${ea.id}"]\`);
    ea.uas.forEach(ua=>{
      ua.fields=ua.fields||{};
      const uc=document.createElement('div');uc.className='ua-card';
      uc.innerHTML=\`
        <div class="ea-title"><span class="lab ua">UA:</span>
          <span class="pl-v uaname" contenteditable="true" data-node="\${ua.id}" data-fk="name" style="flex:1;font-weight:700">\${esc(ua.name)}</span>
          <button class="rem" title="Unterabschnitt entfernen">✕</button></div>
        \${planCardFields(ua,ua.id)}\`;
      bindPlanFields(uc,ua,(el,val)=>{const old=ua.name;ua.name=val||old;el.textContent=ua.name;
        if(old!==ua.name)S.units.forEach(u=>{if(u.ua===old&&u.ea===ea.name)u.ua=ua.name;});renderKraefte();});
      uc.querySelector('.rem').onclick=()=>{ea.uas.splice(ea.uas.indexOf(ua),1);renderPlan();renderKraefte();pushState();};
      ul.appendChild(uc);
      const uaSlot=uc.querySelector(\`.staerke-slot[data-slot="\${ua.id}"]\`);
      if(uaSlot)uaSlot.appendChild(staerkeWidget(ua));
    });
  });
  applyRolePermissions();
}
document.getElementById('planAddEA').onclick=()=>{const n=PLAN.eas.length+1;PLAN.eas.push({id:'ea'+(planSeq++),name:'EA '+n,fields:{},staerke:{h:0,g:0,m:0},uas:[]});renderPlan();renderKraefte();pushState();};

/* ===== Rollen, Sichtbarkeit, Rechte ===== */
function roleIsFG(){return !NET.role||NET.role==='fg';}
function visibleUnits(){ // was zeigt die Kräfte-/ETB-Sicht je Rolle
  if(roleIsFG())return S.units;
  return S.units.filter(u=>u.ea===NET.role);
}
function visibleLog(){
  if(roleIsFG())return S.log;
  // EA sieht eigene Einträge + zentrale Lage-/Info-Meldungen
  return S.log.filter(e=> e.type==='lage' || (e.text&&e.text.indexOf(NET.role)>=0) );
}
function applyRolePermissions(){
  const fg=roleIsFG();
  document.body.classList.toggle('readonly-plan',!fg);
  // eigene EA-Karte im Plan hervorheben & bearbeitbar lassen
  if(!fg){document.querySelectorAll('.ea-card').forEach(card=>{
    const nameEl=card.querySelector('[data-fk="name"]');
    if(nameEl&&nameEl.textContent.trim()===NET.role)card.classList.add('mine');
  });}
  // „+ Einsatzabschnitt“ nur FG
  const addEA=document.getElementById('planAddEA');if(addEA)addEA.classList.toggle('hidden-role',!fg);
  // Einsatz/Abschnitt-Kopf nur FG editierbar
  opName.contentEditable=fg?'true':'false';opSection.contentEditable=fg?'true':'false';
  // Einsatzbeginn/-ende nur Führungsgruppe
  const bs=document.getElementById('btnStart');
  if(bs){
    bs.disabled=!fg;
    bs.classList.toggle('role-locked',!fg);
    bs.title=fg?'Einsatzbeginn/-ende (Führungsgruppe)':'Nur die Führungsgruppe kann den Einsatzbeginn/-ende steuern';
  }
}
function initRoleOverlay(){
  const sel=document.getElementById('roleSelect');
  function fillRoles(){
    const eas=planEANames();
    sel.innerHTML='<option value="fg">Führungsgruppe (Gesamtübersicht)</option>'+
      eas.map(n=>\`<option value="\${esc(n)}">Einsatzabschnitt: \${esc(n)}</option>\`).join('');
  }
  fillRoles();
  const st=document.getElementById('roleConnState');
  const go=document.getElementById('roleGo');
  // provisorische Verbindung, um aktuelle EA-Liste vom Server zu bekommen
  connectNET('fg',(s)=>{
    if(s==='ok'){st.textContent='Mit Server verbunden.';st.className='role-conn ok';}
    else{st.textContent='Kein Server gefunden – Einzelplatzbetrieb.';st.className='role-conn err';}
  });
  // Rollenliste nach erstem State-Empfang aktualisieren
  window.__afterFirstState=()=>{ if(!document.getElementById('roleOverlay').classList.contains('hidden')) fillRoles(); };
  go.onclick=()=>{
    NET.role=sel.value;
    document.getElementById('roleBadge').textContent=(sel.value==='fg')?'Führungsgruppe':sel.value;
    document.getElementById('roleBadge').classList.toggle('ea',sel.value!=='fg');
    // Rolle final anmelden
    if(NET.connected){try{NET.ws.send(JSON.stringify({t:'hello',role:NET.role}));}catch(e){}}
    document.getElementById('roleOverlay').classList.add('hidden');
    setTimeout(()=>map.invalidateSize(),80);
    applyRolePermissions();renderKraefte();renderLog();
    level = roleIsFG()? 'fg' : NET.role; // EA startet in seiner Ebene
    renderKraefte();
  };
}

/* ===== Init ===== */
renderPalette();renderKraefte();initModal();renderCfgList();renderLog();syncStartButton();
initRoleOverlay();
</script>
</body>
</html>
`;
