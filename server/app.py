"""Flask API + 单页看板。

  python3 -m server.app --db server/coldchain.db --http-port 5090 --mqtt-port 1883
"""
import argparse
import os
import time

from flask import Flask, jsonify, request

from . import core, db as dbmod, mqtt_ingest

DB_PATH = os.environ.get("COLDCHAIN_DB", os.path.join(os.path.dirname(__file__), "coldchain.db"))

app = Flask(__name__)


def _err(e):
    return jsonify({"error": str(e)}), 409


# ---------------------------------------------------------------- 任务

@app.post("/api/shipments")
def create_shipment():
    b = request.get_json(force=True)
    chain = b.get("escalation_chain") or []
    if isinstance(chain, str):  # 也接受逗号分隔的字符串
        chain = [x.strip() for x in chain.replace("，", ",").split(",") if x.strip()]
    try:
        s = core.create_shipment(
            DB_PATH, b["name"], b["device_id"],
            float(b["temp_min"]), float(b["temp_max"]),
            float(b.get("offline_grace_sec", 5.0)), time.time(),
            escalation_chain=chain,
            escalate_after_sec=float(b.get("escalate_after_sec", 60.0)),
        )
        return jsonify(s), 201
    except (KeyError, ValueError) as e:
        return jsonify({"error": f"参数错误: {e}"}), 400
    except core.DomainError as e:
        return _err(e)


@app.get("/api/shipments")
def shipments():
    return jsonify(core.list_shipments(DB_PATH))


@app.get("/api/shipments/<int:sid>")
def shipment(sid):
    s = core.get_shipment(DB_PATH, sid)
    return (jsonify(s), 200) if s else (jsonify({"error": "not found"}), 404)


@app.post("/api/shipments/<int:sid>/complete")
def complete(sid):
    try:
        return jsonify(core.complete_shipment(DB_PATH, sid, time.time()))
    except core.DomainError as e:
        return _err(e)


@app.get("/api/shipments/<int:sid>/timeline")
def timeline(sid):
    t = core.get_timeline(DB_PATH, sid)
    return (jsonify(t), 200) if t else (jsonify({"error": "not found"}), 404)


# ---------------------------------------------------------------- 告警

@app.get("/api/alerts")
def alerts():
    return jsonify(core.list_alerts(
        DB_PATH, status=request.args.get("status"),
        shipment_id=request.args.get("shipment_id", type=int),
    ))


@app.get("/api/alerts/<int:aid>")
def alert(aid):
    a = core.get_alert(DB_PATH, aid)
    return (jsonify(a), 200) if a else (jsonify({"error": "not found"}), 404)


@app.post("/api/alerts/<int:aid>/<op>")
def alert_op(aid, op):
    b = request.get_json(force=True, silent=True) or {}
    try:
        return jsonify(core.transition_alert(
            DB_PATH, aid, op, b.get("actor", "anonymous"), b.get("note", ""), time.time()))
    except core.DomainError as e:
        return _err(e)


@app.post("/api/alerts/<int:aid>/notes")
def alert_note(aid):
    b = request.get_json(force=True)
    try:
        return jsonify(core.add_note(
            DB_PATH, aid, b.get("actor", "anonymous"), b.get("note", ""), time.time()))
    except core.DomainError as e:
        return _err(e)


@app.get("/api/devices")
def devices():
    return jsonify(core.list_devices(DB_PATH))


# ---------------------------------------------------------------- 看板页面

@app.get("/")
def index():
    return _DASHBOARD_HTML


_DASHBOARD_HTML = """<!doctype html>
<html lang="zh"><head><meta charset="utf-8"><title>冷链温度监控</title>
<style>
 body{font-family:system-ui,'PingFang SC','Microsoft YaHei',sans-serif;margin:24px;background:#f6f7f9;color:#222}
 h1{font-size:20px} h2{font-size:15px;margin:18px 0 8px}
 .card{background:#fff;border:1px solid #e3e5e8;border-radius:8px;padding:12px 16px;margin-bottom:10px}
 .row{display:flex;gap:18px;align-items:baseline;flex-wrap:wrap}
 .tag{display:inline-block;padding:1px 8px;border-radius:10px;font-size:12px;color:#fff}
 .OPEN{background:#d4380d}.ACKED{background:#d48806}.ESCALATED{background:#722ed1}.RESOLVED{background:#389e0d}
 .TEMP_HIGH{background:#cf1322}.TEMP_LOW{background:#096dd9}.OFFLINE{background:#595959}
 .L1{background:#faad14}.L2{background:#fa541c}.L3{background:#cf1322}
 button{margin-right:6px;padding:3px 10px;border:1px solid #bbb;border-radius:5px;background:#fff;cursor:pointer}
 button:hover{background:#eef}
 table{border-collapse:collapse;width:100%} td,th{border-bottom:1px solid #eee;padding:4px 8px;font-size:13px;text-align:left}
 .tl{border-left:3px solid #ddd;margin:6px 0 6px 4px;padding-left:12px}
 .tl div{margin:5px 0}.tl .t{color:#888;font-size:12px;margin-right:8px}
 .k-shipment{border-color:#1677ff}.k-alert_event{border-color:#fa8c16}.k-violation{border-color:#cf1322}
 select,input{padding:4px 6px}
</style></head><body>
<h1>🚚 冷链运输温度监控</h1>
<div class="card"><div class="row">
 <b>新建运输任务</b>
 <input id="sname" placeholder="任务名" value="疫苗运输·沪A12345" size="16">
 <input id="sdev" placeholder="设备号" value="truck-01" size="10">
 <input id="smin" type="number" step="0.1" value="2" style="width:60px"> ~
 <input id="smax" type="number" step="0.1" value="8" style="width:60px"> ℃
 <input id="schain" placeholder="负责人链(逗号分隔)" value="调度员-王,值班经理-李,运营总监-赵" size="22">
 <input id="swait" type="number" step="1" value="60" style="width:52px" title="超过该秒数未处理完自动升级下一位"> s升级
 <button onclick="createShipment()">创建并发车</button>
 <span id="msg"></span>
</div></div>
<h2>运输任务</h2><div id="ships"></div>
<h2>告警（<a href="javascript:load()">刷新</a>）</h2><div id="alerts"></div>
<h2>异常时间线 <select id="tlship" onchange="loadTimeline()"></select></h2>
<div id="timeline" class="card"></div>
<script>
const fmt = ts => new Date(ts*1000).toLocaleTimeString('zh-CN',{hour12:false}) +
  '.' + String(Math.round(ts*1000)%1000).padStart(3,'0');
async function api(p, m, body){
  const r = await fetch(p, {method:m||'GET', headers:{'Content-Type':'application/json'},
    body: body?JSON.stringify(body):undefined});
  const j = await r.json();
  if(!r.ok){ document.getElementById('msg').textContent = '⚠ ' + (j.error||r.status); throw new Error(j.error); }
  return j;
}
async function createShipment(){
  const s = await api('/api/shipments','POST',{name:sname.value, device_id:sdev.value,
    temp_min:+smin.value, temp_max:+smax.value, offline_grace_sec:3,
    escalation_chain:schain.value.split(/[,，]/).map(x=>x.trim()).filter(Boolean),
    escalate_after_sec:+swait.value});
  document.getElementById('msg').textContent = '已创建 #' + s.id; load();
}
async function op(id, op){
  const note = op==='resolve' ? prompt('处理结果说明','现场已处理，温度恢复正常') : prompt('备注','') || '';
  const actor = prompt('处理人','调度员-王') || '调度员';
  if(note===null) return;
  await api(`/api/alerts/${id}/${op}`,'POST',{actor, note}); load();
}
async function load(){
  const ships = await api('/api/shipments');
  document.getElementById('ships').innerHTML = ships.map(s=>`<div class="card"><div class="row">
    <b>#${s.id} ${s.name}</b><span>${s.device_id}</span><span>阈值 ${s.temp_min}~${s.temp_max}℃</span>
    <span>升级链 ${JSON.parse(s.escalation_chain||'[]').join(' → ')||'—'}（${s.escalate_after_sec}s）</span>
    <span class="tag ${s.status==='IN_TRANSIT'?'OPEN':'RESOLVED'}">${s.status}</span>
    ${s.status==='IN_TRANSIT'?`<button onclick="api('/api/shipments/${s.id}/complete','POST').then(load)">完成任务</button>`:''}
  </div></div>`).join('') || '<i>暂无任务</i>';
  document.getElementById('tlship').innerHTML = ships.map(s=>`<option value="${s.id}">#${s.id} ${s.name}</option>`).join('');
  const alerts = await api('/api/alerts');
  document.getElementById('alerts').innerHTML = alerts.length ? '<table><tr><th>ID</th><th>任务</th><th>类型</th><th>级别</th><th>状态</th><th>负责人</th><th>峰值</th><th>开窗时间</th><th>操作</th></tr>' +
    alerts.map(a=>`<tr><td>${a.id}</td><td>#${a.shipment_id}</td>
      <td><span class="tag ${a.type}">${a.type}</span></td>
      <td><span class="tag ${a.severity}">${a.severity}</span></td>
      <td><span class="tag ${a.status}">${a.status}</span></td>
      <td>${a.assignee||'—'}</td>
      <td>${a.peak_temp??''}</td><td>${fmt(a.opened_at)}</td><td>
      ${a.status!=='RESOLVED'?`
        <button onclick="op(${a.id},'ack')">确认</button>
        <button onclick="op(${a.id},'escalate')">升级</button>
        <button onclick="op(${a.id},'resolve')">关闭</button>`:'✅ 已处理'}
      </td></tr>`).join('') + '</table>' : '<i>暂无告警</i>';
  loadTimeline();
}
async function loadTimeline(){
  const id = document.getElementById('tlship').value;
  if(!id){ document.getElementById('timeline').innerHTML=''; return; }
  const t = await api(`/api/shipments/${id}/timeline`);
  document.getElementById('timeline').innerHTML = t.items.map(i=>{
    const head = i.kind==='alert_event' ? `[告警#${i.alert_id} ${i.alert_type}·${i.action}${i.actor?'·'+i.actor:''}] ` : '';
    return `<div class="tl k-${i.kind}"><div><span class="t">${fmt(i.ts)}</span>${head}${i.text||''}</div></div>`;
  }).join('') || '<i>暂无事件</i>';
}
load(); setInterval(load, 5000);
</script></body></html>"""


def main():
    global DB_PATH
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB_PATH)
    ap.add_argument("--http-port", type=int, default=5090)
    ap.add_argument("--mqtt-port", type=int, default=1883)
    ap.add_argument("--with-broker", action="store_true", help="同时内嵌启动 MQTT broker")
    args = ap.parse_args()
    DB_PATH = args.db

    dbmod.init_db(DB_PATH)
    if args.with_broker:
        from . import broker
        broker.start_broker(args.mqtt_port)
        print(f"[broker] mqtt://127.0.0.1:{args.mqtt_port}")
    mqtt_ingest.start_ingest(DB_PATH, port=args.mqtt_port)
    mqtt_ingest.start_watchdog(DB_PATH)
    print(f"[server] http://127.0.0.1:{args.http_port}  (db={DB_PATH})")
    app.run(host="127.0.0.1", port=args.http_port, threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
