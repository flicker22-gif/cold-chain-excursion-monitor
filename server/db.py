"""SQLite 存储层：建表 + 连接助手。只依赖标准库。"""
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS shipments (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  name          TEXT NOT NULL,
  device_id     TEXT NOT NULL,
  temp_min      REAL NOT NULL,
  temp_max      REAL NOT NULL,
  offline_grace_sec REAL NOT NULL DEFAULT 5.0,   -- 超过该时长未收到任何消息判离线
  escalation_chain TEXT NOT NULL DEFAULT '[]',   -- JSON 数组：负责人升级链，超时未处理完逐级上推
  escalate_after_sec REAL NOT NULL DEFAULT 60.0, -- 现任负责人超过该时长未处理完 → 自动升级下一位
  status        TEXT NOT NULL DEFAULT 'IN_TRANSIT',  -- IN_TRANSIT / COMPLETED
  created_at    REAL NOT NULL,
  finished_at   REAL
);

CREATE TABLE IF NOT EXISTS telemetry (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  device_id   TEXT NOT NULL,
  shipment_id INTEGER NOT NULL,
  msg_id      TEXT NOT NULL,          -- 设备侧唯一消息号，去重靠它
  seq         INTEGER,
  temp        REAL NOT NULL,
  device_ts   REAL NOT NULL,          -- 设备采样时刻（断网补传时早于到达时刻）
  arrived_at  REAL NOT NULL,          -- 服务端收到时刻
  backfilled  INTEGER NOT NULL DEFAULT 0,
  UNIQUE (device_id, msg_id)          -- 重复消息 / 补报重发直接忽略
);

CREATE TABLE IF NOT EXISTS alerts (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  shipment_id INTEGER NOT NULL,
  device_id   TEXT NOT NULL,
  type        TEXT NOT NULL,          -- TEMP_HIGH / TEMP_LOW / OFFLINE
  status      TEXT NOT NULL DEFAULT 'OPEN',  -- OPEN / ACKED / ESCALATED / RESOLVED
  severity    TEXT NOT NULL DEFAULT 'L1',  -- L1 提示 / L2 严重 / L3 紧急；只升不降，关闭后冻结
  assignee    TEXT,                   -- 当前负责人（升级链上的一员，无链则为空）
  assignee_idx INTEGER NOT NULL DEFAULT 0, -- 在升级链上的位置
  assignee_since REAL,                -- 现任负责人接手时刻：自动升级计时的起点
  notified_severity TEXT,             -- 最近一次通知的 (级别, 负责人)：同级不重复通知
  notified_assignee_idx INTEGER NOT NULL DEFAULT -1,
  opened_at   REAL NOT NULL,          -- 服务端开单时刻
  first_ts    REAL NOT NULL,          -- 异常窗口：首个越限样本的设备时刻
  last_ts     REAL NOT NULL,          -- 异常窗口：最近越限样本的设备时刻
  peak_temp   REAL,                   -- 窗口内峰值（高温取 max，低温取 min）
  resolved_at REAL,
  detail      TEXT
);
CREATE INDEX IF NOT EXISTS idx_alerts_ship ON alerts (shipment_id, type, status);

CREATE TABLE IF NOT EXISTS alert_events (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id   INTEGER NOT NULL,
  action     TEXT NOT NULL,  -- OPENED/ACKED/ESCALATED/RESOLVED/NOTE/LATE_DATA/RECOVERED/AUTO_RESOLVED/LEVEL_UP/NOTIFY
  actor      TEXT,
  note       TEXT,
  created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_alert ON alert_events (alert_id);

CREATE TABLE IF NOT EXISTS device_state (
  device_id TEXT PRIMARY KEY,
  online    INTEGER NOT NULL DEFAULT 0,
  last_seen REAL
);
"""

# 老库平滑升级：新列不存在时 ALTER TABLE 补上（新库由上面的 SCHEMA 直接建好）
_MIGRATIONS = [
    ("shipments", "escalation_chain", "TEXT NOT NULL DEFAULT '[]'"),
    ("shipments", "escalate_after_sec", "REAL NOT NULL DEFAULT 60.0"),
    ("alerts", "severity", "TEXT NOT NULL DEFAULT 'L1'"),
    ("alerts", "assignee", "TEXT"),
    ("alerts", "assignee_idx", "INTEGER NOT NULL DEFAULT 0"),
    ("alerts", "assignee_since", "REAL"),
    ("alerts", "notified_severity", "TEXT"),
    ("alerts", "notified_assignee_idx", "INTEGER NOT NULL DEFAULT -1"),
]


def connect(db_path):
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path):
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)
        for table, col, ddl in _MIGRATIONS:
            cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
            if col not in cols:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")
