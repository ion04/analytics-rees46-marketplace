"""
ClickHouse: Medallion Architecture Pipeline (Bronze -> Silver -> Gold).

Команды:
    python pipeline.py tables      # Создание БД и таблиц
    python pipeline.py load        # CSV -> shop.events_raw (Bronze)
    python pipeline.py silver      # Очистка, дедупликация, поиск ботов (Silver)
    python pipeline.py channels    # Построение витрины каналов (Gold)
    python pipeline.py spend       # Генерация синтетических расходов (Gold)
    python pipeline.py cohorts     # Построение витрины когорт (Gold)
    python pipeline.py views       # Создание View для Superset
    python pipeline.py             # Запуск всего пайплайна 
"""
import glob
import os
import sys

import clickhouse_connect
import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DATA_GLOB = os.getenv("DATA_CSV", "data/*.csv")
SEED = int(os.getenv("SEED", "26"))

_client = None

def get_client():
    """Подключение создаётся при первом обращении."""
    global _client
    if _client is None:
        _client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            send_receive_timeout=3600,
        )
    return _client


# 1. DDL (Создание таблиц)
# ==========================================
BRONZE_DDL = [
    "CREATE DATABASE IF NOT EXISTS shop",
    """
     CREATE TABLE IF NOT EXISTS shop.events_raw (
        event_time    DateTime('UTC'),
        event_type    LowCardinality(String),
        product_id    UInt64,
        category_id   UInt64,
        category_code LowCardinality(Nullable(String)),
        brand         LowCardinality(Nullable(String)),
        price         Decimal64(2),
        user_id       UInt64,
        user_session  Nullable(UUID)
    ) ENGINE = MergeTree()
    ORDER BY (event_type, user_id, event_time)
    """
]

SILVER_DDL = [
    """
    CREATE TABLE IF NOT EXISTS shop.excluded_users (
        user_id UInt64,
        reason LowCardinality(String)
    ) ENGINE = MergeTree() ORDER BY user_id
    """,
    """
    CREATE TABLE IF NOT EXISTS shop.products_dim (
        product_id UInt64,
        category_id UInt64,
        category_code LowCardinality(String),
        brand         LowCardinality(String)
    ) ENGINE = ReplacingMergeTree()
    ORDER BY product_id
    """,
    """
    CREATE TABLE IF NOT EXISTS shop.events (
        event_time    DateTime('UTC'),
        event_type    LowCardinality(String),
        product_id    UInt64,
        category_id   UInt64,
        category_code LowCardinality(String),
        brand         LowCardinality(String),
        price         Decimal64(2),
        user_id       UInt64,
        user_session  UUID
    ) ENGINE = ReplacingMergeTree(event_time)
    PARTITION BY toYYYYMM(event_time)
    ORDER BY (event_type, user_id, event_time, product_id, user_session)
    """
]

GOLD_DDL = [
    """
    CREATE TABLE IF NOT EXISTS shop.user_channels (
        user_id          UInt64,
        acquisition_date Date,
        channel          LowCardinality(String)
    ) ENGINE = MergeTree() ORDER BY user_id
    """,
    """
    CREATE TABLE IF NOT EXISTS shop.channel_spend (
        date              Date,
        channel           LowCardinality(String),
        spend             Float64,
        gross_margin_rate Float64
    ) ENGINE = MergeTree() ORDER BY (channel, date)
    """,
    """
    CREATE TABLE IF NOT EXISTS shop.cohort_retention (
        cohort       Date,
        month_index  UInt8,
        active_users UInt64,
        buyers       UInt64
    ) ENGINE = MergeTree() ORDER BY (cohort, month_index)
    """
]

def create_tables():
    client = get_client()
    for q in BRONZE_DDL + SILVER_DDL + GOLD_DDL:
        client.command(q)
    print("tables: ok (Bronze, Silver, Gold)", flush=True)

# ==========================================
# 2. Bronze Layer (Загрузка CSV)
# ==========================================
FILE_STRUCTURE = (
    "event_time String, event_type String, product_id UInt64, category_id UInt64, "
    "category_code Nullable(String), brand Nullable(String), price Float64, user_id UInt64, user_session String"
)

INSERT_FROM_FILE_SQL = """
INSERT INTO shop.events_raw
SELECT
    ifNull(parseDateTimeBestEffortOrNull(event_time, 'UTC'), toDateTime('1970-01-01', 'UTC')) AS event_time,
    event_type,
    product_id,
    category_id,
    nullIf(category_code, '')                  AS category_code,
    nullIf(brand, '')                          AS brand,
    toDecimal64(price, 2)                      AS price,
    user_id,
    toUUIDOrNull(user_session)                 AS user_session
FROM file('{name}', 'CSVWithNames', '{structure}')
"""

def load_csv():
    client = get_client()
    names = sorted(os.path.basename(f) for f in glob.glob(DATA_GLOB))
    if not names:
        raise SystemExit(f"Нет файлов по маске {DATA_GLOB}")
    
    client.command("TRUNCATE TABLE shop.events_raw")
    for name in names:
        print("loading", name, flush=True)
        before = int(client.command("SELECT count() FROM shop.events_raw"))
        client.command(INSERT_FROM_FILE_SQL.format(name=name, structure=FILE_STRUCTURE))
        after = int(client.command("SELECT count() FROM shop.events_raw"))
        print(f"  +{after - before:,} строк", flush=True)

    print("events total:", f"{int(client.command('SELECT count() FROM shop.events_raw')):,}", flush=True)


# 3. Silver Layer (Очистка и дедупликация)

SILVER_LAYER_PIPELINE = {
    'truncates' : [
        "TRUNCATE TABLE shop.events",
        "TRUNCATE TABLE shop.excluded_users",
        "TRUNCATE TABLE shop.products_dim"
    ],
    'insertions' : [
        # 1. Боты по частоте
        """
        INSERT INTO shop.excluded_users (user_id, reason)
        SELECT DISTINCT user_id, 'high_daily_frequency' AS reason
        FROM (
            SELECT user_id, toDate(event_time) AS event_date, count() AS total_events
            FROM shop.events_raw
            GROUP BY user_id, event_date
            HAVING total_events > 999
        )
        """,
        # 2. Боты по скорости в сессии
        """
        INSERT INTO shop.excluded_users (user_id, reason)
        SELECT DISTINCT 
            user_id, 
            'inhuman_session_speed' AS reason
        FROM (
            SELECT 
                user_id,
                dateDiff('second', min(event_time), max(event_time)) AS session_duration_sec,
                count() AS events_in_session
            FROM shop.events_raw
            WHERE user_session IS NOT NULL
            GROUP BY user_id, user_session 
            HAVING events_in_session > 50 AND session_duration_sec < 60
        )
        """,
        # 3. Справочник товаров (SCD Type 1)
        """
        INSERT INTO shop.products_dim
        SELECT
            product_id,
            argMax(category_id, event_time) AS category_id,
            argMax(ifNull(category_code, 'unknown'), event_time) AS category_code,
            argMax(ifNull(brand, 'unknown'), event_time) AS brand
        FROM shop.events_raw
        GROUP BY product_id
        """,
        # 4. Основная таблица событий
        """
        INSERT INTO shop.events
        SELECT
            event_time,
            event_type,
            product_id,
            category_id,
            ifNull(category_code, 'unknown') AS category_code,
            ifNull(brand, 'unknown') AS brand,
            price,
            user_id,
            user_session
        FROM shop.events_raw
        WHERE user_session IS NOT NULL
          AND (event_type != 'purchase' OR price > 0)
        """
    ]
}

def build_silver_layer():
    client = get_client()
    for stage_name, queries in SILVER_LAYER_PIPELINE.items():
        print(f"--> Этап: {stage_name}...", flush=True)
        for q in queries:
            client.command(q)
    print("Silver layer: завершен", flush=True)

# 4. Gold Layer: Каналы

USER_CHANNELS_SQL = """
INSERT INTO shop.user_channels
SELECT
    user_id,
    acquisition_date,
    multiIf(
        buyer = 1,
            multiIf(r < 0.30, 'organic_search', r < 0.50, 'referral',
                    r < 0.70, 'email',          r < 0.90, 'paid_search', 'social'),
            multiIf(r < 0.20, 'organic_search', r < 0.25, 'referral',
                    r < 0.35, 'email',          r < 0.70, 'paid_search', 'social')
    ) AS channel
FROM (
    SELECT
        user_id,
        toDate(min(event_time)) AS acquisition_date,
        countIf(event_type = 'purchase' AND price > 0) > 0 AS buyer,
        (cityHash64(user_id, 'channel_salt') % 10000) / 10000 AS r
    FROM shop.events FINAL
    LEFT ANTI JOIN shop.excluded_users USING (user_id) -- Исключаем ботов
    GROUP BY user_id
)
"""

def build_user_channels():
    client = get_client()
    client.command("TRUNCATE TABLE shop.user_channels")
    client.command(USER_CHANNELS_SQL)
    print("user_channels: ok", flush=True)
    print(client.query_df(
        "SELECT channel, count() AS users FROM shop.user_channels "
        "GROUP BY channel ORDER BY users DESC"
    ).to_string(index=False))


# 5. Gold Layer: Расходы

CHANNEL_PARAMS = {
    "paid_search":    (0.40, 0.22),
    "social":         (0.30, 0.20),
    "email":          (0.05, 0.28),
    "referral":       (0.10, 0.25),
    "organic_search": (0.15, 0.30),
}
MARKETING_SHARE_OF_REVENUE = 0.0875

def build_channel_spend():
    client = get_client()
    rng = np.random.default_rng(SEED)

    days = pd.date_range(
        client.command("SELECT min(acquisition_date) FROM shop.user_channels"),
        client.command("SELECT max(acquisition_date) FROM shop.user_channels"),
        freq="D",
    )
    revenue = float(client.command(
        "SELECT sum(toFloat64(price)) FROM shop.events FINAL " 
        "LEFT ANTI JOIN shop.excluded_users USING (user_id) "
        "WHERE event_type = 'purchase' AND price > 0"
    ))
    daily_budget = MARKETING_SHARE_OF_REVENUE * revenue / days.size

    rows = []
    for ch, (share, margin) in CHANNEL_PARAMS.items():
        spend = daily_budget * share * rng.lognormal(0, 0.15, days.size)
        m = np.clip(margin + rng.normal(0, 0.01, days.size), 0.05, 0.6)
        rows.append(pd.DataFrame({
            "date": days.date, "channel": ch,
            "spend": spend.round(2), "gross_margin_rate": m.round(4),
        }))
    df = pd.concat(rows, ignore_index=True)

    client.command("TRUNCATE TABLE shop.channel_spend")
    client.insert_df("shop.channel_spend", df)
    print("channel_spend rows:", len(df), flush=True)


# 6. Gold Layer: Когорты
COHORTS_SQL = """
INSERT INTO shop.cohort_retention
WITH cohorts AS (
    SELECT user_id, toStartOfMonth(min(event_time)) AS cohort
    FROM shop.events FINAL
    LEFT ANTI JOIN shop.excluded_users USING (user_id)
    GROUP BY user_id
)
SELECT
    c.cohort AS cohort,
    toUInt8(dateDiff('month', c.cohort, toStartOfMonth(e.event_time))) AS month_index,
    uniqExact(e.user_id) AS active_users,
    uniqExactIf(e.user_id, e.event_type = 'purchase' AND e.price > 0) AS buyers
FROM shop.events AS e FINAL
INNER JOIN cohorts AS c USING (user_id)
GROUP BY cohort, month_index
SETTINGS max_bytes_before_external_group_by = 2000000000
"""

def build_cohorts():
    client = get_client()
    client.command("TRUNCATE TABLE shop.cohort_retention")
    client.command(COHORTS_SQL)
    print("cohorts: ok", flush=True)
    print(client.query_df(
        "SELECT cohort, month_index, active_users, buyers "
        "FROM shop.cohort_retention ORDER BY cohort, month_index LIMIT 15"
    ).to_string(index=False))


# 7. Gold Layer: View для Superset
VIEWS = [
    """
    CREATE OR REPLACE VIEW shop.v_channel_daily AS
    SELECT
        s.date AS date, s.channel AS channel,
        s.spend AS spend,
        s.gross_margin_rate AS gross_margin_rate,
        coalesce(u.new_users, 0) AS new_users
    FROM shop.channel_spend AS s
    LEFT JOIN (
        SELECT acquisition_date AS date, channel, count() AS new_users
        FROM shop.user_channels GROUP BY date, channel
    ) AS u USING (date, channel)
    """,
    """
    CREATE OR REPLACE VIEW shop.v_cohort_retention AS
    SELECT
        formatDateTime(r.cohort, '%Y-%m') AS cohort_month,
        r.cohort AS cohort,
        r.month_index AS month_index,
        r.active_users AS active_users,
        r.buyers AS buyers,
        z.cohort_size AS cohort_size,
        r.active_users / z.cohort_size AS retention,
        r.buyers / z.cohort_size AS purchase_retention
    FROM shop.cohort_retention AS r
    INNER JOIN (
        SELECT cohort, active_users AS cohort_size
        FROM shop.cohort_retention WHERE month_index = 0
    ) AS z USING (cohort)
    """,
    """
    CREATE OR REPLACE VIEW shop.v_channel_unit_economics AS
    SELECT
        c.channel AS channel,
        c.users AS users,
        sp.spend AS spend,
        sp.spend / c.users AS cac,
        r.revenue AS revenue,
        r.revenue * sp.avg_margin AS gross_margin,
        r.revenue * sp.avg_margin / c.users AS ltv,
        (r.revenue * sp.avg_margin / c.users) / (sp.spend / c.users) AS ltv_cac,
        r.buyers / c.users AS conversion_to_purchase,
        r.revenue * sp.avg_margin - sp.spend AS profit
    FROM (SELECT channel, count() AS users FROM shop.user_channels GROUP BY channel) AS c
    LEFT JOIN (
        SELECT channel, sum(spend) AS spend, avg(gross_margin_rate) AS avg_margin
        FROM shop.channel_spend GROUP BY channel
    ) AS sp USING (channel)
    LEFT JOIN (
        SELECT uc.channel AS channel,
               sum(toFloat64(e.price)) AS revenue,
               uniqExact(e.user_id) AS buyers
        FROM shop.events AS e FINAL 
        INNER JOIN shop.user_channels AS uc USING (user_id)
        WHERE e.event_type = 'purchase' AND e.price > 0
        GROUP BY channel
    ) AS r USING (channel)
    """,
]

def build_views():
    client = get_client()
    for q in VIEWS:
        client.command(q)
    print("views: ok", flush=True)
    print(client.query_df(
        "SELECT * FROM shop.v_channel_unit_economics ORDER BY ltv_cac DESC"
    ).to_string(index=False))


# CLI
STEPS = {
    "tables": create_tables,
    "load": load_csv,
    "silver": build_silver_layer, 
    "channels": build_user_channels,
    "spend": build_channel_spend,
    "cohorts": build_cohorts,
    "views": build_views,
}

if __name__ == "__main__":
    steps = sys.argv[1:] or list(STEPS)
    unknown = [s for s in steps if s not in STEPS]
    if unknown:
        raise SystemExit(f"Неизвестные шаги: {unknown}. Доступны: {list(STEPS)}")
    
    for s in steps:
        print(f"\n--- Запуск: {s} ---", flush=True)
        STEPS[s]()