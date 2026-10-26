"""
ClickHouse: таблицы -> загрузка CSV через file() -> синтетические каналы и расходы -> витрины.

Команды (можно несколько подряд):
    python pipeline/pipeline.py tables      # БД и таблицы
    python pipeline/pipeline.py load        # CSV -> shop.events через file() на стороне сервера
    python pipeline/pipeline.py channels    # shop.user_channels
    python pipeline/pipeline.py spend       # shop.channel_spend
    python pipeline/pipeline.py views       # представления для Superset
    python pipeline/pipeline.py             # все шаги подряд

Требования для load: папка data/ смонтирована в контейнер как
    ./data:/var/lib/clickhouse/user_files:ro

Настройки из .env: CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_HOST, CLICKHOUSE_PORT.

СИНТЕТИКА (не факты из данных):
    shop.user_channels : канал привлечения; acquisition_date = дата первого события
    shop.channel_spend : расходы и маржинальность по дням и каналам
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
    """Подключение создаётся при первом обращении (удобно для импорта из Airflow)."""
    global _client
    if _client is None:
        _client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            send_receive_timeout=3600,   # загрузка из file() идёт долго
        )
    return _client


# 1. DDL
DDL = [
    "CREATE DATABASE IF NOT EXISTS shop",
    """
    CREATE TABLE IF NOT EXISTS shop.events (
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
    """,
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
]


def create_tables():
    for q in DDL:
        get_client().command(q)
    print("tables: ok")


#  2. Загрузка CSV через file()
FILE_STRUCTURE = (
    "event_time String, event_type String, product_id UInt64, category_id UInt64, "
    "category_code String, brand String, price Float64, user_id UInt64, user_session String"
)

INSERT_FROM_FILE_SQL = """
INSERT INTO shop.events
SELECT
    parseDateTimeBestEffort(event_time, 'UTC') AS event_time,
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
    # Имена берём из локальной папки data/, читает файлы сам ClickHouse
    # из /var/lib/clickhouse/user_files (туда смонтирован ./data).
    client = get_client()
    names = sorted(os.path.basename(f) for f in glob.glob(DATA_GLOB))
    if not names:
        raise SystemExit(f"Нет файлов по маске {DATA_GLOB}")

    client.command("TRUNCATE TABLE shop.events")
    for name in names:
        print("loading", name, flush=True)
        before = int(client.command("SELECT count() FROM shop.events"))
        client.command(INSERT_FROM_FILE_SQL.format(name=name, structure=FILE_STRUCTURE))
        after = int(client.command("SELECT count() FROM shop.events"))
        print(f"  +{after - before:,} строк", flush=True)

    print("events total:", f"{int(client.command('SELECT count() FROM shop.events')):,}")
    print(client.query_df(
        "SELECT event_type, count() AS rows, countIf(price <= 0) AS bad_price "
        "FROM shop.events GROUP BY event_type ORDER BY rows DESC"
    ).to_string(index=False))


# 3. Каналы (синтетика)
# Детерминированно (хэш user_id), неравномерно, с зависимостью от наличия покупки.
# Сценарное допущение: платные каналы приводят больше "не покупателей",
# органика и рефералы - больше покупателей.
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
    FROM shop.events
    GROUP BY user_id
)
"""


def build_user_channels():
    client = get_client()
    client.command("TRUNCATE TABLE shop.user_channels")
    client.command(USER_CHANNELS_SQL)
    print(client.query_df(
        "SELECT channel, count() AS users FROM shop.user_channels "
        "GROUP BY channel ORDER BY users DESC"
    ))


#  4. Расходы (синтетика)
CHANNEL_PARAMS = {
    #                 budget_share, gross_margin_rate
    "paid_search":    (0.40, 0.22),
    "social":         (0.30, 0.20),
    "email":          (0.05, 0.28),
    "referral":       (0.10, 0.25),
    "organic_search": (0.15, 0.30),
}  # доли бюджета в сумме = 1.0

# Сценарное допущение: маркетинг ~ 8.75% выручки (~35% валовой маржи при марже ~25%)
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
        "SELECT sum(toFloat64(price)) FROM shop.events "
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
    print("channel_spend rows:", len(df))


#  5. Витрины
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
    # Выручка - из реальных purchase; расходы и маржа - синтетические.
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
        FROM shop.events AS e
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
    print(client.query_df(
        "SELECT * FROM shop.v_channel_unit_economics ORDER BY ltv_cac DESC"
    ).to_string())


# CLI
STEPS = {
    "tables": create_tables,
    "load": load_csv,
    "channels": build_user_channels,
    "spend": build_channel_spend,
    "views": build_views,
}

if __name__ == "__main__":
    steps = sys.argv[1:] or list(STEPS)
    unknown = [s for s in steps if s not in STEPS]
    if unknown:
        raise SystemExit(f"Неизвестные шаги: {unknown}. Доступны: {list(STEPS)}")
    for s in steps:
        print(f"--- {s}", flush=True)
        STEPS[s]()