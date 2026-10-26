import glob
import os

import clickhouse_connect
import numpy as np
import pandas as pd

from dotenv import load_dotenv
load_dotenv()

DATA_GLOB = os.getenv('DATA_CSV', 'data/*.csv')
SEED = int(os.getenv('SEED', '26'))

client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username=os.getenv('CLICKHOUSE_USER', 'default'),
    password=os.getenv('CLICKHOUSE_PASSWORD', ''),
)

#  1. DDL
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
        client.command(q)


# 2. Загрузка CSV
def load_csv():
    files = sorted(glob.glob(DATA_GLOB))
    if not files:
        raise SystemExit(f"Нет файлов по маске {DATA_GLOB}")
    client.command("TRUNCATE TABLE shop.events")  
    for f in files:
        print("loading", f)
        client.insert_file(
            f,
            "shop.events",
            fmt="CSVWithNames",
            settings={"date_time_input_format": "best_effort"},
        )
    print("events:", client.command("SELECT count() FROM shop.events"))


# 3. Каналы (синтетика)
# Назначение детерминировано (хэш user_id), распределение неравномерное
# и зависит от того, покупал ли пользователь. Это сценарное допущение:
# платные каналы приводят больше "не покупателей", органика и рефералы - больше покупателей.
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
        countIf(event_type = 'purchase') > 0 AS buyer,
        (cityHash64(user_id, 'channel_salt') % 10000) / 10000 AS r
    FROM shop.events
    GROUP BY user_id
)
"""


def build_user_channels():
    client.command("TRUNCATE TABLE shop.user_channels")
    client.command(USER_CHANNELS_SQL)
    print(client.query_df(
        "SELECT channel, count() AS users FROM shop.user_channels GROUP BY channel ORDER BY users DESC"
    ))


# 4. Расходы (синтетика)
CHANNEL_PARAMS = {
    #                 budget_share, gross_margin_rate
    'paid_search':           (0.40, 0.22),
    'social':                (0.30, 0.20),
    'email':                 (0.05, 0.28),
    'referral':              (0.10, 0.25),
    'organic_search':        (0.15, 0.30)
}

MARKETING_RATIO = 0.35

def build_channel_spend():
    rng = np.random.default_rng(SEED)

    days = pd.date_range(
        client.command("SELECT min(acquisition_date) FROM shop.user_channels"),
        client.command("SELECT max(acquisition_date) FROM shop.user_channels"),
        freq="D",
    )
    
    revenue = float(client.command(
        "SELECT toFloat64(sum(price)) FROM shop.events WHERE event_type = 'purchase'"
    ))

    avg_margin = 0.25
    daily_budget = MARKETING_RATIO * avg_margin * revenue / days.size

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





## 5. Витрины для Superset
#VIEWS = [
#    # Дневной факт по каналам: новые пользователи, расходы, выручка и маржа от реальных purchase
#    """
#    CREATE OR REPLACE VIEW shop.v_channel_daily AS
#    SELECT
#        s.date AS date, s.channel AS channel,
#        s.spend AS spend,
#        s.gross_margin_rate AS gross_margin_rate,
#        coalesce(u.new_users, 0) AS new_users
#    FROM shop.channel_spend AS s
#    LEFT JOIN (
#        SELECT acquisition_date AS date, channel, count() AS new_users
#        FROM shop.user_channels GROUP BY date, channel
#    ) AS u USING (date, channel)
#    """,
#    # Юнит-экономика по каналам: CAC, LTV (по марже), LTV/CAC.
#    # Выручка - из реальных purchase; маржа и расходы - синтетические.
#    """
#    CREATE OR REPLACE VIEW shop.v_channel_unit_economics AS
#    SELECT
#        c.channel AS channel,
#        c.users AS users,
#        sp.spend AS spend,
#        sp.spend / c.users AS cac,
#        r.revenue AS revenue,
#        r.revenue * sp.avg_margin AS gross_margin,
#        r.revenue * sp.avg_margin / c.users AS ltv,
#        (r.revenue * sp.avg_margin / c.users) / (sp.spend / c.users) AS ltv_cac,
#        r.buyers / c.users AS conversion_to_purchase
#    FROM (SELECT channel, count() AS users FROM shop.user_channels GROUP BY channel) AS c
#    LEFT JOIN (
#        SELECT channel, sum(spend) AS spend, avg(gross_margin_rate) AS avg_margin
#        FROM shop.channel_spend GROUP BY channel
#    ) AS sp USING (channel)
#    LEFT JOIN (
#        SELECT uc.channel AS channel, sum(e.price) AS revenue, uniqExact(e.user_id) AS buyers
#        FROM shop.events AS e
#        INNER JOIN shop.user_channels AS uc USING (user_id)
#        WHERE e.event_type = 'purchase'
#        GROUP BY channel
#    ) AS r USING (channel)
#    """,
#]
#
#
#def build_views():
#    for q in VIEWS:
#        client.command(q)
#    print(client.query_df("SELECT * FROM shop.v_channel_unit_economics ORDER BY ltv_cac DESC"))

if __name__ == "__main__":
    create_tables()
    load_csv()
    build_user_channels()
    build_channel_spend()
    #build_views()