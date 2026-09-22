# Databricks notebook source
# MAGIC %md
# MAGIC # Energy Trading Lakehouse — Workshop Dataset
# MAGIC
# MAGIC Builds a complete energy-trading star schema in Unity Catalog, ready for
# MAGIC **AI/BI Dashboards** and **Genie**.
# MAGIC
# MAGIC ### What this notebook creates
# MAGIC | Step | What it builds |
# MAGIC |---|---|
# MAGIC | 1 | Configuration |
# MAGIC | 2 | Dimension tables (who / what / where / when / how) |
# MAGIC | 3 | Fact tables (the trades, prices and daily positions) |
# MAGIC | 4 | Keys and relationships |
# MAGIC | 5 | FX rates + the enriched trade view (adds market price & EUR values) |
# MAGIC | 6 | The metric view — the governed layer Genie and dashboards use |
# MAGIC | 7 | AI-generated descriptions (optional) |
# MAGIC
# MAGIC ### How to run it
# MAGIC Set the widgets in **Step 1**, then **Run all**. Takes a few minutes.
# MAGIC
# MAGIC > The data is synthetic but calibrated to real 2024–2026 energy benchmarks.
# MAGIC > It represents a whole **market** — every participant's trades — not one company.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 · Configuration
# MAGIC
# MAGIC Set your catalog and schema, then run this cell.

# COMMAND ----------

# DBTITLE 1,Setup

########################################
# Set your target catalog and schema here, then Run All.
########################################
dbutils.widgets.text("catalog", "energy", "Catalog")
dbutils.widgets.text("schema",  "energy_trading",       "Schema")
CATALOG = dbutils.widgets.get("catalog").strip() or "energy"
SCHEMA  = dbutils.widgets.get("schema").strip()  or "energy_trading"

from datetime import date as _date

NUM_TRADES = 1000_000
DATE_START = "2024-01-01"
# DATE_END is pinned so the dataset is reproducible between runs.
dbutils.widgets.text("date_end", "2026-09-30", "Date End (YYYY-MM-DD, blank = today)")
# Pinned by default so every run produces the same data for the workshop.
# Clear the widget to use today's date instead.
DATE_END = dbutils.widgets.get("date_end").strip() or _date.today().isoformat()

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Target: {CATALOG}.{SCHEMA}")
print(f"Trades: {NUM_TRADES:,}")
print(f"Date range: {DATE_START} → {DATE_END}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 · Dimension tables
# MAGIC
# MAGIC The "lookup" tables that describe each trade: the trader, the counterparty,
# MAGIC the product, the delivery location, the market type and the calendar.

# COMMAND ----------

# DBTITLE 1,dim_time — unified date+hour dimension, key = yyyyMMddHH
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Generate all dates × 24 hours
df_dates = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{DATE_START}'),
        to_date('{DATE_END}'),
        interval 1 day
    )) AS cal_date
""")

df_hours = spark.range(0, 24).withColumnRenamed("id", "hour")

df_time = (
    df_dates.crossJoin(df_hours)
    # Primary key: yyyyMMddHH string
    .withColumn("time_key",
        F.concat(F.date_format("cal_date", "yyyyMMdd"), F.lpad(F.col("hour").cast("string"), 2, "0"))
    )
    # Timestamp
    .withColumn("event_timestamp",
        F.to_timestamp(F.concat(F.col("cal_date").cast("string"), F.lit(" "), F.lpad(F.col("hour").cast("string"), 2, "0"), F.lit(":00:00")))
    )
    # Date attributes
    .withColumn("cal_date", F.col("cal_date"))
    .withColumn("year", F.year("cal_date"))
    .withColumn("quarter", F.quarter("cal_date"))
    .withColumn("month", F.month("cal_date"))
    .withColumn("month_name", F.date_format("cal_date", "MMMM"))
    .withColumn("week_of_year", F.weekofyear("cal_date"))
    .withColumn("day_of_week", F.dayofweek("cal_date"))
    .withColumn("day_name", F.date_format("cal_date", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("cal_date").isin(1, 7))
    .withColumn("is_business_day", ~F.dayofweek("cal_date").isin(1, 7))
    .withColumn("fiscal_year",
        F.when(F.month("cal_date") >= 10, F.year("cal_date") + 1)
         .otherwise(F.year("cal_date"))
    )
    .withColumn("fiscal_quarter",
        F.when(F.month("cal_date") >= 10, F.concat(F.lit("Q"), ((F.month("cal_date") - 10) / 3 + 1).cast("int").cast("string")))
         # Oct fiscal start: Jan-Mar=Q2, Apr-Jun=Q3, Jul-Sep=Q4 → floor((m+2)/3)+1
         .otherwise(F.concat(F.lit("Q"), (((F.month("cal_date") + 2) / 3).cast("int") + 1).cast("string")))
    )
    .withColumn("trading_season",
        F.when(F.month("cal_date").isin(10, 11, 12, 1, 2, 3), "Winter")
         .otherwise("Summer")
    )
    # Hour attributes
    .withColumn("hour_of_day", F.col("hour"))
    .withColumn("hour_label",
        F.concat(F.lpad(F.col("hour").cast("string"), 2, "0"), F.lit(":00–"), F.lpad(F.col("hour").cast("string"), 2, "0"), F.lit(":59"))
    )
    .withColumn("period_type", F.when((F.col("hour") >= 8) & (F.col("hour") <= 20), "Peak").otherwise("Off-Peak"))
    .withColumn("is_peak", (F.col("hour") >= 8) & (F.col("hour") <= 20))
    .withColumn("is_super_peak", (F.col("hour") >= 10) & (F.col("hour") <= 14))
    .drop("hour")
)

df_time.write.mode("overwrite").saveAsTable("dim_time")
print(f"dim_time: {df_time.count():,} rows")

# COMMAND ----------

# DBTITLE 1,dim_geography — country + continent
from pyspark.sql import Row

geography_data = [
    (1, "NO", "Norway", "Europe", "EUR", "CET+1"),
    (2, "SE", "Sweden", "Europe", "SEK", "CET+1"),
    (3, "DK", "Denmark", "Europe", "DKK", "CET"),
    (4, "FI", "Finland", "Europe", "EUR", "EET"),
    (5, "DE", "Germany", "Europe", "EUR", "CET"),
    (6, "FR", "France", "Europe", "EUR", "CET"),
    (7, "NL", "Netherlands", "Europe", "EUR", "CET"),
    (8, "BE", "Belgium", "Europe", "EUR", "CET"),
    (9, "UK", "United Kingdom", "Europe", "GBP", "GMT"),
    (10, "CH", "Switzerland", "Europe", "CHF", "CET"),
    (11, "AT", "Austria", "Europe", "EUR", "CET"),
    (12, "IT", "Italy", "Europe", "EUR", "CET"),
    (13, "ES", "Spain", "Europe", "EUR", "CET"),
    (14, "US", "United States", "North America", "USD", "EST"),
    (15, "SG", "Singapore", "Asia", "SGD", "SGT"),
    (16, "XX", "Unknown", "Unknown", "USD", "UTC"),
]

geographies = [
    Row(
        geography_key=gk,
        country_code=cc,
        country_name=name,
        continent=continent,
        local_currency=currency,
        timezone=tz,
    )
    for gk, cc, name, continent, currency, tz in geography_data
]

df_geography = spark.createDataFrame(geographies)
df_geography.write.mode("overwrite").saveAsTable("dim_geography")
print(f"dim_geography: {df_geography.count()} rows")

# COMMAND ----------

# DBTITLE 1,dim_trader — 121 traders across 13 companies
import random

companies = [
    ("Danske Commodities", "DK", "Trader"),
    ("Equinor ASA", "NO", "Producer"),
    ("Vattenfall AB", "SE", "Utility"),
    ("Statkraft AS", "NO", "Producer"),
    ("E.ON SE", "DE", "Utility"),
    ("EDF SA", "FR", "Utility"),
    ("Shell Energy", "NL", "Major"),
    ("BP Trading", "UK", "Major"),
    ("Axpo Group", "CH", "Utility"),
    ("Centrica plc", "UK", "Utility"),
    ("Fortum Oyj", "FI", "Utility"),
    ("Ørsted A/S", "DK", "Utility"),
    ("Kong Corporation", "XX", "Producer"),
]

desks = [
    ("Power-Nordic", "Power Trading"),
    ("Power-CWE", "Power Trading"),
    ("Power-UK", "Power Trading"),
    ("Gas-TTF", "Gas Trading"),
    ("Gas-NBP", "Gas Trading"),
    ("Oil-Brent", "Oil Trading"),
    ("Carbon-EUA", "Environmental Trading"),
    ("Renewables", "Green Trading"),
    ("LNG-Global", "LNG Trading"),
]

seniorities = ["Junior Trader", "Trader", "Senior Trader", "Head of Desk"]
first_names = ["Erik", "Anna", "Lars", "Sofia", "Magnus", "Ingrid", "Olof", "Freya",
               "Niels", "Katrine", "Bjorn", "Astrid", "Henrik", "Maja", "Sven", "Elsa",
               "Anders", "Linnea", "Petter", "Saga", "Rasmus", "Liv", "Axel", "Tove",
               "Oscar", "Ida", "Mikkel", "Nora", "Johan", "Emilia", "Tobias", "Clara",
               "Viktor", "Freja", "Filip", "Hanna", "Lukas", "Sigrid", "William", "Thea"]
last_names = ["Andersen", "Johansson", "Nielsen", "Larsen", "Hansen", "Berg", "Lindqvist",
              "Mikkelsen", "Dahl", "Petersen", "Strand", "Holm", "Lund", "Sørensen",
              "Eriksson", "Olsen", "Virtanen", "Korhonen", "Müller", "Dupont"]

# Controlled company allocation — Equinor gets 18 traders (including 6 on LNG desk)
# to reflect their major role as a top-3 global LNG producer
company_allocation = [
    ("Equinor ASA", "NO", "Producer", 18),
    ("Shell Energy", "NL", "Major", 14),
    ("BP Trading", "UK", "Major", 12),
    ("Vattenfall AB", "SE", "Utility", 10),
    ("Statkraft AS", "NO", "Producer", 10),
    ("E.ON SE", "DE", "Utility", 10),
    ("EDF SA", "FR", "Utility", 8),
    ("Danske Commodities", "DK", "Trader", 10),
    ("Axpo Group", "CH", "Utility", 8),
    ("Centrica plc", "UK", "Utility", 6),
    ("Fortum Oyj", "FI", "Utility", 7),
    ("Ørsted A/S", "DK", "Utility", 7),
    # Kong Corporation: 1 sole trader — King Kong (added manually below)
]

random.seed(42)
traders = []
trader_key = 1
for company_name, company_country, company_type, count in company_allocation:
    for j in range(count):
        # Equinor: first 6 traders on LNG desk, rest on other desks
        if company_name == "Equinor ASA" and j < 6:
            desk_name, desk_group = "LNG-Global", "LNG Trading"
        else:
            desk_name, desk_group = random.choice(desks)
        traders.append(Row(
            trader_key=trader_key,
            trader_id=f"TRD-{trader_key:04d}",
            trader_name=f"{random.choice(first_names)} {random.choice(last_names)}",
            company=company_name,
            company_country=company_country,
            company_type=company_type,
            desk=desk_name,
            desk_group=desk_group,
            seniority=random.choice(seniorities),
            is_active=random.random() > 0.1,
        ))
        trader_key += 1

# Kong Corporation — single trader "King Kong", LNG desk, gas producer only
KONG_TRADER_KEY = trader_key
traders.append(Row(
    trader_key=KONG_TRADER_KEY,
    trader_id=f"TRD-{KONG_TRADER_KEY:04d}",
    trader_name="King Kong",
    company="Kong Corporation",
    company_country="XX",
    company_type="Producer",
    desk="LNG-Global",
    desk_group="LNG Trading",
    seniority="Head of Desk",
    is_active=True,
))

# Manta Resources — single trader "B.Rock Van Guard", APAC multi-commodity
# Known for outsized clip sizes and selling well below the prevailing curve
MANTA_TRADER_KEY = KONG_TRADER_KEY + 1
traders.append(Row(
    trader_key=MANTA_TRADER_KEY,
    trader_id=f"TRD-{MANTA_TRADER_KEY:04d}",
    trader_name="B.Rock Van Guard",
    company="Manta Resources",
    company_country="SG",
    company_type="Trader",
    desk="Energy-LNG-APAC",
    desk_group="Multi-Commodity Trading",
    seniority="Head of Desk",
    is_active=True,
))

df_trader = spark.createDataFrame(traders)
df_trader.write.mode("overwrite").saveAsTable("dim_trader")
print(f"dim_trader: {df_trader.count()} rows")

# COMMAND ----------

# DBTITLE 1,dim_counterparty
counterparty_data = [
    ("Equinor ASA", "Producer", "NO", "AAA"),
    ("Vattenfall AB", "Utility", "SE", "AA"),
    ("Ørsted A/S", "Utility", "DK", "AA"),
    ("Statkraft AS", "Producer", "NO", "AAA"),
    ("Fortum Oyj", "Utility", "FI", "A"),
    ("E.ON SE", "Utility", "DE", "AA"),
    ("RWE AG", "Utility", "DE", "A"),
    ("EDF SA", "Utility", "FR", "AA"),
    ("Engie SA", "Utility", "FR", "A"),
    ("Shell Energy", "Major", "NL", "AAA"),
    ("BP Trading", "Major", "UK", "AAA"),
    ("TotalEnergies", "Major", "FR", "AA"),
    ("Axpo Group", "Utility", "CH", "A"),
    ("Uniper SE", "Utility", "DE", "BBB"),
    ("Centrica plc", "Utility", "UK", "A"),
    ("Verbund AG", "Utility", "AT", "AA"),
    ("Enel SpA", "Utility", "IT", "A"),
    ("Iberdrola SA", "Utility", "ES", "AA"),
    ("Energi Danmark", "Trader", "DK", "A"),
    ("Nord Pool Spot", "Exchange", "NO", "AAA"),
    ("EEX AG", "Exchange", "DE", "AAA"),
    ("ICE Endex", "Exchange", "NL", "AAA"),
    ("Nasdaq OMX Commodities", "Exchange", "NO", "AAA"),
    ("Danske Commodities", "Trader", "DK", "A"),
    ("Vitol Group", "Trader", "NL", "AA"),
    ("Trafigura", "Trader", "SG", "A"),
    ("Mercuria Energy", "Trader", "CH", "A"),
    ("Gunvor Group", "Trader", "CH", "BBB"),
    ("Koch Industries", "Trader", "US", "AA"),
    ("Glencore", "Trader", "CH", "A"),
    ("Kong Corporation", "Producer", "XX", "BBB"),
    ("Manta Resources", "Trader", "SG", "BBB"),
]

counterparties = [
    Row(
        counterparty_key=i + 1,
        counterparty_id=f"CP-{i + 1:04d}",
        counterparty_name=name,
        counterparty_type=cp_type,
        country_code=country,
        credit_rating=rating,
        is_exchange=cp_type == "Exchange",
    )
    for i, (name, cp_type, country, rating) in enumerate(counterparty_data)
]

df_counterparty = spark.createDataFrame(counterparties)
df_counterparty.write.mode("overwrite").saveAsTable("dim_counterparty")
print(f"dim_counterparty: {df_counterparty.count()} rows")

# COMMAND ----------

# DBTITLE 1,dim_instrument
instrument_data = [
    ("PWR-BASE-DA", "Power", "Baseload Day-Ahead", "MWh", "EUR"),
    ("PWR-PEAK-DA", "Power", "Peak Day-Ahead", "MWh", "EUR"),
    ("PWR-BASE-WK", "Power", "Baseload Week-Ahead", "MWh", "EUR"),
    ("PWR-BASE-MO", "Power", "Baseload Month-Ahead", "MWh", "EUR"),
    ("PWR-BASE-QT", "Power", "Baseload Quarter-Ahead", "MWh", "EUR"),
    ("PWR-BASE-YR", "Power", "Baseload Year-Ahead", "MWh", "EUR"),
    ("PWR-PEAK-WK", "Power", "Peak Week-Ahead", "MWh", "EUR"),
    ("PWR-PEAK-MO", "Power", "Peak Month-Ahead", "MWh", "EUR"),
    ("GAS-TTF-DA", "Natural Gas", "TTF Day-Ahead", "MWh", "EUR"),
    ("GAS-TTF-MO", "Natural Gas", "TTF Month-Ahead", "MWh", "EUR"),
    ("GAS-TTF-QT", "Natural Gas", "TTF Quarter-Ahead", "MWh", "EUR"),
    ("GAS-TTF-YR", "Natural Gas", "TTF Year-Ahead", "MWh", "EUR"),
    ("GAS-NBP-DA", "Natural Gas", "NBP Day-Ahead", "Therm", "GBP"),
    ("GAS-NBP-MO", "Natural Gas", "NBP Month-Ahead", "Therm", "GBP"),
    ("OIL-BRENT-FU", "Crude Oil", "Brent Futures", "Barrel", "USD"),
    ("OIL-WTI-FU", "Crude Oil", "WTI Futures", "Barrel", "USD"),
    ("CARBON-EUA-SP", "Carbon", "EUA Spot", "tCO2", "EUR"),
    ("CARBON-EUA-FU", "Carbon", "EUA Futures", "tCO2", "EUR"),
    ("GO-NORDIC", "Guarantees of Origin", "Nordic GO", "MWh", "EUR"),
    ("GO-EU", "Guarantees of Origin", "EU GO", "MWh", "EUR"),
    ("LNG-JKM-SP", "LNG", "JKM Spot", "MT", "USD"),
    ("LNG-JKM-FU", "LNG", "JKM Futures", "MT", "USD"),
    ("LNG-TTF-SP", "LNG", "TTF LNG Spot", "MT", "EUR"),
    ("LNG-DES-NWE", "LNG", "DES Northwest Europe", "MT", "EUR"),
]

instruments = [
    Row(
        instrument_key=i + 1,
        instrument_code=code,
        commodity=commodity,
        instrument_name=name,
        unit=unit,
        currency=currency,
        is_physical=commodity in ("Power", "Natural Gas", "Crude Oil", "LNG"),
        is_financial=commodity in ("Carbon", "Guarantees of Origin") or "Futures" in name,
    )
    for i, (code, commodity, name, unit, currency) in enumerate(instrument_data)
]

df_instrument = spark.createDataFrame(instruments)
df_instrument.write.mode("overwrite").saveAsTable("dim_instrument")
print(f"dim_instrument: {df_instrument.count()} rows")

# COMMAND ----------

# DBTITLE 1,dim_delivery_point — with geography_key FK
# geography_key mapping: NO=1, SE=2, DK=3, FI=4, DE=5, FR=6, NL=7, BE=8, UK=9, US=14
delivery_data = [
    ("NO1", "Norway South-East", 1, "Power"),
    ("NO2", "Norway South-West", 1, "Power"),
    ("NO3", "Norway Central", 1, "Power"),
    ("NO4", "Norway North", 1, "Power"),
    ("NO5", "Norway West", 1, "Power"),
    ("SE1", "Sweden Luleå", 2, "Power"),
    ("SE2", "Sweden Sundsvall", 2, "Power"),
    ("SE3", "Sweden Stockholm", 2, "Power"),
    ("SE4", "Sweden Malmö", 2, "Power"),
    ("DK1", "Denmark West", 3, "Power"),
    ("DK2", "Denmark East", 3, "Power"),
    ("FI", "Finland", 4, "Power"),
    ("SYS", "Nordic System Price", 1, "Power"),
    ("DE-LU", "Germany-Luxembourg", 5, "Power"),
    ("FR", "France", 6, "Power"),
    ("NL", "Netherlands", 7, "Power"),
    ("BE", "Belgium", 8, "Power"),
    ("UK", "United Kingdom", 9, "Power"),
    ("TTF", "Title Transfer Facility", 7, "Gas"),
    ("NBP", "National Balancing Point", 9, "Gas"),
    ("THE", "Trading Hub Europe", 5, "Gas"),
    ("ZTP", "Zeebrugge Trading Point", 8, "Gas"),
    ("ICE-BRENT", "ICE Brent Delivery", 9, "Oil"),
    ("NYMEX-WTI", "NYMEX WTI Delivery", 14, "Oil"),
    ("ICE-EUA", "ICE EUA Delivery", 9, "Carbon"),
    ("EEX-EUA", "EEX EUA Delivery", 5, "Carbon"),
    ("GATE-NL", "Gate Terminal Rotterdam", 7, "LNG"),
    ("DRAGON-UK", "Dragon LNG Milford Haven", 9, "LNG"),
    ("SNOHVIT-NO", "Snøhvit LNG Hammerfest", 1, "LNG"),
    ("DUNKERQUE-FR", "Dunkerque LNG Terminal", 6, "LNG"),
    ("KONG-XX", "Kong LNG Terminal", 16, "LNG"),
    ("SLNG-SG", "Singapore LNG Terminal (Jurong)", 15, "LNG"),
]

delivery_points = [
    Row(
        delivery_point_key=i + 1,
        zone_code=code,
        zone_name=name,
        geography_key=geo_key,
        commodity_class=commodity,
    )
    for i, (code, name, geo_key, commodity) in enumerate(delivery_data)
]

df_dp = spark.createDataFrame(delivery_points)
df_dp.write.mode("overwrite").saveAsTable("dim_delivery_point")
print(f"dim_delivery_point: {df_dp.count()} rows")

# COMMAND ----------

# DBTITLE 1,dim_market
market_data = [
    (1, "SPOT", "Spot / Day-Ahead", "Physical delivery next day", 1),
    (2, "INTRADAY", "Intraday", "Same-day physical delivery", 0),
    (3, "FORWARD_WK", "Forward (Weekly)", "Physical forward, weekly granularity", 7),
    (4, "FORWARD_MO", "Forward (Monthly)", "Physical forward, monthly granularity", 30),
    (5, "FORWARD_QT", "Forward (Quarterly)", "Physical forward, quarterly granularity", 90),
    (6, "FORWARD_YR", "Forward (Yearly)", "Physical forward, yearly granularity", 365),
    (7, "FUTURES", "Futures", "Exchange-traded financial contract", None),
    (8, "OPTIONS", "Options", "Right to buy/sell at strike price", None),
    (9, "SWAP", "Swap", "OTC financial swap", None),
    (10, "SPREAD", "Spread", "Cross-commodity or locational spread", None),
]

markets = [
    Row(
        market_key=mk,
        market_code=code,
        market_name=name,
        description=desc,
        typical_tenor_days=tenor,
        is_physical=tenor is not None,
        is_otc=code in ("SWAP", "SPREAD", "OPTIONS"),
    )
    for mk, code, name, desc, tenor in market_data
]

df_market = spark.createDataFrame(markets)
df_market.write.mode("overwrite").saveAsTable("dim_market")
print(f"dim_market: {df_market.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 · Fact tables
# MAGIC
# MAGIC `fact_trade` is the heart of the model — one row per trade.
# MAGIC `fact_market_price` records the hourly market ("going rate") price at each hub.
# MAGIC `fact_position` rolls trades up to a daily net position.

# COMMAND ----------

# DBTITLE 1,fact_trade
from pyspark.sql import functions as F
from pyspark.sql.types import *

num_traders = 122  # 120 base + King Kong + B.Rock Van Guard
num_base_traders = 120  # FIX F1: base trades draw only 1..120 so King Kong (121)
                        # and B.Rock (122) keep ONLY their scripted trades
num_counterparties = len(counterparty_data)
num_instruments = len(instrument_data)
num_delivery_points = len(delivery_data)
num_markets = len(market_data)

# Base prices calibrated to Yahoo Finance daily closes across the demo's
# window (2024-01-01..2026-05-21). For every series with a Yahoo ticker, the
# base sits BELOW the observed mean so the seasonal × Iran-spike multipliers
# (see below) layer up to a modeled average that matches the real index.
# Average multiplier impact across the period is ~1.16 (seasonal 1.063 ×
# Iran-spike 1.094) — i.e. base × 1.16 ≈ Yahoo mean.
#
# Yahoo sources (pulled 2026-05-17):
#   BZ=F  Brent     mean $76.05  stdev $11.4   → keys 15
#   CL=F  WTI       mean $72.02  stdev $10.7   → key 16
#   TTF=F TTF gas   mean €36.48  stdev €7.5    → keys 9-12
#   JKM=F JKM LNG   mean $12.49/MMBtu (=$650/MT) stdev $2.39  → keys 21-22
#   KEUA  EUA ETF   stdev/mean 9.6% (level via market reports ~€72/tCO2)
#   NG=F  Henry Hub — NOT applied; demo gas curve is European TTF, not US.
#
# Series without a public Yahoo ticker (NBP, European power, GOs, LNG-TTF,
# LNG-DES-NWE) use published market averages for 2024-2026.
#
# Iran-Israel conflict spike windows layered on top (see _iran_spike):
#   Apr 2024  — Iran missile attack on Israel
#   Oct 2024  — Iran barrage + Israeli retaliation
#   Jun-Sep 2025 — sustained tension / Strait of Hormuz risk
#   Jan-Mar 2026 — ongoing escalation
price_profiles = {
    1: (95.0, 35.0),    # PWR-BASE-DA  — European baseload ~€85-110/MWh (no Yahoo source)
    2: (125.0, 45.0),   # PWR-PEAK-DA  — peak premium 25-35%
    3: (92.0, 28.0),    # PWR-BASE-WK
    4: (88.0, 22.0),    # PWR-BASE-MO
    5: (84.0, 18.0),    # PWR-BASE-QT
    6: (80.0, 15.0),    # PWR-BASE-YR
    7: (118.0, 32.0),   # PWR-PEAK-WK
    8: (112.0, 26.0),   # PWR-PEAK-MO
    9: (32.0, 6.0),     # GAS-TTF-DA   — Yahoo TTF=F (mean €36.48, stdev €7.5)
    10: (30.0, 5.0),    # GAS-TTF-MO
    11: (28.0, 4.0),    # GAS-TTF-QT
    12: (26.0, 4.0),    # GAS-TTF-YR
    13: (95.0, 14.0),   # GAS-NBP-DA   — published NBP ~80-110 p/therm
    14: (90.0, 12.0),   # GAS-NBP-MO
    15: (65.0, 11.0),   # OIL-BRENT    — Yahoo BZ=F (mean $76.05, stdev $11.4)
    16: (62.0, 10.0),   # OIL-WTI      — Yahoo CL=F (mean $72.02, stdev $10.7)
    17: (62.0, 8.0),    # CARBON-EUA-SP — published EUA ~€60-85/tCO2, vol from KEUA ETF
    18: (64.0, 8.0),    # CARBON-EUA-FU
    19: (2.8, 1.3),     # GO-NORDIC    — niche European market (no Yahoo)
    20: (3.8, 1.6),     # GO-EU
    21: (560.0, 80.0),  # LNG-JKM-SP   — Yahoo JKM=F (mean $12.49/MMBtu × 52 = $650/MT, stdev $124/MT)
    22: (550.0, 70.0),  # LNG-JKM-FU
    23: (475.0, 70.0),  # LNG-TTF-SP   — European LNG, JKM minus ~€30-50/MT basis
    24: (485.0, 65.0),  # LNG-DES-NWE
}

price_base_expr = "CASE instrument_key " + " ".join(
    f"WHEN {k} THEN {v[0]}" for k, v in price_profiles.items()
) + " ELSE 40.0 END"

price_vol_expr = "CASE instrument_key " + " ".join(
    f"WHEN {k} THEN {v[1]}" for k, v in price_profiles.items()
) + " ELSE 10.0 END"

# Read trader dimension to drive direction bias, volume scaling, and variation
df_trader_lookup = spark.table("dim_trader").select("trader_key", "company_type", "company", "seniority")

df_trades = (
    spark.range(0, NUM_TRADES)
    .withColumn("trade_id", F.concat(F.lit("T-"), F.lpad(F.col("id").cast("string"), 8, "0")))
    # Random trade date within range
    .withColumn("_trade_date",
        F.date_add(F.lit(DATE_START), (F.rand(seed=42) * F.datediff(F.lit(DATE_END), F.lit(DATE_START))).cast("int"))
    )
    # Random hour within business hours (07–17)
    .withColumn("_trade_hour", F.lit(7) + (F.rand(seed=101) * 11).cast("int"))
    # time_key = yyyyMMddHH
    .withColumn("time_key",
        F.concat(
            F.date_format("_trade_date", "yyyyMMdd"),
            F.lpad(F.col("_trade_hour").cast("string"), 2, "0")
        )
    )
    # Full timestamp for convenience
    .withColumn("trade_timestamp",
        F.to_timestamp(
            F.concat(
                F.col("_trade_date").cast("string"),
                F.lit(" "),
                F.lpad(F.col("_trade_hour").cast("string"), 2, "0"),
                F.lit(":"),
                F.lpad((F.rand(seed=202) * 60).cast("int").cast("string"), 2, "0"),
                F.lit(":"),
                F.lpad((F.rand(seed=303) * 60).cast("int").cast("string"), 2, "0"),
            )
        )
    )
    # Dimension keys
    .withColumn("trader_key", ((F.rand(seed=1) * num_base_traders).cast("int") + 1).cast("long"))  # FIX F1: exclude scripted traders 121/122
    .withColumn("counterparty_key", (F.rand(seed=2) * num_counterparties).cast("int") + 1)
    .withColumn("instrument_key", (F.rand(seed=3) * num_instruments).cast("int") + 1)
    .withColumn("delivery_point_key",
        # FIX F7: pick a delivery point whose commodity_class matches the instrument's
        # commodity, instead of drawing independently.
        #   instrument_key ranges -> delivery_point_key ranges
        #   1-8   Power        -> 1-18  (power zones)
        #   9-14  Natural Gas  -> 19-22 (gas hubs)
        #   15-16 Crude Oil    -> 23-24 (oil venues)
        #   17-18 Carbon       -> 25-26 (carbon venues)
        #   19-20 GO           -> 1-18  (delivered on power zones)
        #   21-24 LNG          -> 27-32 (LNG terminals)
        F.when(F.col("instrument_key") <= 8,  (F.rand(seed=4)  * 18).cast("int") + 1)
         .when(F.col("instrument_key") <= 14, (F.rand(seed=41) * 4).cast("int") + 19)
         .when(F.col("instrument_key") <= 16, (F.rand(seed=42) * 2).cast("int") + 23)
         .when(F.col("instrument_key") <= 18, (F.rand(seed=43) * 2).cast("int") + 25)
         .when(F.col("instrument_key") <= 20, (F.rand(seed=44) * 18).cast("int") + 1)
         .otherwise((F.rand(seed=45) * 6).cast("int") + 27)
    )
    .withColumn("market_key", (F.rand(seed=5) * num_markets).cast("int") + 1)
    # Join trader to get company_type, seniority, company for variation
    .join(F.broadcast(df_trader_lookup), "trader_key", "left")
    # ── Direction driven by company type ──
    #   Producer (Equinor, Statkraft): ~80% SELL
    #   Utility (Vattenfall, E.ON, EDF, etc.): ~70% BUY
    #   Major (Shell, BP): ~55% BUY
    #   Trader (Danske Commodities): 50/50
    .withColumn("_sell_threshold",
        F.when(F.col("company_type") == "Producer", 0.20)
         .when(F.col("company_type") == "Utility", 0.70)
         .when(F.col("company_type") == "Major", 0.55)
         .otherwise(0.50)
    )
    .withColumn("direction",
        F.when(F.rand(seed=7) < F.col("_sell_threshold"), "BUY").otherwise("SELL")
    )
    # ── Trader variation: seniority drives trade size ──
    #   Head of Desk: 2.5–4x volume (large block trades)
    #   Senior Trader: 1.5–2.5x
    #   Trader: 0.7–1.3x (baseline)
    #   Junior Trader: 0.3–0.7x (small clip sizes)
    .withColumn("_seniority_mult",
        F.when(F.col("seniority") == "Head of Desk", F.rand(seed=50) * 1.5 + 2.5)
         .when(F.col("seniority") == "Senior Trader", F.rand(seed=50) * 1.0 + 1.5)
         .when(F.col("seniority") == "Junior Trader", F.rand(seed=50) * 0.4 + 0.3)
         .otherwise(F.rand(seed=50) * 0.6 + 0.7)  # Trader
    )
    # ── Company size multiplier — larger firms trade bigger clips ──
    .withColumn("_company_mult",
        F.when(F.col("company").isin("Equinor ASA", "Shell Energy", "BP Trading", "Kong Corporation"), 1.8)
         .when(F.col("company").isin("EDF SA", "E.ON SE", "Vattenfall AB"), 1.3)
         .when(F.col("company").isin("Danske Commodities", "Statkraft AS"), 1.1)
         .otherwise(0.7)  # smaller utilities
    )
    # ── Seasonal volume multiplier for power & gas ──
    # European winter (Oct–Mar): heating demand drives 40-80% higher volumes
    # Summer (Jun–Aug): demand drops 25-40%
    # LNG also seasonal: winter premium as Asia/Europe compete for cargoes
    .withColumn("_seasonal_vol_mult",
        F.when(
            F.col("instrument_key").between(1, 14) | F.col("instrument_key").between(21, 24),  # Power, Gas, LNG
            F.when(F.month("_trade_date").isin(12, 1, 2), 1.8)          # Deep winter: +80%
             .when(F.month("_trade_date").isin(11, 3), 1.4)              # Shoulder winter: +40%
             .when(F.month("_trade_date").isin(10, 4), 1.15)             # Autumn/spring
             .when(F.month("_trade_date").isin(6, 7, 8), 0.6)            # Summer lull: -40%
             .otherwise(1.0)
        ).otherwise(1.0)  # Oil, carbon, GOs — less seasonal
    )
    # ── Base volumes by commodity ──
    .withColumn("_raw_volume",
        F.when(F.col("instrument_key").isin(15, 16), F.rand(seed=8) * 90000 + 10000)        # Oil: 10k-100k bbl
         .when(F.col("instrument_key").isin(17, 18), F.rand(seed=8) * 49000 + 1000)          # Carbon: 1k-50k tCO2
         .when(F.col("instrument_key").isin(19, 20), F.rand(seed=8) * 24500 + 500)           # GOs: 500-25k MWh
         .when(F.col("instrument_key").between(21, 24), F.rand(seed=8) * 65000 + 5000)       # LNG: 5k-70k MT
         .when(F.col("instrument_key").between(9, 14), F.rand(seed=8) * 95000 + 5000)        # Gas: 5k-100k MWh
         .otherwise(F.rand(seed=8) * 475 + 25)                                                # Power: 25-500 MW
    )
    # Final volume = base × seniority × company × seasonal
    .withColumn("volume", F.round(F.col("_raw_volume") * F.col("_seniority_mult") * F.col("_company_mult") * F.col("_seasonal_vol_mult"), 2))
    # ── Price with seasonal + Iran-conflict event spike + random noise ──
    .withColumn("_base_price", F.expr(price_base_expr))
    .withColumn("_volatility", F.expr(price_vol_expr))
    .withColumn("_seasonal_price_factor",
        F.when(F.month("_trade_date").isin(12, 1, 2), 1.35)     # Deep winter: prices spike
         .when(F.month("_trade_date").isin(11, 3), 1.15)         # Shoulder winter
         .when(F.month("_trade_date").isin(6, 7, 8), 0.80)       # Summer dip
         .otherwise(1.0)
    )
    # Event-driven Iran-Israel conflict price spikes
    #   Gas/Oil/LNG (keys 9-16, 21-24): full impact, +18-28%
    #   Power (keys 1-8): smaller follow-through, +8-15%
    #   Carbon/GOs: minimal
    .withColumn("_iran_spike",
        F.when(F.col("instrument_key").between(9, 16) | F.col("instrument_key").between(21, 24),
            F.when((F.col("_trade_date") >= F.lit("2024-04-10")) & (F.col("_trade_date") <= F.lit("2024-05-15")), 1.18)
             .when((F.col("_trade_date") >= F.lit("2024-10-01")) & (F.col("_trade_date") <= F.lit("2024-11-20")), 1.22)
             .when((F.col("_trade_date") >= F.lit("2025-06-15")) & (F.col("_trade_date") <= F.lit("2025-09-30")), 1.28)
             .when((F.col("_trade_date") >= F.lit("2026-01-10")) & (F.col("_trade_date") <= F.lit("2026-03-05")), 1.20)
             .otherwise(1.0))
         .when(F.col("instrument_key").between(1, 8),
            F.when((F.col("_trade_date") >= F.lit("2024-10-01")) & (F.col("_trade_date") <= F.lit("2024-11-20")), 1.10)
             .when((F.col("_trade_date") >= F.lit("2025-06-15")) & (F.col("_trade_date") <= F.lit("2025-09-30")), 1.15)
             .when((F.col("_trade_date") >= F.lit("2026-01-10")) & (F.col("_trade_date") <= F.lit("2026-03-05")), 1.08)
             .otherwise(1.0))
         .otherwise(1.0)
    )
    .withColumn("price",
        F.round(F.col("_base_price") * F.col("_seasonal_price_factor") * F.col("_iran_spike") + (F.randn(seed=9) * F.col("_volatility")), 2)
    )
    .withColumn("price", F.greatest(F.col("price"), F.lit(0.01)))
    .withColumn("notional_value", F.round(F.col("price") * F.col("volume"), 2))
    # Delivery period
    .withColumn("delivery_start", F.date_add("_trade_date", (F.rand(seed=10) * 30).cast("int") + 1))
    .withColumn("delivery_end", F.date_add("delivery_start",
        F.when(F.col("market_key").isin(1, 2), 1)
         .when(F.col("market_key") == 3, 7)
         .when(F.col("market_key") == 4, 30)
         .when(F.col("market_key") == 5, 90)
         .when(F.col("market_key") == 6, 365)
         .otherwise(30)
    ))
    # Trade status
    .withColumn("status",
        F.when(F.rand(seed=11) < 0.85, "CONFIRMED")
         .when(F.rand(seed=11) < 0.93, "SETTLED")
         .when(F.rand(seed=11) < 0.97, "PENDING")
         .otherwise("CANCELLED")
    )
    .drop("id", "_trade_date", "_trade_hour", "_base_price", "_volatility",
          "_seasonal_price_factor", "_iran_spike", "_seasonal_vol_mult", "_raw_volume",
          "_seniority_mult", "_company_mult", "company_type", "company",
          "seniority", "_sell_threshold")
)

# --- Equinor LNG guarantee: ≥25M MT/year (~56M MT over 2.25yr date range) ---
# Equinor is a top-3 global LNG exporter (~25-30M MT/year from Hammerfest, Melkøya)
# 900 trades × avg ~65k MT = ~58.5M MT total = ~26M MT/year
EQUINOR_LNG_TRADES = 900
equinor_lng_trader_keys = list(range(1, 7))  # The 6 LNG desk traders
lng_instrument_keys = [21, 22, 23, 24]       # LNG instruments
lng_dp_keys = [27, 28, 29, 30]               # LNG delivery points

df_equinor_lng = (
    spark.range(0, EQUINOR_LNG_TRADES)
    .withColumn("trade_id", F.concat(F.lit("EQ-LNG-"), F.lpad(F.col("id").cast("string"), 6, "0")))
    .withColumn("_trade_date",
        F.date_add(F.lit(DATE_START), (F.rand(seed=500) * F.datediff(F.lit(DATE_END), F.lit(DATE_START))).cast("int"))
    )
    .withColumn("_trade_hour", F.lit(7) + (F.rand(seed=501) * 11).cast("int"))
    .withColumn("time_key",
        F.concat(F.date_format("_trade_date", "yyyyMMdd"), F.lpad(F.col("_trade_hour").cast("string"), 2, "0"))
    )
    .withColumn("trade_timestamp",
        F.to_timestamp(F.concat(
            F.col("_trade_date").cast("string"), F.lit(" "),
            F.lpad(F.col("_trade_hour").cast("string"), 2, "0"), F.lit(":"),
            F.lpad((F.rand(seed=502) * 60).cast("int").cast("string"), 2, "0"), F.lit(":"),
            F.lpad((F.rand(seed=503) * 60).cast("int").cast("string"), 2, "0"),
        ))
    )
    # Equinor LNG traders only
    .withColumn("trader_key", F.element_at(F.array([F.lit(k).cast("long") for k in equinor_lng_trader_keys]), (F.rand(seed=504) * len(equinor_lng_trader_keys)).cast("int") + 1))
    .withColumn("counterparty_key", (F.rand(seed=505) * num_counterparties).cast("int") + 1)
    # LNG instruments only
    .withColumn("instrument_key", F.element_at(F.array([F.lit(k) for k in lng_instrument_keys]), (F.rand(seed=506) * len(lng_instrument_keys)).cast("int") + 1))
    # LNG delivery points only — weighted toward Snøhvit (Equinor's own terminal)
    .withColumn("delivery_point_key",
        F.when(F.rand(seed=507) < 0.45, F.lit(29))    # Snøhvit: 45% — Equinor's home terminal
         .when(F.rand(seed=507) < 0.70, F.lit(27))     # Gate Rotterdam: 25%
         .when(F.rand(seed=507) < 0.88, F.lit(30))     # Dunkerque: 18%
         .otherwise(F.lit(28))                           # Dragon UK: 12%
    )
    .withColumn("market_key", F.element_at(F.array(F.lit(1), F.lit(4), F.lit(5), F.lit(7)), (F.rand(seed=508) * 4).cast("int") + 1))
    # Equinor as producer: ~85% SELL
    .withColumn("direction", F.when(F.rand(seed=509) < 0.15, "BUY").otherwise("SELL"))
    # Large LNG cargoes: 45,000–85,000 MT (standard to Q-Max size)
    # Seasonal volume: winter cargoes are larger (urgent demand, full tankers)
    .withColumn("_seasonal_vol",
        F.when(F.month("_trade_date").isin(12, 1, 2), 1.3)       # Deep winter: max cargoes
         .when(F.month("_trade_date").isin(11, 3), 1.15)
         .when(F.month("_trade_date").isin(6, 7, 8), 0.85)        # Summer: smaller parcels
         .otherwise(1.0)
    )
    .withColumn("volume", F.round((F.rand(seed=510) * 40000 + 45000) * F.col("_seasonal_vol"), 2))
    # LNG prices ~$450-650/MT with strong seasonal variation + Iran-conflict spike
    .withColumn("_seasonal_price",
        F.when(F.month("_trade_date").isin(12, 1, 2), 1.40)       # Winter premium: +40%
         .when(F.month("_trade_date").isin(11, 3), 1.20)
         .when(F.month("_trade_date").isin(6, 7, 8), 0.80)         # Summer discount
         .otherwise(1.0)
    )
    .withColumn("_iran_spike",
        F.when((F.col("_trade_date") >= F.lit("2024-04-10")) & (F.col("_trade_date") <= F.lit("2024-05-15")), 1.18)
         .when((F.col("_trade_date") >= F.lit("2024-10-01")) & (F.col("_trade_date") <= F.lit("2024-11-20")), 1.22)
         .when((F.col("_trade_date") >= F.lit("2025-06-15")) & (F.col("_trade_date") <= F.lit("2025-09-30")), 1.28)
         .when((F.col("_trade_date") >= F.lit("2026-01-10")) & (F.col("_trade_date") <= F.lit("2026-03-05")), 1.20)
         .otherwise(1.0)
    )
    .withColumn("price", F.round(F.lit(520.0) * F.col("_seasonal_price") * F.col("_iran_spike") + F.randn(seed=511) * 60.0, 2))
    .withColumn("price", F.greatest(F.col("price"), F.lit(200.0)))
    .withColumn("notional_value", F.round(F.col("price") * F.col("volume"), 2))
    .withColumn("delivery_start", F.date_add("_trade_date", (F.rand(seed=512) * 45).cast("int") + 7))
    .withColumn("delivery_end", F.date_add("delivery_start", 30))
    .withColumn("status",
        F.when(F.rand(seed=513) < 0.88, "CONFIRMED")
         .when(F.rand(seed=513) < 0.95, "SETTLED")
         .otherwise("PENDING")
    )
    .drop("id", "_trade_date", "_trade_hour", "_seasonal_vol", "_seasonal_price", "_iran_spike")
)

# --- Kong Corporation LNG: King Kong trades similar to other LNG traders but 25% larger ---
# Equinor LNG: 900 trades / 6 traders = ~150/trader, avg ~65k MT
# King Kong: ~200 trades, avg ~81k MT (25% larger cargoes)
KONG_LNG_TRADES = 200

df_kong_lng = (
    spark.range(0, KONG_LNG_TRADES)
    .withColumn("trade_id", F.concat(F.lit("KONG-"), F.lpad(F.col("id").cast("string"), 6, "0")))
    .withColumn("_trade_date",
        F.date_add(F.lit(DATE_START), (F.rand(seed=600) * F.datediff(F.lit(DATE_END), F.lit(DATE_START))).cast("int"))
    )
    .withColumn("_trade_hour", F.lit(7) + (F.rand(seed=601) * 11).cast("int"))
    .withColumn("time_key",
        F.concat(F.date_format("_trade_date", "yyyyMMdd"), F.lpad(F.col("_trade_hour").cast("string"), 2, "0"))
    )
    .withColumn("trade_timestamp",
        F.to_timestamp(F.concat(
            F.col("_trade_date").cast("string"), F.lit(" "),
            F.lpad(F.col("_trade_hour").cast("string"), 2, "0"), F.lit(":"),
            F.lpad((F.rand(seed=602) * 60).cast("int").cast("string"), 2, "0"), F.lit(":"),
            F.lpad((F.rand(seed=603) * 60).cast("int").cast("string"), 2, "0"),
        ))
    )
    # King Kong is the sole trader
    .withColumn("trader_key", F.lit(KONG_TRADER_KEY).cast("long"))
    .withColumn("counterparty_key", (F.rand(seed=604) * num_counterparties).cast("int") + 1)
    # LNG instruments only
    .withColumn("instrument_key", F.element_at(F.array([F.lit(k) for k in lng_instrument_keys]), (F.rand(seed=605) * len(lng_instrument_keys)).cast("int") + 1))
    # Kong LNG Terminal (delivery_point_key = 31) as primary, others as secondary
    .withColumn("delivery_point_key",
        F.when(F.rand(seed=606) < 0.60, F.lit(31))     # Kong Terminal: 60%
         .when(F.rand(seed=606) < 0.80, F.lit(27))      # Gate Rotterdam: 20%
         .when(F.rand(seed=606) < 0.92, F.lit(29))      # Snøhvit: 12%
         .otherwise(F.lit(28))                            # Dragon UK: 8%
    )
    .withColumn("market_key", F.element_at(F.array(F.lit(1), F.lit(4), F.lit(5), F.lit(7)), (F.rand(seed=607) * 4).cast("int") + 1))
    # Kong Corporation is a producer: 100% SELL
    .withColumn("direction", F.lit("SELL"))
    # Massive volumes: 8M–15M MT per trade (avg ~11.25M MT)
    .withColumn("_seasonal_vol",
        F.when(F.month("_trade_date").isin(12, 1, 2), 1.25)
         .when(F.month("_trade_date").isin(11, 3), 1.10)
         .when(F.month("_trade_date").isin(6, 7, 8), 0.80)
         .otherwise(1.0)
    )
    # 25% larger than Equinor LNG cargoes (Equinor: 45k-85k MT → Kong: 56k-106k MT)
    .withColumn("volume", F.round((F.rand(seed=608) * 50000 + 56000) * F.col("_seasonal_vol"), 2))
    # LNG prices + Iran-conflict event spike
    .withColumn("_seasonal_price",
        F.when(F.month("_trade_date").isin(12, 1, 2), 1.40)
         .when(F.month("_trade_date").isin(11, 3), 1.20)
         .when(F.month("_trade_date").isin(6, 7, 8), 0.80)
         .otherwise(1.0)
    )
    .withColumn("_iran_spike",
        F.when((F.col("_trade_date") >= F.lit("2024-04-10")) & (F.col("_trade_date") <= F.lit("2024-05-15")), 1.18)
         .when((F.col("_trade_date") >= F.lit("2024-10-01")) & (F.col("_trade_date") <= F.lit("2024-11-20")), 1.22)
         .when((F.col("_trade_date") >= F.lit("2025-06-15")) & (F.col("_trade_date") <= F.lit("2025-09-30")), 1.28)
         .when((F.col("_trade_date") >= F.lit("2026-01-10")) & (F.col("_trade_date") <= F.lit("2026-03-05")), 1.20)
         .otherwise(1.0)
    )
    .withColumn("price", F.round(F.lit(520.0) * F.col("_seasonal_price") * F.col("_iran_spike") + F.randn(seed=609) * 60.0, 2))
    .withColumn("price", F.greatest(F.col("price"), F.lit(200.0)))
    .withColumn("notional_value", F.round(F.col("price") * F.col("volume"), 2))
    .withColumn("delivery_start", F.date_add("_trade_date", (F.rand(seed=610) * 45).cast("int") + 7))
    .withColumn("delivery_end", F.date_add("delivery_start", 30))
    .withColumn("status",
        F.when(F.rand(seed=611) < 0.90, "CONFIRMED")
         .when(F.rand(seed=611) < 0.97, "SETTLED")
         .otherwise("PENDING")
    )
    .drop("id", "_trade_date", "_trade_hour", "_seasonal_vol", "_seasonal_price", "_iran_spike")
)

# --- Manta Resources: B.Rock Van Guard dumps Power, Gas and LNG far below market ---
# APAC multi-commodity trader with grossly oversized clip sizes (~10x typical) and
# sells 35-50% below the prevailing curve — and dumps even harder during the
# Iran-Israel conflict spikes when other desks are scrambling to buy. A textbook
# predatory "panic-the-market" pattern: he sells volume when prices are high,
# depressing the curve and making other longs bleed.
MANTA_TRADES = 800
SLNG_DP_KEY = len(delivery_data)  # Singapore LNG terminal — added last in delivery_data

df_manta = (
    spark.range(0, MANTA_TRADES)
    .withColumn("trade_id", F.concat(F.lit("MANTA-"), F.lpad(F.col("id").cast("string"), 6, "0")))
    # Date sampling: 50% uniform, 50% biased into Iran-conflict spike windows
    # so his footprint is heavily over-represented when others are panic-buying
    .withColumn("_date_class", F.rand(seed=720))
    .withColumn("_uniform_date",
        F.date_add(F.lit(DATE_START), (F.rand(seed=700) * F.datediff(F.lit(DATE_END), F.lit(DATE_START))).cast("int"))
    )
    # Pick one of the four spike windows uniformly, then a date within it
    .withColumn("_spike_window_pick", (F.rand(seed=721) * 4).cast("int"))
    .withColumn("_spike_date",
        F.when(F.col("_spike_window_pick") == 0,  # Apr 2024 — 36 day window
            F.date_add(F.lit("2024-04-10"), (F.rand(seed=722) * 36).cast("int")))
         .when(F.col("_spike_window_pick") == 1,  # Oct 2024 — 51 day window
            F.date_add(F.lit("2024-10-01"), (F.rand(seed=722) * 51).cast("int")))
         .when(F.col("_spike_window_pick") == 2,  # Jun-Sep 2025 — 108 day window
            F.date_add(F.lit("2025-06-15"), (F.rand(seed=722) * 108).cast("int")))
         .otherwise(                              # Jan-Mar 2026 — 55 day window
            F.date_add(F.lit("2026-01-10"), (F.rand(seed=722) * 55).cast("int")))
    )
    .withColumn("_trade_date",
        F.when(F.col("_date_class") < 0.50, F.col("_spike_date"))
         .otherwise(F.col("_uniform_date"))
    )
    .withColumn("_trade_hour", F.lit(7) + (F.rand(seed=701) * 11).cast("int"))
    .withColumn("time_key",
        F.concat(F.date_format("_trade_date", "yyyyMMdd"), F.lpad(F.col("_trade_hour").cast("string"), 2, "0"))
    )
    .withColumn("trade_timestamp",
        F.to_timestamp(F.concat(
            F.col("_trade_date").cast("string"), F.lit(" "),
            F.lpad(F.col("_trade_hour").cast("string"), 2, "0"), F.lit(":"),
            F.lpad((F.rand(seed=702) * 60).cast("int").cast("string"), 2, "0"), F.lit(":"),
            F.lpad((F.rand(seed=703) * 60).cast("int").cast("string"), 2, "0"),
        ))
    )
    .withColumn("trader_key", F.lit(MANTA_TRADER_KEY).cast("long"))
    .withColumn("counterparty_key", (F.rand(seed=704) * num_counterparties).cast("int") + 1)
    # Instrument mix: 30% Power (1-8), 30% Gas (9-14), 40% LNG (21-24)
    .withColumn("_instr_class", F.rand(seed=705))
    .withColumn("instrument_key",
        F.when(F.col("_instr_class") < 0.30, (F.rand(seed=706) * 8).cast("int") + 1)
         .when(F.col("_instr_class") < 0.60, (F.rand(seed=707) * 6).cast("int") + 9)
         .otherwise((F.rand(seed=708) * 4).cast("int") + 21)
    )
    # Delivery points: SLNG-SG primary, plus European hubs for cross-region action
    .withColumn("delivery_point_key",
        F.when(F.rand(seed=709) < 0.50, F.lit(SLNG_DP_KEY))   # Singapore SLNG: 50%
         .when(F.rand(seed=709) < 0.70, F.lit(19))             # TTF: 20%
         .when(F.rand(seed=709) < 0.85, F.lit(20))             # NBP: 15%
         .when(F.rand(seed=709) < 0.95, F.lit(27))             # Gate Rotterdam: 10%
         .otherwise(F.lit(28))                                  # Dragon UK: 5%
    )
    .withColumn("market_key",
        F.element_at(F.array(F.lit(1), F.lit(4), F.lit(5), F.lit(7)),
                     (F.rand(seed=710) * 4).cast("int") + 1)
    )
    # 98% SELL — B.Rock dumps, almost never lifts
    .withColumn("direction", F.when(F.rand(seed=711) < 0.02, "BUY").otherwise("SELL"))
    # Detect if this trade landed in a conflict spike window — drives both
    # the dump intensity and the volume aggression below
    .withColumn("_in_spike",
        ((F.col("_trade_date") >= F.lit("2024-04-10")) & (F.col("_trade_date") <= F.lit("2024-05-15"))) |
        ((F.col("_trade_date") >= F.lit("2024-10-01")) & (F.col("_trade_date") <= F.lit("2024-11-20"))) |
        ((F.col("_trade_date") >= F.lit("2025-06-15")) & (F.col("_trade_date") <= F.lit("2025-09-30"))) |
        ((F.col("_trade_date") >= F.lit("2026-01-10")) & (F.col("_trade_date") <= F.lit("2026-03-05")))
    )
    # Grossly oversized clip sizes — ~10x standard baseline, +30% during spikes
    .withColumn("_spike_vol_mult", F.when(F.col("_in_spike"), F.lit(1.30)).otherwise(F.lit(1.0)))
    .withColumn("volume",
        F.round(
            (F.when(F.col("instrument_key").between(1, 8), F.rand(seed=712) * 4750 + 250)         # Power: 250-5000 MW
              .when(F.col("instrument_key").between(9, 14), F.rand(seed=712) * 950000 + 50000)   # Gas:   50k-1M MWh
              .otherwise(F.rand(seed=712) * 650000 + 50000))                                      # LNG:   50k-700k MT
            * F.col("_spike_vol_mult"),
            2)
    )
    # Price: market base × season × Iran-spike × dump_factor (so the dump is
    # measured against the *actual* prevailing market, not a static base).
    # Outside spikes: dump_factor 0.50-0.65 (35-50% below market)
    # During spikes: dump_factor 0.40-0.52 (48-60% below market) — predatory
    .withColumn("_market_base", F.expr(price_base_expr))
    .withColumn("_seasonal_factor",
        F.when(F.month("_trade_date").isin(12, 1, 2), 1.35)
         .when(F.month("_trade_date").isin(11, 3), 1.15)
         .when(F.month("_trade_date").isin(6, 7, 8), 0.80)
         .otherwise(1.0)
    )
    .withColumn("_iran_spike",
        F.when(F.col("instrument_key").between(9, 16) | F.col("instrument_key").between(21, 24),
            F.when((F.col("_trade_date") >= F.lit("2024-04-10")) & (F.col("_trade_date") <= F.lit("2024-05-15")), 1.18)
             .when((F.col("_trade_date") >= F.lit("2024-10-01")) & (F.col("_trade_date") <= F.lit("2024-11-20")), 1.22)
             .when((F.col("_trade_date") >= F.lit("2025-06-15")) & (F.col("_trade_date") <= F.lit("2025-09-30")), 1.28)
             .when((F.col("_trade_date") >= F.lit("2026-01-10")) & (F.col("_trade_date") <= F.lit("2026-03-05")), 1.20)
             .otherwise(1.0))
         .when(F.col("instrument_key").between(1, 8),
            F.when((F.col("_trade_date") >= F.lit("2024-10-01")) & (F.col("_trade_date") <= F.lit("2024-11-20")), 1.10)
             .when((F.col("_trade_date") >= F.lit("2025-06-15")) & (F.col("_trade_date") <= F.lit("2025-09-30")), 1.15)
             .when((F.col("_trade_date") >= F.lit("2026-01-10")) & (F.col("_trade_date") <= F.lit("2026-03-05")), 1.08)
             .otherwise(1.0))
         .otherwise(1.0)
    )
    .withColumn("_dump_factor",
        F.when(F.col("_in_spike"), F.rand(seed=713) * 0.12 + 0.40)   # 0.40-0.52 (48-60% below)
         .otherwise(F.rand(seed=713) * 0.15 + 0.50)                    # 0.50-0.65 (35-50% below)
    )
    .withColumn("price",
        F.round(
            F.col("_market_base") * F.col("_seasonal_factor") * F.col("_iran_spike") * F.col("_dump_factor")
            + F.randn(seed=714) * 4.0,
            2)
    )
    .withColumn("price", F.greatest(F.col("price"), F.lit(0.01)))
    .withColumn("notional_value", F.round(F.col("price") * F.col("volume"), 2))
    .withColumn("delivery_start", F.date_add("_trade_date", (F.rand(seed=715) * 30).cast("int") + 1))
    .withColumn("delivery_end", F.date_add("delivery_start",
        F.when(F.col("market_key").isin(1, 2), 1)
         .when(F.col("market_key") == 3, 7)
         .when(F.col("market_key") == 4, 30)
         .when(F.col("market_key") == 5, 90)
         .when(F.col("market_key") == 6, 365)
         .otherwise(30)
    ))
    .withColumn("status",
        F.when(F.rand(seed=716) < 0.85, "CONFIRMED")
         .when(F.rand(seed=716) < 0.95, "SETTLED")
         .otherwise("PENDING")
    )
    .drop("id", "_date_class", "_uniform_date", "_spike_window_pick", "_spike_date",
          "_trade_date", "_trade_hour", "_instr_class", "_in_spike", "_spike_vol_mult",
          "_market_base", "_seasonal_factor", "_iran_spike", "_dump_factor")
)

# Union all trade blocks
df_trades = df_trades.unionByName(df_equinor_lng).unionByName(df_kong_lng).unionByName(df_manta)

df_trades.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("fact_trade")
print(f"fact_trade: {df_trades.count():,} rows (incl. {EQUINOR_LNG_TRADES:,} Equinor LNG + {KONG_LNG_TRADES:,} Kong LNG + {MANTA_TRADES:,} Manta dump trades)")

# COMMAND ----------

# DBTITLE 1,fact_market_price — hourly prices per delivery point, keyed by time_key
df_time_slim = spark.table("dim_time").select("time_key", "cal_date", "hour_of_day", "month", "is_peak")
df_dps = spark.table("dim_delivery_point").select("delivery_point_key", "zone_code", "commodity_class")

df_price_grid = df_time_slim.crossJoin(df_dps)

df_market_prices = (
    df_price_grid
    # Base prices calibrated to Yahoo Finance (Brent BZ=F, TTF=F, JKM=F)
    # and published market averages where Yahoo doesn't carry the series.
    .withColumn("_base",
        F.when(F.col("commodity_class") == "Power", 95.0)    # no Yahoo ticker — market estimate
         .when(F.col("commodity_class") == "Gas", 32.0)      # Yahoo TTF=F mean €36 / 1.16 mult
         .when(F.col("commodity_class") == "Oil", 65.0)      # Yahoo BZ=F mean $76 / 1.16 mult
         .when(F.col("commodity_class") == "Carbon", 62.0)   # published EUA mean €72 / 1.16 mult
         .when(F.col("commodity_class") == "LNG", 560.0)     # Yahoo JKM=F mean $650/MT / 1.16 mult
         .otherwise(40.0)
    )
    # Commodity-specific seasonal price factors
    .withColumn("_season",
        F.when(F.col("commodity_class").isin("Power", "Gas", "LNG"),
            F.when(F.col("month").isin(12, 1, 2), 1.45)       # Deep winter: power/gas spike
             .when(F.col("month").isin(11, 3), 1.20)
             .when(F.col("month").isin(6, 7, 8), 0.70)         # Summer: demand drops
             .otherwise(1.0)
        ).otherwise(
            F.when(F.col("month").isin(12, 1, 2), 1.10)        # Oil/carbon: mild seasonality
             .when(F.col("month").isin(6, 7, 8), 0.95)
             .otherwise(1.0)
        )
    )
    .withColumn("_peak_adj",
        F.when((F.col("commodity_class") == "Power") & F.col("is_peak"), 1.35)
         .otherwise(1.0)
    )
    # Iran-Israel conflict event spikes — applied to Gas/Oil/LNG fully,
    # Power partially (follow-through from gas), Carbon/GOs unaffected
    .withColumn("_iran_spike",
        F.when(F.col("commodity_class").isin("Gas", "Oil", "LNG"),
            F.when((F.col("cal_date") >= F.lit("2024-04-10")) & (F.col("cal_date") <= F.lit("2024-05-15")), 1.18)
             .when((F.col("cal_date") >= F.lit("2024-10-01")) & (F.col("cal_date") <= F.lit("2024-11-20")), 1.22)
             .when((F.col("cal_date") >= F.lit("2025-06-15")) & (F.col("cal_date") <= F.lit("2025-09-30")), 1.28)
             .when((F.col("cal_date") >= F.lit("2026-01-10")) & (F.col("cal_date") <= F.lit("2026-03-05")), 1.20)
             .otherwise(1.0))
         .when(F.col("commodity_class") == "Power",
            F.when((F.col("cal_date") >= F.lit("2024-10-01")) & (F.col("cal_date") <= F.lit("2024-11-20")), 1.10)
             .when((F.col("cal_date") >= F.lit("2025-06-15")) & (F.col("cal_date") <= F.lit("2025-09-30")), 1.15)
             .when((F.col("cal_date") >= F.lit("2026-01-10")) & (F.col("cal_date") <= F.lit("2026-03-05")), 1.08)
             .otherwise(1.0))
         .otherwise(1.0)
    )
    .withColumn("price",
        F.round(F.col("_base") * F.col("_season") * F.col("_peak_adj") * F.col("_iran_spike") + F.randn(seed=901) * 8.0, 2)
    )
    .withColumn("price", F.greatest(F.col("price"), F.lit(0.01)))
    # Market volume also seasonal — winter sees much higher traded volumes
    .withColumn("_vol_season",
        F.when(F.col("commodity_class").isin("Power", "Gas", "LNG"),
            F.when(F.col("month").isin(12, 1, 2), 1.6)
             .when(F.col("month").isin(11, 3), 1.3)
             .when(F.col("month").isin(6, 7, 8), 0.5)
             .otherwise(1.0)
        ).otherwise(1.0)
    )
    .withColumn("volume_traded", F.round((F.rand(seed=902) * 50000 + 1000) * F.col("_vol_season"), 0).cast("long"))
    .select("time_key", "delivery_point_key", "price", "volume_traded")
)

df_market_prices.write.mode("overwrite").saveAsTable("fact_market_price")
print(f"fact_market_price: {df_market_prices.count():,} rows")

# COMMAND ----------

# DBTITLE 1,fact_position — daily net positions, keyed by day-level time_key (hour 00)
df_positions = (
    spark.table("fact_trade")
    # Derive day-level time_key (hour 00) for daily aggregation
    .withColumn("position_time_key", F.concat(F.substring("time_key", 1, 8), F.lit("00")))
    .withColumn("signed_volume",
        F.when(F.col("direction") == "BUY", F.col("volume"))
         .otherwise(-F.col("volume"))
    )
    .groupBy("position_time_key", "trader_key", "instrument_key", "delivery_point_key")
    .agg(
        F.sum("signed_volume").alias("net_volume"),
        F.sum(F.abs(F.col("volume"))).alias("gross_volume"),
        F.count("*").alias("trade_count"),
        F.avg("price").alias("avg_price"),
        F.sum("notional_value").alias("total_notional"),
        F.sum(F.when(F.col("direction") == "BUY", F.col("volume")).otherwise(0)).alias("buy_volume"),
        F.sum(F.when(F.col("direction") == "SELL", F.col("volume")).otherwise(0)).alias("sell_volume"),
    )
    .withColumnRenamed("position_time_key", "time_key")
    .withColumn("net_volume", F.round("net_volume", 2))
    .withColumn("gross_volume", F.round("gross_volume", 2))
    .withColumn("avg_price", F.round("avg_price", 2))
    .withColumn("total_notional", F.round("total_notional", 2))
    .withColumn("buy_volume", F.round("buy_volume", 2))
    .withColumn("sell_volume", F.round("sell_volume", 2))
    .withColumn("position_type",
        F.when(F.col("net_volume") > 0, "LONG")
         .when(F.col("net_volume") < 0, "SHORT")
         .otherwise("FLAT")
    )
)

df_positions.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("fact_position")
print(f"fact_position: {df_positions.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 · Keys and relationships
# MAGIC
# MAGIC Declares primary and foreign keys. These are *informational* in Unity Catalog —
# MAGIC they aren't enforced, but they tell Genie and BI tools how the tables join.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set PK columns to NOT NULL
# MAGIC ALTER TABLE ${catalog}.${schema}.dim_counterparty ALTER COLUMN counterparty_key SET NOT NULL;
# MAGIC ALTER TABLE ${catalog}.${schema}.dim_delivery_point ALTER COLUMN delivery_point_key SET NOT NULL;
# MAGIC ALTER TABLE ${catalog}.${schema}.dim_geography ALTER COLUMN geography_key SET NOT NULL;
# MAGIC ALTER TABLE ${catalog}.${schema}.dim_instrument ALTER COLUMN instrument_key SET NOT NULL;
# MAGIC ALTER TABLE ${catalog}.${schema}.dim_market ALTER COLUMN market_key SET NOT NULL;
# MAGIC ALTER TABLE ${catalog}.${schema}.dim_time ALTER COLUMN time_key SET NOT NULL;
# MAGIC ALTER TABLE ${catalog}.${schema}.dim_trader ALTER COLUMN trader_key SET NOT NULL;
# MAGIC
# MAGIC -- Drop existing constraints if any, cascading to dependent FKs (makes this cell idempotent)
# MAGIC ALTER TABLE ${catalog}.${schema}.dim_counterparty DROP CONSTRAINT IF EXISTS pk_dim_counterparty CASCADE;
# MAGIC ALTER TABLE ${catalog}.${schema}.dim_delivery_point DROP CONSTRAINT IF EXISTS pk_dim_delivery_point CASCADE;
# MAGIC ALTER TABLE ${catalog}.${schema}.dim_geography DROP CONSTRAINT IF EXISTS pk_dim_geography CASCADE;
# MAGIC ALTER TABLE ${catalog}.${schema}.dim_instrument DROP CONSTRAINT IF EXISTS pk_dim_instrument CASCADE;
# MAGIC ALTER TABLE ${catalog}.${schema}.dim_market DROP CONSTRAINT IF EXISTS pk_dim_market CASCADE;
# MAGIC ALTER TABLE ${catalog}.${schema}.dim_time DROP CONSTRAINT IF EXISTS pk_dim_time CASCADE;
# MAGIC ALTER TABLE ${catalog}.${schema}.dim_trader DROP CONSTRAINT IF EXISTS pk_dim_trader CASCADE;
# MAGIC
# MAGIC -- Add Primary Key constraints
# MAGIC ALTER TABLE ${catalog}.${schema}.dim_counterparty ADD CONSTRAINT pk_dim_counterparty PRIMARY KEY (counterparty_key);
# MAGIC ALTER TABLE ${catalog}.${schema}.dim_delivery_point ADD CONSTRAINT pk_dim_delivery_point PRIMARY KEY (delivery_point_key);
# MAGIC ALTER TABLE ${catalog}.${schema}.dim_geography ADD CONSTRAINT pk_dim_geography PRIMARY KEY (geography_key);
# MAGIC ALTER TABLE ${catalog}.${schema}.dim_instrument ADD CONSTRAINT pk_dim_instrument PRIMARY KEY (instrument_key);
# MAGIC ALTER TABLE ${catalog}.${schema}.dim_market ADD CONSTRAINT pk_dim_market PRIMARY KEY (market_key);
# MAGIC ALTER TABLE ${catalog}.${schema}.dim_time ADD CONSTRAINT pk_dim_time PRIMARY KEY (time_key);
# MAGIC ALTER TABLE ${catalog}.${schema}.dim_trader ADD CONSTRAINT pk_dim_trader PRIMARY KEY (trader_key);

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE ${catalog}.${schema}.dim_delivery_point ADD CONSTRAINT fk_delivery_point_geography FOREIGN KEY (geography_key) REFERENCES ${catalog}.${schema}.dim_geography(geography_key);

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_market_price ADD CONSTRAINT fk_market_price_time FOREIGN KEY (time_key) REFERENCES ${catalog}.${schema}.dim_time(time_key);
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_market_price ADD CONSTRAINT fk_market_price_delivery_point FOREIGN KEY (delivery_point_key) REFERENCES ${catalog}.${schema}.dim_delivery_point(delivery_point_key);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable type widening on fact tables
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_position SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true');
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_trade SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true');
# MAGIC
# MAGIC -- fact_position: widen INT columns to BIGINT
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_position ALTER COLUMN trader_key TYPE BIGINT;
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_position ALTER COLUMN instrument_key TYPE BIGINT;
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_position ALTER COLUMN delivery_point_key TYPE BIGINT;
# MAGIC
# MAGIC -- fact_trade: widen INT columns to BIGINT
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_trade ALTER COLUMN trader_key TYPE BIGINT;
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_trade ALTER COLUMN counterparty_key TYPE BIGINT;
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_trade ALTER COLUMN instrument_key TYPE BIGINT;
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_trade ALTER COLUMN delivery_point_key TYPE BIGINT;
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_trade ALTER COLUMN market_key TYPE BIGINT;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop constraint from earlier partial run
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_position DROP CONSTRAINT IF EXISTS fk_position_time;
# MAGIC
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_position ADD CONSTRAINT fk_position_time FOREIGN KEY (time_key) REFERENCES ${catalog}.${schema}.dim_time(time_key);
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_position ADD CONSTRAINT fk_position_trader FOREIGN KEY (trader_key) REFERENCES ${catalog}.${schema}.dim_trader(trader_key);
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_position ADD CONSTRAINT fk_position_instrument FOREIGN KEY (instrument_key) REFERENCES ${catalog}.${schema}.dim_instrument(instrument_key);
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_position ADD CONSTRAINT fk_position_delivery_point FOREIGN KEY (delivery_point_key) REFERENCES ${catalog}.${schema}.dim_delivery_point(delivery_point_key);

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_trade ADD CONSTRAINT fk_trade_time FOREIGN KEY (time_key) REFERENCES ${catalog}.${schema}.dim_time(time_key);
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_trade ADD CONSTRAINT fk_trade_trader FOREIGN KEY (trader_key) REFERENCES ${catalog}.${schema}.dim_trader(trader_key);
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_trade ADD CONSTRAINT fk_trade_counterparty FOREIGN KEY (counterparty_key) REFERENCES ${catalog}.${schema}.dim_counterparty(counterparty_key);
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_trade ADD CONSTRAINT fk_trade_instrument FOREIGN KEY (instrument_key) REFERENCES ${catalog}.${schema}.dim_instrument(instrument_key);
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_trade ADD CONSTRAINT fk_trade_delivery_point FOREIGN KEY (delivery_point_key) REFERENCES ${catalog}.${schema}.dim_delivery_point(delivery_point_key);
# MAGIC ALTER TABLE ${catalog}.${schema}.fact_trade ADD CONSTRAINT fk_trade_market FOREIGN KEY (market_key) REFERENCES ${catalog}.${schema}.dim_market(market_key);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 · FX rates and the enriched trade view
# MAGIC
# MAGIC Trades are priced in **EUR, GBP and USD**, so totals can't simply be added up.
# MAGIC We create a small FX table and convert everything to **EUR** for reporting.
# MAGIC
# MAGIC We also attach each trade's **market price** (the going rate at that hour and hub)
# MAGIC and flag sells that were priced far below it.

# COMMAND ----------

# ── FX rates: convert every trading currency to EUR ──────────────────────────
from pyspark.sql import functions as F, Row

fx_rows = [
    Row(currency_code="EUR", rate_to_eur=1.00),
    Row(currency_code="GBP", rate_to_eur=1.17),   # 1 GBP ≈ 1.17 EUR
    Row(currency_code="USD", rate_to_eur=0.92),   # 1 USD ≈ 0.92 EUR
]

(spark.createDataFrame(fx_rows)
      .write.mode("overwrite")
      .saveAsTable(f"{CATALOG}.{SCHEMA}.dim_fx_rate"))

print(f"✓ dim_fx_rate created in {CATALOG}.{SCHEMA}")
display(spark.table(f"{CATALOG}.{SCHEMA}.dim_fx_rate"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### The enriched trade view
# MAGIC
# MAGIC `trade_enriched` is what the dashboard and Genie actually read. For each trade it adds:
# MAGIC
# MAGIC | Column | Meaning |
# MAGIC |---|---|
# MAGIC | `market_price` | the going rate at that hour and delivery point |
# MAGIC | `pct_vs_market` | how far the trade price sat from the market (negative = below) |
# MAGIC | `is_below_market` | a sell priced **more than 35% below** the market — worth investigating |
# MAGIC | `price_eur`, `notional_value_eur` | money converted to euros so totals are comparable |
# MAGIC
# MAGIC Cancelled trades are excluded, and a trade is only compared against a price for the
# MAGIC **same commodity** — comparing a gas trade to an oil hub would be meaningless.

# COMMAND ----------

# ── trade_enriched: trades + market price + EUR values + surveillance flag ──
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.trade_enriched AS
SELECT
    ft.*,
    i.commodity,
    i.currency,
    dp.commodity_class,

    -- the market ("going rate") price at that hour and delivery point
    mp.price                                   AS market_price,
    (ft.price - mp.price) / mp.price           AS pct_vs_market,

    -- money converted to EUR so figures can be added up safely
    ft.price          * fx.rate_to_eur         AS price_eur,
    ft.notional_value * fx.rate_to_eur         AS notional_value_eur,

    -- flag: a SELL priced far below the market, compared like-for-like
    CASE
      WHEN ft.direction = 'SELL'
       AND (ft.price - mp.price) / mp.price < -0.35
       AND (   (i.commodity = 'Natural Gas' AND dp.commodity_class = 'Gas')
            OR (i.commodity = 'Crude Oil'   AND dp.commodity_class = 'Oil')
            OR (i.commodity = dp.commodity_class) )
      THEN true ELSE false
    END                                        AS is_below_market

FROM      {CATALOG}.{SCHEMA}.fact_trade          ft
LEFT JOIN {CATALOG}.{SCHEMA}.fact_market_price   mp
       ON ft.time_key = mp.time_key
      AND ft.delivery_point_key = mp.delivery_point_key
LEFT JOIN {CATALOG}.{SCHEMA}.dim_instrument      i
       ON ft.instrument_key = i.instrument_key
LEFT JOIN {CATALOG}.{SCHEMA}.dim_delivery_point  dp
       ON ft.delivery_point_key = dp.delivery_point_key
LEFT JOIN {CATALOG}.{SCHEMA}.dim_fx_rate         fx
       ON i.currency = fx.currency_code
WHERE ft.status <> 'CANCELLED'
""")

print(f"✓ trade_enriched created in {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### A small sample for the price-trend chart
# MAGIC
# MAGIC Plotting a million trades doesn't render. This view narrows to **TTF gas at the TTF hub**,
# MAGIC so a dashboard can show the market price line with individual trades marked above or
# MAGIC below it.

# COMMAND ----------

# ── A plottable slice: one commodity, one hub ───────────────────────────────
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.trade_vs_market_sample AS
SELECT
    te.trade_timestamp,
    te.trade_id,
    te.price,
    te.market_price,
    te.pct_vs_market,
    te.direction,
    te.volume,
    t.trader_name,
    t.company,
    CASE WHEN te.is_below_market THEN 'Below market' ELSE 'At market' END AS price_status
FROM      {CATALOG}.{SCHEMA}.trade_enriched te
JOIN      {CATALOG}.{SCHEMA}.dim_trader      t  ON te.trader_key = t.trader_key
JOIN      {CATALOG}.{SCHEMA}.dim_delivery_point dp ON te.delivery_point_key = dp.delivery_point_key
WHERE dp.zone_code = 'TTF'
  AND te.commodity = 'Natural Gas'
  AND te.market_price IS NOT NULL
""")

print(f"✓ trade_vs_market_sample created in {CATALOG}.{SCHEMA}")
print("  Use this view for the 'price vs market' scatter chart.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 · The metric view
# MAGIC
# MAGIC This is the **governed layer**. It defines the joins and the business measures once,
# MAGIC so every dashboard and Genie answer uses the same definitions.
# MAGIC
# MAGIC Measures we don't need for the workshop are left in but **commented out** — uncomment
# MAGIC any of them if a question comes up. Money measures use the **EUR** columns so totals
# MAGIC are meaningful.

# COMMAND ----------

# ── The metric view: one governed definition for dashboards and Genie ───────
metric_view_yaml = """
version: 1.1

source: {CATALOG}.{SCHEMA}.trade_enriched

joins:
  - name: dt
    source: {CATALOG}.{SCHEMA}.dim_time
    "on": source.time_key = dt.time_key
  - name: dtr
    source: {CATALOG}.{SCHEMA}.dim_trader
    "on": source.trader_key = dtr.trader_key
  - name: dcp
    source: {CATALOG}.{SCHEMA}.dim_counterparty
    "on": source.counterparty_key = dcp.counterparty_key
  - name: di
    source: {CATALOG}.{SCHEMA}.dim_instrument
    "on": source.instrument_key = di.instrument_key
  - name: ddp
    source: {CATALOG}.{SCHEMA}.dim_delivery_point
    "on": source.delivery_point_key = ddp.delivery_point_key
    joins:
      - name: dg
        source: {CATALOG}.{SCHEMA}.dim_geography
        "on": ddp.geography_key = dg.geography_key
  - name: dm
    source: {CATALOG}.{SCHEMA}.dim_market
    "on": source.market_key = dm.market_key

comment: "Energy trading KPIs. Market-wide trade data with market-price comparison.
  All money measures are in EUR."

dimensions:
  # ── When ──
  - name: Trade Date
    expr: dt.cal_date
  - name: Trade Month
    expr: DATE_TRUNC('MONTH', dt.cal_date)
  - name: Year
    expr: dt.year
  - name: Quarter
    expr: dt.quarter
  - name: Trading Season
    expr: dt.trading_season
  - name: Is Peak
    expr: dt.is_peak

  # ── Who ──
  - name: Trader Name
    expr: dtr.trader_name
  - name: Trader Company
    expr: dtr.company
  - name: Trader Company Type
    expr: dtr.company_type
  - name: Trading Desk
    expr: dtr.desk
  - name: Trader Seniority
    expr: dtr.seniority

  # ── With whom ──
  - name: Counterparty Name
    expr: dcp.counterparty_name
  - name: Counterparty Type
    expr: dcp.counterparty_type
  - name: Credit Rating
    expr: dcp.credit_rating
  - name: Is Exchange
    expr: dcp.is_exchange

  # ── What ──
  - name: Commodity
    expr: di.commodity
  - name: Instrument Name
    expr: di.instrument_name
  - name: Unit
    expr: di.unit
  - name: Currency
    expr: di.currency

  # ── Where ──
  - name: Zone Name
    expr: ddp.zone_name
  - name: Zone Code
    expr: ddp.zone_code
  - name: Country Name
    expr: ddp.dg.country_name
  - name: Continent
    expr: ddp.dg.continent

  # ── How ──
  - name: Market Name
    expr: dm.market_name
  - name: Is OTC
    expr: dm.is_otc

  # ── About the trade itself ──
  - name: Direction
    expr: source.direction
  - name: Status
    expr: source.status
  - name: Price Status
    expr: CASE WHEN source.is_below_market THEN 'Below market' ELSE 'At market' END

measures:
  # ══ Activity ══════════════════════════════════════════════════════════
  - name: Trade Count
    expr: COUNT(1)
    comment: Total number of trades

  - name: Total Volume
    expr: SUM(source.volume)
    comment: Total traded volume (mixed units across commodities)

  - name: Unique Traders
    expr: COUNT(DISTINCT source.trader_key)
    comment: Number of distinct traders

  - name: Unique Counterparties
    expr: COUNT(DISTINCT source.counterparty_key)
    comment: Number of distinct counterparties

  # ══ Price ═════════════════════════════════════════════════════════════
  - name: VWAP
    expr: SUM(source.price * source.volume) / NULLIF(SUM(source.volume), 0)
    comment: Volume-weighted average price

  # ══ Direction ═════════════════════════════════════════════════════════
  - name: Buy Volume
    expr: SUM(CASE WHEN source.direction = 'BUY' THEN source.volume ELSE 0 END)
    comment: Total volume bought

  - name: Sell Volume
    expr: SUM(CASE WHEN source.direction = 'SELL' THEN source.volume ELSE 0 END)
    comment: Total volume sold

  # ══ Money (EUR) ═══════════════════════════════════════════════════════
  - name: Total Notional EUR
    expr: SUM(source.notional_value_eur)
    comment: Total value of all trades, converted to EUR
    format:
      type: currency
      currency_code: EUR
      abbreviation: compact

  # ══ Surveillance ══════════════════════════════════════════════════════
  - name: Avg Pct vs Market
    expr: SUM(source.volume * source.pct_vs_market) / NULLIF(SUM(source.volume), 0)
    comment: Volume-weighted average distance from the market price (negative = below)
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2

  - name: Below Market Sell Count
    expr: COUNT(CASE WHEN source.is_below_market THEN 1 END)
    comment: Number of sells priced far below the market

  - name: Below Market Ratio
    expr: COUNT(CASE WHEN source.is_below_market THEN 1 END)
        / NULLIF(COUNT(CASE WHEN source.direction = 'SELL' THEN 1 END), 0)
    comment: Share of sells that were priced far below the market
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2

  - name: Worst Discount
    expr: -MIN(source.pct_vs_market)
    comment: The single largest gap below the market price
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2

  - name: Flagged Traders
    expr: COUNT(DISTINCT CASE WHEN source.is_below_market THEN source.trader_key END)
    comment: Number of distinct traders with at least one below-market sell

  - name: Below Market Notional EUR
    expr: SUM(CASE WHEN source.is_below_market THEN source.notional_value_eur END)
    comment: Value of below-market sells, in EUR
    format:
      type: currency
      currency_code: EUR
      abbreviation: compact

  - name: Prev Month Below Market Ratio
    expr: MEASURE(`Below Market Ratio`)
    window:
      - order: Trade Month
        semiadditive: last
        range: trailing 1 month
    comment: Prior month's ratio, for month-over-month comparison

  - name: Revenue EUR
    expr: SUM(CASE WHEN source.direction = 'SELL' THEN source.notional_value_eur END)
    comment: Value of everything sold, converted to EUR
    format:
      type: currency
      currency_code: EUR
      abbreviation: compact

  - name: Cost EUR
    expr: SUM(CASE WHEN source.direction = 'BUY' THEN source.notional_value_eur END)
    comment: Value of everything bought, converted to EUR
    format:
      type: currency
      currency_code: EUR
      abbreviation: compact

  - name: Net Cash Flow EUR
    expr: SUM(CASE WHEN source.direction = 'SELL' THEN source.notional_value_eur
                   ELSE -source.notional_value_eur END)
    comment: Sell value minus buy value, in EUR. Net cash flow, not accounting profit.
    format:
      type: currency
      currency_code: EUR
      abbreviation: compact

  - name: Prev Month Revenue EUR
    expr: MEASURE(`Revenue EUR`)
    window:
      - order: Trade Month
        semiadditive: last
        range: trailing 1 month
    comment: Prior month revenue, for month-over-month comparison

  - name: MoM Revenue Growth
    expr: (MEASURE(`Revenue EUR`) - MEASURE(`Prev Month Revenue EUR`))
        / NULLIF(MEASURE(`Prev Month Revenue EUR`), 0) * 100
    comment: Month-over-month revenue growth, percent
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2

  # ══ Parked — uncomment if needed ══════════════════════════════════════
  # Unweighted mean across mixed currencies; use VWAP instead.
  # - name: Average Price
  #   expr: AVG(source.price)
  #
  # Rarely asked for, and mixes units across commodities.
  # - name: Average Volume
  #   expr: AVG(source.volume)
  # - name: Max Volume
  #   expr: MAX(source.volume)
  # - name: Max Price
  #   expr: MAX(source.price)
  # - name: Min Price
  #   expr: MIN(source.price)
  # - name: Unique Instruments
  #   expr: COUNT(DISTINCT source.instrument_key)
  #
  # Directional money splits — available if the story needs them.
  # - name: Buy Notional EUR
  #   expr: SUM(CASE WHEN source.direction = 'BUY' THEN source.notional_value_eur END)
  # - name: Sell Notional EUR
  #   expr: SUM(CASE WHEN source.direction = 'SELL' THEN source.notional_value_eur END)
  #
  # Net cash flow (sell minus buy). Not true profit and loss.
  # - name: Net Cash Flow EUR
  #   expr: SUM(CASE WHEN source.direction = 'SELL' THEN source.notional_value_eur
  #                  ELSE -source.notional_value_eur END)
  #
  # Rolling windows — can behave unexpectedly when regrouped.
  # - name: Rolling 12M Trade Count
  #   expr: MEASURE(`Trade Count`)
  #   window:
  #     - order: Trade Date
  #       semiadditive: last
  #       range: trailing 12 month
  # - name: Rolling 6M Total Volume
  #   expr: MEASURE(`Total Volume`)
  #   window:
  #     - order: Trade Date
  #       semiadditive: last
  #       range: trailing 6 month
""".format(CATALOG=CATALOG, SCHEMA=SCHEMA)

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.energy_trading_metrics
WITH METRICS
LANGUAGE YAML
AS $$
{metric_view_yaml}
$$
""")

print(f"✓ energy_trading_metrics created in {CATALOG}.{SCHEMA}")
print("  Point your AI/BI dashboard and Genie space at this metric view.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 · AI-generated descriptions *(optional)*
# MAGIC
# MAGIC Uses a Databricks foundation model to write table and column descriptions into
# MAGIC Unity Catalog. Good descriptions help Genie answer more accurately.
# MAGIC
# MAGIC Set `dry_run = false` in the configuration below to actually apply them.

# COMMAND ----------

# DBTITLE 1,Part 4 configuration (AI descriptions)
# catalog / schema are inherited from Part 1's Configuration cell (CATALOG, SCHEMA).
dbutils.widgets.text("model", "databricks-meta-llama-3-3-70b-instruct", "AI model endpoint")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run (preview only)")
dbutils.widgets.dropdown("skip_documented", "false", ["true", "false"], "Skip already-documented objects")

MODEL           = dbutils.widgets.get("model").strip()
DRY_RUN         = dbutils.widgets.get("dry_run") == "true"
SKIP_DOCUMENTED = dbutils.widgets.get("skip_documented") == "true"
DOMAIN          = ("an ENERGY TRADING business (power, gas, oil, and carbon markets: trades, "
                   "positions, market prices, counterparties, delivery points, and instruments)")

print(f"Target:          {CATALOG}.{SCHEMA}")
print(f"Model:           {MODEL}")
print(f"Dry run:         {DRY_RUN}  (set dry_run=false to apply comments)")
print(f"Skip documented: {SKIP_DOCUMENTED}")

# COMMAND ----------

# DBTITLE 1,Helpers — enumerate objects, build prompts
import json
from pyspark.sql.functions import expr

def sql_lit(text: str) -> str:
    """Collapse whitespace and escape single quotes for a SQL string literal."""
    return " ".join(str(text).split()).replace("'", "''")

def list_objects():
    """Return [(name, table_type, is_view, comment)] for every object in the schema."""
    rows = spark.sql(f"""
        SELECT table_name, table_type, comment
        FROM {CATALOG}.information_schema.tables
        WHERE table_schema = '{SCHEMA}'
        ORDER BY table_type, table_name
    """).collect()
    out = []
    for r in rows:
        is_view = "VIEW" in (r["table_type"] or "").upper()
        out.append((r["table_name"], r["table_type"], is_view, r["comment"]))
    return out

def list_columns(table_name: str):
    """Ordered [(column_name, data_type)] for a table/view."""
    rows = spark.sql(f"""
        SELECT column_name, data_type
        FROM {CATALOG}.information_schema.columns
        WHERE table_schema = '{SCHEMA}' AND table_name = '{table_name}'
        ORDER BY ordinal_position
    """).collect()
    return [(r["column_name"], r["data_type"]) for r in rows]

def sample_rows(table_name: str, n: int = 3) -> str:
    """A compact string of up to n sample rows (base tables only)."""
    try:
        pdf = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.`{table_name}` LIMIT {n}").toPandas()
        return pdf.to_json(orient="records")[:2000]
    except Exception as e:
        return f"(no sample available: {e})"

def view_definition(table_name: str) -> str:
    try:
        rows = spark.sql(f"""
            SELECT view_definition
            FROM {CATALOG}.information_schema.views
            WHERE table_schema = '{SCHEMA}' AND table_name = '{table_name}'
        """).collect()
        return (rows[0]["view_definition"] or "")[:3000] if rows else ""
    except Exception:
        return ""

def build_prompt(name, is_view, columns):
    cols_desc = "\n".join(f"  - {c} ({t})" for c, t in columns)
    kind = "metric view" if is_view else "table"
    context = (f"VIEW DEFINITION:\n{view_definition(name)}"
               if is_view else f"SAMPLE ROWS (JSON):\n{sample_rows(name)}")
    col_keys = ", ".join(f'"{c}"' for c, _ in columns)
    return (
        f"You are a data catalog assistant documenting a star-schema data warehouse for {DOMAIN}.\n\n"
        f"Document the {kind} `{CATALOG}.{SCHEMA}.{name}`.\n\n"
        f"COLUMNS:\n{cols_desc}\n\n{context}\n\n"
        "Return ONLY a JSON object with this exact shape and nothing else:\n"
        '{"table_description": "<2 to 5 sentences describing what this object represents, '
        'its grain, and how it is used for analytics and reporting>", '
        f'"columns": {{{col_keys}: "<2 to 3 sentences describing this column, its meaning, '
        'units where relevant, and any keys/relationships>"}}}\n'
        "Rules: table_description must be 2-5 sentences; every column description must be "
        "2-3 sentences; be specific to the domain described above; do not invent columns; "
        "return valid JSON only."
    )

objects = list_objects()
print(f"Found {len(objects)} objects in {CATALOG}.{SCHEMA}:")
for name, ttype, is_view, comment in objects:
    flag = " (documented)" if comment else ""
    print(f"  - {name:24s} [{ttype}]{flag}")

# COMMAND ----------

# DBTITLE 1,Generate descriptions with ai_query (batch)
targets = [(n, tt, iv) for (n, tt, iv, cmt) in objects if not (SKIP_DOCUMENTED and cmt)]

meta = {}          # name -> {"is_view", "columns"}
prompt_rows = []   # (name, prompt)
raw = {}
if not targets:
    # Skip only THIS section — never dbutils.notebook.exit(), which would halt the
    # entire merged full-solution notebook so later parts (e.g. the Kong GL model)
    # would never run.
    print("Nothing to document (all objects already have comments) — skipping this section.")
else:
    for name, ttype, is_view in targets:
        cols = list_columns(name)
        meta[name] = {"is_view": is_view, "columns": [c for c, _ in cols]}
        prompt_rows.append((name, build_prompt(name, is_view, cols)))

    prompts_df = spark.createDataFrame(prompt_rows, "name STRING, prompt STRING")
    result_df = prompts_df.withColumn(
        "out",
        expr(f"to_json(ai_query('{MODEL}', prompt, failOnError => false))")
    )
    # Materialize once and pull to the driver (object count is small)
    raw = {r["name"]: r["out"] for r in result_df.select("name", "out").collect()}
    print(f"Generated responses for {len(raw)} objects.")

# COMMAND ----------

# DBTITLE 1,Parse responses
import re

def _extract_json(text: str):
    """Try to extract a JSON object from text that may include markdown fences."""
    # Try to find a fenced JSON block first
    m = re.search(r'```(?:json)?\s*\n?(\{.*?\})\s*```', text, re.DOTALL)
    if m:
        return m.group(1)
    # Otherwise find the first { ... } block
    start = text.find('{')
    if start == -1:
        return None
    depth = 0
    for i in range(start, len(text)):
        if text[i] == '{':
            depth += 1
        elif text[i] == '}':
            depth -= 1
            if depth == 0:
                return text[start:i+1]
    return None

def parse_response(out_json: str):
    """out_json is to_json() of the ai_query struct: {result, errorMessage}."""
    try:
        outer = json.loads(out_json)
    except Exception as e:
        return None, f"could not parse ai_query envelope: {e}"
    err = outer.get("errorMessage") or outer.get("error")
    if err:
        return None, err
    resp = outer.get("result") or outer.get("response")
    if not resp:
        return None, "empty response"
    # Try direct JSON parse first
    try:
        return json.loads(resp), None
    except Exception:
        pass
    # Try extracting JSON from markdown/text response
    extracted = _extract_json(resp)
    if extracted:
        try:
            return json.loads(extracted), None
        except Exception as e:
            return None, f"extracted JSON is invalid: {e}"
    return None, "model did not return valid JSON (no JSON object found in response)"

parsed = {}   # name -> {"table": str, "columns": {col: str}}
errors = {}
for name, out_json in raw.items():
    data, err = parse_response(out_json)
    if err:
        errors[name] = err
        continue
    parsed[name] = {
        "table": (data.get("table_description") or "").strip(),
        "columns": {k: (v or "").strip() for k, v in (data.get("columns") or {}).items()},
    }

if errors:
    print("⚠️  Errors:")
    for n, e in errors.items():
        print(f"   {n}: {e}")
print(f"Parsed descriptions for {len(parsed)} objects.")

# COMMAND ----------

# DBTITLE 1,Preview — proposed descriptions
preview = []
for name, d in parsed.items():
    preview.append((name, "TABLE", d["table"]))
    for col in meta[name]["columns"]:
        preview.append((name, col, d["columns"].get(col, "⚠️ (missing — will be skipped)")))

preview_df = spark.createDataFrame(preview, "object STRING, target STRING, proposed_description STRING")
print("Review the proposed descriptions below. Set dry_run=false and re-run to apply.")
display(preview_df)

# COMMAND ----------

# DBTITLE 1,Apply comments to Unity Catalog
def apply_comments():
    applied, skipped, failed = 0, 0, []
    for name, d in parsed.items():
        is_view = meta[name]["is_view"]
        fq = f"{CATALOG}.{SCHEMA}.`{name}`"
        obj_kw = "VIEW" if is_view else "TABLE"

        # Object-level comment
        if d["table"]:
            try:
                spark.sql(f"COMMENT ON {obj_kw} {fq} IS '{sql_lit(d['table'])}'")
                applied += 1
            except Exception as e:
                failed.append((name, "TABLE", str(e)[:200]))

        # Column-level comments
        alter_kw = "ALTER VIEW" if is_view else "ALTER TABLE"
        for col in meta[name]["columns"]:
            desc = d["columns"].get(col)
            if not desc:
                skipped += 1
                continue
            try:
                spark.sql(f"{alter_kw} {fq} ALTER COLUMN `{col}` COMMENT '{sql_lit(desc)}'")
                applied += 1
            except Exception as e:
                failed.append((name, col, str(e)[:200]))
    return applied, skipped, failed

if DRY_RUN:
    print("DRY RUN — no comments written. Set the dry_run widget to 'false' and re-run to apply.")
else:
    applied, skipped, failed = apply_comments()
    print(f"✅ Applied {applied} comments.  Skipped {skipped} (no description).")
    if failed:
        print(f"⚠️  {len(failed)} failed (metric-view columns may not accept ALTER COLUMN):")
        for name, tgt, err in failed:
            print(f"   {name}.{tgt}: {err}")

# COMMAND ----------

# DBTITLE 1,Verify — show applied comments
if not DRY_RUN:
    display(spark.sql(f"""
        SELECT table_name AS object, 'TABLE' AS target, comment AS description
        FROM {CATALOG}.information_schema.tables
        WHERE table_schema = '{SCHEMA}' AND comment IS NOT NULL
        UNION ALL
        SELECT table_name, column_name, comment
        FROM {CATALOG}.information_schema.columns
        WHERE table_schema = '{SCHEMA}' AND comment IS NOT NULL
        ORDER BY object, target
    """))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Done
# MAGIC
# MAGIC Your schema now contains:
# MAGIC
# MAGIC **Tables** — `dim_time`, `dim_geography`, `dim_trader`, `dim_counterparty`,
# MAGIC `dim_instrument`, `dim_delivery_point`, `dim_market`, `dim_fx_rate`,
# MAGIC `fact_trade`, `fact_market_price`, `fact_position`
# MAGIC
# MAGIC **Views** — `trade_enriched` (trades + market price + EUR),
# MAGIC `trade_vs_market_sample` (a plottable slice for the price chart)
# MAGIC
# MAGIC **Metric view** — `energy_trading_metrics`
# MAGIC
# MAGIC ### Next steps
# MAGIC 1. Create an **AI/BI Dashboard** on `energy_trading_metrics`.
# MAGIC 2. Create a **Genie space** on the same metric view.
# MAGIC 3. Ask it something — for example *"which trader sold furthest below the market price?"*
# MAGIC
