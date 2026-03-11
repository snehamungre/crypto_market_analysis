# crypto_market_analysis

An end-to-end data engineering pipeline that ingests live cryptocurrency market data from a public REST API, processes it using PySpark, performs analytical transformations using both the DataFrame API and Spark SQL, and produces a structured analytics dataset with built-in data quality validations.

---

## Table of Contents

- [Architecture and Design](#architecture-and-design)
- [Data Quality Rules](#data-quality-rules)
- [SQL Transformations](#sql-transformations)
- [Partitioning Strategy](#partitioning-strategy)
- [Sample Output](#output)
- [Assumptions](#assumptions)
- [Limitations](#limitations)
- [How to Run](#how-to-run)

---

## Architecture and Design

Cryptocurrency markets update continuously, making a scheduled daily pipeline the most appropriate design for this use case. The pipeline follows a three-layer architecture:

**1. Raw Ingestion — `data/raw/`**
Market data is fetched from the [CoinGecko Markets API](https://docs.coingecko.com/v3.0.1/reference/coins-markets) REST endpoint and saved as a timestamped JSON file. Note that the API returns data available at the time of the request; individual coin records may have been last updated on different dates.

**2. Processed Layer — `data/processed/`**
The raw JSON is read, cleaned, and validated according to the data quality rules described below. The cleaned data is then written to Parquet format, partitioned by `updated_date`, in append mode to preserve historical snapshots.

**3. Analytics Layer — `data/analytics/`**
The processed Parquet data is read by `analysis.py`, which applies Spark SQL aggregations and window function rankings. The resulting analytical tables are written to `data/analytics/` for downstream reporting use cases.

The pipeline is orchestrated by a scheduler in `main.py` that runs all three stages sequentially every 24 hours.

---

## Data Quality Rules

The following validation rules are applied during the processing stage. Records that fail these checks are excluded from the processed dataset.

- **Duplicate removal** — Records are deduplicated based on all columns excluding timestamp fields.
- **Non-negative values** — Rows where `current_price`, `market_cap`, `total_volume`, `circulating_supply`, or `total_supply` are negative are filtered out.
- **Null value handling** — Rows with null values in critical numeric fields (`current_price`, `market_cap`, `total_volume`, `circulating_supply`, `total_supply`) are dropped.
- **Type casting** — Timestamp fields (`last_updated`, `ath_date`, `atl_date`) are cast to proper timestamp types to ensure schema consistency.
- **Derived date column** — An `updated_date` column is extracted from `last_updated` for partitioning and date-based analysis.

> **Note:** The CoinGecko free tier does not guarantee uniform coin coverage across API calls. Some coins may appear on fewer days than others, which affects the reliability of cross-coin averages over time.

---

## SQL Transformations

Spark SQL is used for all aggregation and ranking queries. DataFrames are registered as temporary views before querying.

**Average Market Capitalisation by Coin**

Calculates the average market cap per coin across all ingested dates, then ranks coins in descending order.

```sql
WITH avg_market AS (
    SELECT name, AVG(market_cap) AS avg_market_cap
    FROM crypto_prices
    GROUP BY name
)
SELECT *, RANK() OVER (ORDER BY avg_market_cap DESC) AS avg_market_cap_rank
FROM avg_market
```

**Average Price by Coin**

Calculates the average current price per coin across all ingested dates, then ranks coins in descending order.

```sql
WITH average_prices AS (
    SELECT name, AVG(current_price) AS average_price
    FROM crypto_prices
    GROUP BY name
)
SELECT *, RANK() OVER (ORDER BY average_price DESC) AS avg_price_rank
FROM average_prices
```

Both queries use **window functions** (`RANK() OVER`) to assign rankings without collapsing the result set, which allows the rankings to be joined back to other metrics in the final analytics output.

---

## Partitioning Strategy

The processed Parquet data is partitioned by `updated_date`.

This decision is motivated by two factors:

1. **Pipeline design** — Because the scheduler appends new data daily, partitioning by date ensures each run writes to its own partition, preventing data fragmentation and avoiding overwrites of historical records.
2. **Query efficiency** — Several analytical operations target only the most recent snapshot, while others compute averages across all dates. Date-based partitioning enables Spark to apply partition pruning, allowing these queries to read only the relevant subset of data rather than scanning the full dataset.

---

## Assumptions

**Top-Performing Coin Ranking Methodology**

Coins are ranked using a weighted sum across three market metrics, each selected for its relevance to long-term coin performance:

- **Market Capitalisation** — A higher market cap indicates a more established coin with greater market presence.
- **Price** — Price is used as a proxy for demand. In cryptocurrency markets, price and demand tend to correlate more directly than in traditional asset classes.
- **Volume/Market Cap Ratio** — Used as a measure of market stability. A disproportionately high ratio indicates elevated buy/sell activity relative to the coin's size, which may signal volatility.

The composite rank is calculated as a weighted sum with the following weights:

| Metric                  | Weight |
|-------------------------|--------|
| Market Capitalisation   | 0.4    |
| Price                   | 0.4    |
| Volume/Market Cap Ratio | 0.2    |

Rankings are derived from historical data rather than single-day snapshots, as aggregated metrics are less susceptible to short-term market fluctuations.


#### Output

The table below shows a sample of the final analytics output, combining average market cap, average price, and volume/market cap ratio rankings into a single top-performing assets view.

| name        | top_performing_rank |   avg_market_cap   | avg_market_cap_rank | average_price | avg_price_rank |   total_volume  | vol_market_ratio | vol_market_rank | top_performing_score |
|-------------|:-------------------:|:------------------:|:-------------------:|:-------------:|:--------------:|:---------------:|:----------------:|:---------------:|:--------------------:|
| Ethereum    |          1          |  2.447566175596E11 |          2          |    2026.751   |        4       | 3.1391592618E10 |      0.12236     |        27       |           8          |
| Bitcoin     |          2          | 1.3776601759463E12 |          1          |    68839.2    |        1       |  7.194525264E10 |      0.12236     |        52       |          11          |
| Solana      |          3          |  4.92225953663E10  |          7          |     86.283    |       13       |  6.426767762E9  |      0.12365     |        26       |          13          |
| Tether Gold |          4          |   2.8750949604E9   |          35         |    5173.367   |        3       |   9.75957539E8  |      0.12365     |        8        |          17          |
| PAX Gold    |          5          |   2.5447346606E9   |          37         |    5214.701   |        2       |   7.61452059E8  |      0.12365     |        11       |          18          |
---

---

## Limitations

- The CoinGecko free tier API returns up to 250 coins per request and does not provide bulk historical data. Historical depth is accumulated organically through daily scheduled runs.
- Not all coins are returned consistently across every API call. Average-based metrics should be interpreted with this in mind, as some coins may have fewer data points than others.
- The `updated_date` column is derived from the `last_updated` field provided by the API, which reflects when CoinGecko last updated that coin's data — not necessarily when the pipeline ran.

---

## How to Run

### Prerequisites

Ensure the following are installed on your machine before proceeding:

- **Java 17 or above** — Required by PySpark. You can verify your version by running `java -version` in your terminal. Java can be downloaded from [oracle.net](https://www.oracle.com/ae/java/technologies/downloads/).
- **Python 3.8 or above**

### 1. Clone the Repository

```bash
git clone https://github.com/snehamungre/crypto_market_analysis.git
cd crypto_market_analysis
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure the API Token

Create a `.env` file in the project root directory with the following contents:

```
API_KEY=your_coingecko_api_key_here
```

A free API token can be obtained from the [CoinGecko API portal](https://docs.coingecko.com/v3.0.1/reference/coins-markets). Do not commit this file to version control.

### 4. Run the Pipeline

From the project root directory, run:

```bash
python src/main.py
```

The pipeline will execute immediately on startup, then continue to run on a 24-hour schedule. Progress messages will be printed to the terminal at each stage.

### 5. View the Output

Analytical results are written to `data/analytics/` after each successful run.



