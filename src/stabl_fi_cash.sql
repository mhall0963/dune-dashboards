-- SPDX-FileCopyrightText: 2023-present Matthew Hall <matt.hall0963@gmail.com>
-- SPDX-License-Identifier: MPL-2.0

-- Source: https://github.com/nebel-finance/dune-dashboards


WITH

-- Total supply
supply AS (
  SELECT
    block_date,
    CAST(output_0 AS DOUBLE) / 1e18 AS value
  FROM (
    SELECT *,
      DATE(call_block_time) AS block_date,
      ROW_NUMBER() OVER(
        PARTITION BY DATE(call_block_time)
        ORDER BY call_block_time DESC, call_trace_address DESC
      ) AS rn
    FROM stabl_fi_v2_polygon.CASH_call_totalSupply
    WHERE call_success = TRUE
  )
  WHERE rn = 1
),

-- All wallets
wallet_list AS (
  SELECT DISTINCT("to") AS wallet
  FROM stabl_fi_v2_polygon.CASH_evt_Transfer
),

--- Transactions per wallet
tx AS (
  SELECT
    d.block_date,
    w.wallet,
    SUM(IF("to" = w.wallet, CAST(tx.value AS DOUBLE), -CAST(tx.value AS DOUBLE))) / 1e18 AS value
  FROM (
    SELECT block_date FROM supply
  ) d
  CROSS JOIN wallet_list w
  LEFT JOIN stabl_fi_v2_polygon.CASH_evt_Transfer tx
    ON d.block_date = DATE(tx.evt_block_time)
    AND (w.wallet = tx."to" OR w.wallet = tx."from")
  GROUP BY block_date, wallet
),

txs AS (
  SELECT
    d.block_date,
    w.wallet,
    tx.value,
    SUM(tx.value) OVER(
      PARTITION BY w.wallet
      ORDER BY d.block_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS balance
  FROM (
    SELECT block_date FROM supply
  ) d
  CROSS JOIN wallet_list w
  LEFT JOIN tx ON d.block_date = tx.block_date AND w.wallet = tx.wallet
),

-- Total holders
holder AS (
  SELECT
    block_date,
    COUNT(wallet) AS value
  FROM txs
  WHERE balance >= 1  -- exclude wallets with less than 1$ of CASH
  GROUP BY block_date
),

-- Holder breakdown by balance
holder_balance AS (
  SELECT
    COALESCE(SUM(balance) FILTER (WHERE balance >= 1 AND balance <= 100),0) AS "1-100 CASH (b)",
    COALESCE(SUM(balance) FILTER (WHERE balance > 100 AND balance <= 1000),0) AS "100-1K CASH (b)",
    COALESCE(SUM(balance) FILTER (WHERE balance > 1000 AND balance <= 10000),0) AS "1K-10K CASH (b)",
    COALESCE(SUM(balance) FILTER (WHERE balance > 10000 AND balance <= 100000),0) AS "10K-100K CASH (b)",
    COALESCE(SUM(balance) FILTER (WHERE balance > 100000 AND balance <= 1000000),0) AS "100K-1M CASH (b)",
    COALESCE(SUM(balance) FILTER (WHERE balance > 1000000),0) AS ">1M CASH (b)",
    block_date
  FROM txs
  GROUP BY block_date
),

-- Holder breakdown by wallet number
holder_wallet AS (
  SELECT
    ROUND(COUNT(1) FILTER (WHERE balance >= 1 AND balance <= 100)) AS "1-100 CASH (w)",
    ROUND(COUNT(1) FILTER (WHERE balance > 100 AND balance <= 1000)) AS "100-1K CASH (w)",
    ROUND(COUNT(1) FILTER (WHERE balance > 1000 AND balance <= 10000)) AS "1K-10K CASH (w)",
    ROUND(COUNT(1) FILTER (WHERE balance > 10000 AND balance <= 100000)) AS "10K-100K CASH (w)",
    ROUND(COUNT(1) FILTER (WHERE balance > 100000 AND balance <= 1000000)) AS "100K-1M CASH (w)",
    ROUND(COUNT(1) FILTER (WHERE balance > 1000000)) AS ">1M CASH (w)",
    block_date
  FROM txs
  GROUP BY block_date
),

-- Dashboard
dashboard AS (
  SELECT
    s.block_date AS "Date",
    '||' AS "||",
    s.value AS "Total Supply",
    s.value - LAG(s.value) OVER(ORDER BY s.block_date) AS "Supply Change",
    '| ' AS "| ",
    hb."1-100 CASH (b)", hb."100-1K CASH (b)", hb."1K-10K CASH (b)", hb."10K-100K CASH (b)", hb."100K-1M CASH (b)", hb.">1M CASH (b)",
    '|' AS "|",
    h.value AS "Holder",
    h.value - LAG(h.value) OVER(ORDER BY s.block_date) AS "Holder Change",
    ' |' AS " |",
    hw."1-100 CASH (w)", hw."100-1K CASH (w)", hw."1K-10K CASH (w)", hw."10K-100K CASH (w)", hw."100K-1M CASH (w)", hw.">1M CASH (w)"
  FROM supply s
  LEFT JOIN holder h ON s.block_date = h.block_date
  LEFT JOIN holder_wallet hw ON s.block_date = hw.block_date
  LEFT JOIN holder_balance hb ON s.block_date = hb.block_date
  WHERE s.block_date >= DATE('2023-06-01')
  ORDER BY s.block_date DESC
)

SELECT * FROM dashboard
