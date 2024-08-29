-- SPDX-FileCopyrightText: 2024-present Matthew Hall <matt.hall0963@gmail.com>
-- SPDX-License-Identifier: MPL-2.0

-- Source: https://github.com/mhall0963/dune-dashboards
-- Date: 2024-08-29


WITH burn_txs AS (
  SELECT
    date_trunc('day', evt_block_time) AS date,
    value / 1e18 AS amount
  FROM erc20_base.evt_transfer
  WHERE
    contract_address = 0x548f93779fbc992010c07467cbaf329dd5f059b7
    AND to = 0x000000000000000000000000000000000000dEaD
),

daily_burns AS (
  SELECT
    date,
    SUM(amount) AS burned
  FROM burn_txs
  GROUP BY date
  ORDER BY date
),

cumulative_burn AS (
  SELECT
    date,
    SUM(burned) OVER (ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_burn
  FROM daily_burns
)

SELECT
  date,
  total_burn,
  10000000 - total_burn AS total_supply
FROM cumulative_burn
WHERE date >= DATE('2024-06-01')
ORDER BY date ASC
