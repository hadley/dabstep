# Implied domain knowledge from dev.json

Insights extracted by comparing questions, guidelines, and expected answers
in the 10-task dev set against the actual data.

## Terminology

* **"Nth of the year"** means `day_of_year = N`, NOT month N.
  Verified: task 1681 asks "the 10th of the year 2023" and the answer matches
  `day_of_year == 10` (which falls in January), not `month == 10`.

* **"Relative fee"** refers to the `rate` field in the fees table
  (the variable component), as opposed to `fixed_amount` (the absolute/fixed
  component). Task 1871 uses "relative fee of the fee with ID=384 changed to 1"
  meaning `rate = 1`.

* **"Not Applicable"** is used when the question asks about a concept that
  doesn't exist in the data — not just when an entity is missing. Task 70 asks
  about a "high-fraud rate fine" for a merchant that exists, but "fines" are not
  a concept in the data (only fees), so the answer is "Not Applicable".

## Fee matching rules

* **Empty list = matches all values; null scalar = matches all values.**
  Confirmed by task 1464: fee IDs matching `account_type = R` and `aci = B`
  include rules where `account_type` is empty (matches all) or contains "R",
  AND `aci` is empty or contains "B".

* **fraud_percent range**: use `>=` for `fraud_percent_min` (inclusive) and
  `<` for `fraud_percent_max` (exclusive), per the data dictionary. In practice
  `between()` (inclusive both ends) also gives correct results for the dev set
  since no merchant's fraud_percent lands exactly on a boundary.

* **"Applicable fee IDs"** for a merchant in a time period = the union of all
  fee rule IDs that match ANY transaction for that merchant in that period.
  Verified with tasks 1681 and 1753.

## Fee calculations

* **Fee formula**: `fixed_amount + rate * eur_amount / 10000` (per data dictionary).

* **"Average fee a card scheme would charge for X EUR"**: average is computed
  across all matching **fee rules** (not across actual transactions). Filter the
  fees table by the stated constraints, compute `fixed_amount + rate * X / 10000`
  for each matching rule, then take `mean()`. Verified with tasks 1273 and 1305.

* **MCC matching by description**: when a question references an MCC by its
  description (e.g. "Eating Places and Restaurants"), look up the numeric MCC
  code from `merchant_category_codes.parquet` first.

## Data scope

* **Only year 2023** exists in the payments data, so references to months
  without a year (e.g. "in January", "in March") mean 2023.

## Task 1871 (delta calculation) — unresolved

Task 1871 asks about the delta in total fees if fee 384's rate changed to 1.
The expected answer is `-0.94000000000005`. Several approaches were tried
(cheapest fee per transaction, all-fees-summed, specificity-based, average)
but none reproduced the exact answer. The closest was the "cheapest fee with
re-evaluation" approach at `-0.94119200000000`. The exact deduplication /
assignment strategy for multi-matching fee rules remains unclear from the
dev set alone.

## Task 2697 (ACI incentivization) — format note

The guidelines say the answer format is `{card_scheme}:{fee}` but the actual
answer is `E:13.57` where `E` is an ACI code, not a card scheme. The true
format is `{ACI}:{fee}`.
