library(nanoparquet)
library(dplyr)
library(purrr)

payments <- read_parquet("data/payments.parquet")
fees <- arrow::read_parquet("data/fees.parquet")
merchants <- read_parquet("data/merchants.parquet")
merchant_months <- read_parquet("data/merchant_months.parquet")

# Enrich payments with merchant info and monthly aggregates
payments <- payments |>
  left_join(merchants, by = "merchant") |>
  left_join(merchant_months, by = c("merchant", "year", "month")) |>
  mutate(intracountry = issuing_country == acquirer_country)

# Join fees on card_scheme + range conditions
possible <- payments |>
  inner_join(
    fees,
    join_by(
      card_scheme,
      between(fraud_percent, fraud_percent_min, fraud_percent_max)
    ),
    relationship = "many-to-many",
    suffix = c("", ".fee")
  )

matched <- possible |>
  mutate(
    ok_is_credit = is.na(is_credit.fee) | is_credit == is_credit.fee,
    ok_intracountry = is.na(intracountry.fee) |
      intracountry == intracountry.fee,
    ok_capture_delay = is.na(capture_delay.fee) |
      capture_delay == capture_delay.fee,
    ok_monthly_volume = is.na(monthly_volume.fee) |
      monthly_volume == monthly_volume.fee,
    ok_account_type = map2_lgl(account_type.fee, account_type, \(at, val) {
      length(at) == 0 || val %in% at
    }),
    ok_mcc = map2_lgl(
      merchant_category_code.fee,
      merchant_category_code,
      \(mc, val) length(mc) == 0 || val %in% mc
    ),
    ok_aci = map2_lgl(aci.fee, aci, \(a, val) length(a) == 0 || val %in% a)
  ) |>
  filter(
    ok_is_credit,
    ok_intracountry,
    ok_capture_delay,
    ok_monthly_volume,
    ok_account_type,
    ok_mcc,
    ok_aci
  )
