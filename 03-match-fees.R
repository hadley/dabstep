library(nanoparquet)
library(dplyr)
library(lubridate)

payments <- read_parquet("data/payments.parquet")
fees <- arrow::read_parquet("data/fees.parquet")
merchants <- read_parquet("data/merchants.parquet")
merchant_months <- read_parquet("data/merchant_months.parquet")

# Join merchant info and monthly aggregates onto payments
payments <- payments |>
  left_join(merchants, by = "merchant") |>
  left_join(merchant_months, by = c("merchant", "year", "month"))

# Compute intracountry
payments$intracountry <- payments$issuing_country == payments$acquirer_country

# Match each payment to fee rules
fee_id <- integer(nrow(payments))
for (i in seq_len(nrow(payments))) {
  p <- payments[i, ]
  matched <- which(
    fees$card_scheme == p$card_scheme &
    mapply(\(at) length(at) == 0 || p$account_type %in% at, fees$account_type) &
    mapply(\(mc) length(mc) == 0 || p$merchant_category_code %in% mc, fees$merchant_category_code) &
    mapply(\(a) length(a) == 0 || p$aci %in% a, fees$aci) &
    (is.na(fees$is_credit) | fees$is_credit == p$is_credit) &
    (is.na(fees$intracountry) | fees$intracountry == p$intracountry) &
    (is.na(fees$capture_delay) | fees$capture_delay == p$capture_delay) &
    p$fraud_percent >= fees$fraud_percent_min & p$fraud_percent < fees$fraud_percent_max &
    p$total_volume >= fees$volume_min & p$total_volume < fees$volume_max
  )
  fee_id[i] <- if (length(matched) == 1) matched else NA_integer_
}

payments$fee_id <- fees$ID[fee_id]
