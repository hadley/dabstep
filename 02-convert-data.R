library(nanoparquet)

dir.create("data", showWarnings = FALSE)

# Acquirer countries -----------------------------------------------------------

acquirer_countries <- read.csv("raw-data/acquirer_countries.csv")
acquirer_countries$X <- NULL
acquirer_countries <- acquirer_countries[order(acquirer_countries$acquirer), ]
write_parquet(acquirer_countries, "data/acquirer_countries.parquet")

# Merchant category codes ------------------------------------------------------

merchant_category_codes <- read.csv("raw-data/merchant_category_codes.csv")
merchant_category_codes$X <- NULL
merchant_category_codes <- merchant_category_codes[
  order(merchant_category_codes$mcc),
]
write_parquet(merchant_category_codes, "data/merchant_category_codes.parquet")

# Merchants & merchant-acquirers -----------------------------------------------

merchant_data <- jsonlite::fromJSON("raw-data/merchant_data.json")
merchants <- merchant_data[, c("merchant", "capture_delay", "merchant_category_code", "account_type")]
# Convert numeric days to fee-matching ranges
days <- suppressWarnings(as.numeric(merchants$capture_delay))
merchants$capture_delay[!is.na(days)] <- ifelse(
  days[!is.na(days)] < 3, "<3",
  ifelse(days[!is.na(days)] <= 5, "3-5", ">5")
)
merchants <- merchants[order(merchants$merchant), ]
write_parquet(merchants, "data/merchants.parquet")

merchant_acquirers <- data.frame(
  merchant = rep(merchant_data$merchant, lengths(merchant_data$acquirer)),
  acquirer = unlist(merchant_data$acquirer)
)
merchant_acquirers <- merchant_acquirers[order(merchant_acquirers$merchant, merchant_acquirers$acquirer), ]
write_parquet(merchant_acquirers, "data/merchant_acquirers.parquet")

# Fees -------------------------------------------------------------------------

fees <- jsonlite::fromJSON("raw-data/fees.json")
fees$is_credit <- as.logical(fees$is_credit)
fees$intracountry <- as.logical(fees$intracountry)
fees$merchant_category_code <- lapply(fees$merchant_category_code, as.integer)
fees$account_type <- lapply(fees$account_type, as.character)
fees$aci <- lapply(fees$aci, as.character)

# Parse range strings into min/max columns
parse_range <- function(x, parse_val = as.numeric) {
  min <- rep(-Inf, length(x))
  max <- rep(Inf, length(x))
  for (i in seq_along(x)) {
    if (is.na(x[i])) next
    val <- gsub("%", "", x[i])
    if (grepl("^<", val)) {
      max[i] <- parse_val(sub("^<", "", val))
    } else if (grepl("^>", val)) {
      min[i] <- parse_val(sub("^>", "", val))
    } else {
      parts <- strsplit(val, "-")[[1]]
      min[i] <- parse_val(parts[1])
      max[i] <- parse_val(parts[2])
    }
  }
  data.frame(min = min, max = max)
}

parse_volume_val <- function(x) {
  x <- gsub("k", "e3", x)
  x <- gsub("m", "e6", x)
  as.numeric(x)
}

fraud <- parse_range(fees$monthly_fraud_level)
fees$fraud_level_min <- fraud$min
fees$fraud_level_max <- fraud$max

volume <- parse_range(fees$monthly_volume, parse_volume_val)
fees$volume_min <- volume$min
fees$volume_max <- volume$max

# Convert capture_delay to enum matching merchant values
fees$capture_delay <- factor(fees$capture_delay,
  levels = c("immediate", "<3", "3-5", ">5", "manual")
)

fees$monthly_fraud_level <- NULL
fees$monthly_volume <- NULL

arrow::write_parquet(fees, "data/fees.parquet")

# Payments ---------------------------------------------------------------------

payments <- read.csv("raw-data/payments.csv")
bool_cols <- grep("^(is_|has_)", names(payments), value = TRUE)
payments[bool_cols] <- lapply(payments[bool_cols], \(x) x == "True")

# Match payments to fee rules --------------------------------------------------

match_fees <- function(payments, fees, merchants) {
  # Join merchant info onto payments
  payments <- merge(payments, merchants, by = "merchant")

  # Compute intracountry
  payments$intracountry <- payments$issuing_country == payments$acquirer_country

  # Compute monthly aggregates per merchant
  payments$month <- ceiling(payments$day_of_year / 30.44)
  monthly <- aggregate(
    cbind(total_volume = eur_amount, fraud_volume = eur_amount * has_fraudulent_dispute) ~ merchant + month,
    data = payments,
    FUN = sum
  )
  monthly$fraud_level <- monthly$fraud_volume / monthly$total_volume * 100
  payments <- merge(payments, monthly, by = c("merchant", "month"))

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
      p$fraud_level >= fees$fraud_level_min & p$fraud_level < fees$fraud_level_max &
      p$total_volume >= fees$volume_min & p$total_volume < fees$volume_max
    )
    fee_id[i] <- if (length(matched) == 1) matched else NA_integer_
  }

  payments$fee_id <- fees$ID[fee_id]
  payments
}

write_parquet(payments, "data/payments.parquet", compression = "gzip")
