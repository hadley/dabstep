library(nanoparquet)

dir.create("data", showWarnings = FALSE)

acquirer_countries <- read.csv("raw-data/acquirer_countries.csv")
acquirer_countries$X <- NULL
acquirer_countries <- acquirer_countries[order(acquirer_countries$acquirer), ]
write_parquet(acquirer_countries, "data/acquirer_countries.parquet")

merchant_category_codes <- read.csv("raw-data/merchant_category_codes.csv")
merchant_category_codes$X <- NULL
merchant_category_codes <- merchant_category_codes[
  order(merchant_category_codes$mcc),
]
write_parquet(merchant_category_codes, "data/merchant_category_codes.parquet")

merchant_data <- jsonlite::fromJSON("raw-data/merchant_data.json")
merchants <- merchant_data[, c("merchant", "capture_delay", "merchant_category_code", "account_type")]
merchants <- merchants[order(merchants$merchant), ]
write_parquet(merchants, "data/merchants.parquet")

merchant_acquirers <- data.frame(
  merchant = rep(merchant_data$merchant, lengths(merchant_data$acquirer)),
  acquirer = unlist(merchant_data$acquirer)
)
merchant_acquirers <- merchant_acquirers[order(merchant_acquirers$merchant, merchant_acquirers$acquirer), ]
write_parquet(merchant_acquirers, "data/merchant_acquirers.parquet")

payments <- read.csv("raw-data/payments.csv")
bool_cols <- grep("^(is_|has_)", names(payments), value = TRUE)
payments[bool_cols] <- lapply(payments[bool_cols], \(x) x == "True")
write_parquet(payments, "data/payments.parquet", compression = "gzip")
