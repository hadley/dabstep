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

payments <- read.csv("raw-data/payments.csv")
bool_cols <- grep("^(is_|has_)", names(payments), value = TRUE)
payments[bool_cols] <- lapply(payments[bool_cols], \(x) x == "True")
write_parquet(payments, "data/payments.parquet", compression = "gzip")
