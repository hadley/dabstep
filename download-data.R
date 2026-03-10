library(fs)

dir_create("raw-data")

base_url <- "https://huggingface.co/datasets/adyen/DABstep/resolve/main/data/context/"

files <- c(
  "acquirer_countries.csv",
  "fees.json",
  "manual.md",
  "merchant_category_codes.csv",
  "merchant_data.json",
  "payments-readme.md",
  "payments.csv"
)

for (file in files) {
  dest <- path("raw-data", file)
  if (file_exists(dest)) {
    message("Skipping ", file, " (already exists)")
    next
  }
  message("Downloading ", file)
  download.file(
    url = paste0(base_url, file),
    destfile = dest,
    mode = "wb"
  )
}

message("Done!")
