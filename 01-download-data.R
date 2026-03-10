# The raw data is not saved in git to keep the repo relatively small
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

dir.create("raw-data", showWarnings = FALSE)
for (file in files) {
  dest <- file.path("raw-data", file)
  if (file.exists(dest)) {
    message("Skipping ", file)
  } else {
    message("Downloading ", file)
    download.file(url = paste0(base_url, file), destfile = dest, mode = "wb")
  }
}

message("Done!")
