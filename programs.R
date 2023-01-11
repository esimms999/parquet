# Start at https://arrow-user2022.netlify.app/packages-and-data.html

install.packages(c(
  "arrow", "dplyr", "dbplyr", "duckdb", "fs", "janitor",
  "palmerpenguins", "remotes", "scales", "stringr",
  "lubridate", "tictoc"
))

library(arrow)
library(dplyr)
library(dbplyr)
library(duckdb)
library(stringr)
library(lubridate)
library(palmerpenguins)
library(tictoc)
library(scales)
library(janitor)
library(fs)

install.packages(c("ggplot2", "ggrepel", "sf"))

library(ggplot2)
library(ggrepel)
library(sf)

nyc_taxi <- open_dataset("~/R/parquet/Datasets/nyc-taxi-tiny")
nrow(nyc_taxi)

zone_counts <- nyc_taxi |>
  count(dropoff_location_id) |>
  arrange(desc(n)) |>
  collect()

zone_counts

tic() # start timer
nyc_taxi |>
  count(dropoff_location_id) |>
  arrange(desc(n)) |>
  collect() |>
  invisible() # suppress printing
toc() # stop timer

shapefile <- "~/R/parquet/Datasets/taxi_zones/taxi_zones.shp"
shapedata <- read_sf(shapefile)

shapedata |>
  ggplot(aes(fill = LocationID)) +
  geom_sf(size = .1) +
  theme_bw() +
  theme(panel.grid = element_blank())

left_join(
  x = shapedata,
  y = zone_counts,
  by = c("LocationID" = "dropoff_location_id")
) |>
  ggplot(aes(fill = n)) +
  geom_sf(size = .1) +
  scale_fill_distiller(
    name = "Number of trips",
    limits = c(0, 17000000),
    labels = label_comma(),
    direction = 1
  ) +
  theme_bw() +
  theme(panel.grid = element_blank())

nyc_taxi |>
  filter(year %in% 2017:2021) |>
  group_by(year) |>
  summarize(
    all_trips = n(),
    shared_trips = sum(passenger_count > 1, na.rm = TRUE)
  ) |>
  mutate(pct_shared = shared_trips / all_trips * 100) |>
  collect()

nyc_taxi |>
  filter(year == 2019) |>
  count(month) |>
  collect()

nyc_taxi |>
  filter(year == 2019) |>
  group_by(month) |>
  summarize(longest_trip = max(trip_distance, na.rm = TRUE)) |>
  arrange(month) |>
  collect()

shared_rides <- nyc_taxi |>
  filter(year %in% 2017:2021) |>
  group_by(year) |>
  summarize(
    all_trips = n(),
    shared_trips = sum(passenger_count > 1, na.rm = TRUE)
  ) |>
  mutate(pct_shared = shared_trips / all_trips * 100)

millions <- function(x) x / 10^6
shared_rides |>
  collect() |>
  mutate_at(c("all_trips", "shared_trips"), millions)

nyc_taxi_zones <- "~/R/parquet/Datasets/taxi_zone_lookup.csv" |>
  read_csv_arrow() |>
  clean_names()

nyc_taxi_zones

nyc_taxi_zones_arrow <- arrow_table(nyc_taxi_zones)
nyc_taxi_zones_arrow

nyc_taxi_zones |>
  mutate(
    abbr_zone = zone |>
      str_remove_all("[aeiou' ]") |>
      str_remove_all("/.*"),
    abbr_zone_len = str_length(abbr_zone)
  ) |>
  select(zone, abbr_zone, abbr_zone_len) |>
  arrange(desc(abbr_zone_len))

nyc_taxi_zones_arrow |>
  mutate(
    abbr_zone = zone |>
      str_replace_all("[aeiou' ]", "") |>
      str_replace_all("/.*", "")
  ) |>
  mutate(
    abbr_zone_len = str_length(abbr_zone)
  ) |>
  select(zone, abbr_zone, abbr_zone_len) |>
  arrange(desc(abbr_zone_len)) |>
  collect()

nyc_taxi |>
  filter(
    year == 2022,
    month == 1
  ) |>
  mutate(
    day = day(pickup_datetime),
    weekday = wday(pickup_datetime, label = TRUE),
    hour = hour(pickup_datetime),
    minute = minute(pickup_datetime),
    second = second(pickup_datetime)
  ) |>
  filter(
    hour == 3,
    minute == 14,
    second == 15
  ) |>
  select(
    pickup_datetime, year, month, day, weekday
  ) |>
  collect()

