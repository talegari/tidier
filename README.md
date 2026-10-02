
<!-- README.md is generated from README.Rmd. Please edit that file -->

# tidier

<!-- badges: start -->

[![CRAN
status](https://www.r-pkg.org/badges/version/tidier)](https://CRAN.R-project.org/package=tidier)
[![R-CMD-check](https://github.com/talegari/tidier/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/talegari/tidier/actions/workflows/R-CMD-check.yaml)

<!-- badges: end -->

`tidier` package provides [Apache Spark](https://spark.apache.org/)
style window aggregation for R dataframes via
[mutate](https://dplyr.tidyverse.org/reference/mutate.html) in
[dplyr](https://dplyr.tidyverse.org/index.html) flavor.

## Example

**Create a new column with average temp over last seven days in the same
month**.

``` r
set.seed(101)
airquality |>
  # create date column
  dplyr::mutate(date_col = lubridate::make_date(1973, Month, Day)) |>
  # create gaps by removing some days
  dplyr::slice_sample(prop = 0.8) |> 
  # compute mean temperature over last seven days in the same month
  tidier::mutate(avg_temp_over_last_week = mean(Temp, na.rm = TRUE),
                 .order_by = date_col,
                 .by       = Month,
                 .frame    = range_between(lubridate::days(7), # 7 days before current row
                                           lubridate::days(-1) # do not include current row
                                           )
                 )
#> # A tibble: 122 × 8
#>    Ozone Solar.R  Wind  Temp Month   Day date_col   avg_temp_over_last_week
#>    <int>   <int> <dbl> <int> <int> <int> <date>                       <dbl>
#>  1    10     264  14.3    73     7    12 1973-07-12                    85.5
#>  2    NA     127   8      78     6    26 1973-06-26                    75.4
#>  3    16      77   7.4    82     8     3 1973-08-03                    81  
#>  4    14     191  14.3    75     9    28 1973-09-28                    71.8
#>  5    NA     138   8      83     6    30 1973-06-30                    76.6
#>  6    NA      98  11.5    80     6    28 1973-06-28                    75.8
#>  7   122     255   4      89     8     7 1973-08-07                    83.7
#>  8    47      95   7.4    87     9     5 1973-09-05                    92.5
#>  9    23     220  10.3    78     9     8 1973-09-08                    90.7
#> 10    NA     286   8.6    78     6     1 1973-06-01                   NaN  
#> # ℹ 112 more rows
```

## Features

- `mutate` supports
  - `.by` (group by),
  - `.order_by` (order by),
  - `.frame` (window frame defined by `rows_between` or
    `range_between`),
  - `.complete` (whether to compute over incomplete window).
- `tidier::mutate` is single-threaded. For heavy parallelization across
  many groups, users can combine `tidyr::nest()` with parallel map
  (e.g. `furrr::future_map()`) and `tidyr::unnest()`.

## Motivation

This implementation is inspired by Apache Spark’s `windowspec`,
`rows between` and `range between`.

## Installation

- dev: `remotes::install_github("talegari/tidier")`
- cran: `install.packages("tidier")`

## Acknowledgements

`tidier` package is deeply indebted to the amazing packages and people
behind it.

1.  [`dplyr`](https://cran.r-project.org/package=dplyr):

<!-- -->

    Wickham H, François R, Henry L, Müller K, Vaughan D (2023). _dplyr: A
    Grammar of Data Manipulation_. R package version 1.1.0,
    <https://CRAN.R-project.org/package=dplyr>.

2.  [`slider`](https://cran.r-project.org/package=slider):

<!-- -->

    Vaughan D (2021). _slider: Sliding Window Functions_. R package
    version 0.2.2, <https://CRAN.R-project.org/package=slider>.
