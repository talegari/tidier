
<!-- README.md is generated from README.Rmd. Please edit that file -->

# tidier

<!-- badges: start -->

[![CRAN
status](https://www.r-pkg.org/badges/version/tidier)](https://CRAN.R-project.org/package=tidier)
[![R-CMD-check](https://github.com/talegari/tidier/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/talegari/tidier/actions/workflows/R-CMD-check.yaml)
[![](https://img.shields.io/badge/devel%20version-0.2.0-blue.svg)](https://github.com/talegari/tidier)

<!-- badges: end -->

`tidier` package provides ‘[Apache Spark](https://spark.apache.org/)’
style window aggregation for R dataframes and remote `dbplyr` tbls via
‘[mutate](https://dplyr.tidyverse.org/reference/mutate.html)’ in
‘[dplyr](https://dplyr.tidyverse.org/index.html)’ flavour.

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
                 .order_by = Day,
                 .by       = Month,
                 .frame    = c(lubridate::days(7), # 7 days before current row
                               lubridate::days(-1) # do not include current row
                               ),
                 .index    = date_col
                 )
#> # A tibble: 122 × 8
#>    Month Ozone Solar.R  Wind  Temp   Day date_col   avg_temp_over_last_week
#>    <int> <int>   <int> <dbl> <int> <int> <date>                       <dbl>
#>  1     6    NA     286   8.6    78     1 1973-06-01                   NaN  
#>  2     6    NA     242  16.1    67     3 1973-06-03                    78  
#>  3     6    NA     186   9.2    84     4 1973-06-04                    72.5
#>  4     6    NA     264  14.3    79     6 1973-06-06                    76.3
#>  5     6    29     127   9.7    82     7 1973-06-07                    77  
#>  6     6    NA     273   6.9    87     8 1973-06-08                    78  
#>  7     6    NA     259  10.9    93    11 1973-06-11                    83  
#>  8     6    NA     250   9.2    92    12 1973-06-12                    85.2
#>  9     6    23     148   8      82    13 1973-06-13                    86.6
#> 10     6    NA     332  13.8    80    14 1973-06-14                    87.2
#> # ℹ 112 more rows
```

## Features

- `mutate` supports
  - `.by` (group by),
  - `.order_by` (order by),
  - `.frame` (endpoints of window frame),
  - `.index` (identify index column like date column, in df version
    only),
  - `.complete` (whether to compute over incomplete window, in df
    version only).
- `mutate` automatically uses a future backend (via
  [`furrr`](https://furrr.futureverse.org/), in df version only).

## Motivation

This implementation is inspired by Apache Spark’s
[`windowSpec`](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.Column.over.html?highlight=windowspec)
class with
[`rangeBetween`](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.WindowSpec.rangeBetween.html)
and
[`rowsBetween`](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.WindowSpec.rowsBetween.html).

## Ecosystem

1.  [`dbplyr`](https://dbplyr.tidyverse.org/) implements this via
    [`dbplyr::win_over`](https://dbplyr.tidyverse.org/reference/win_over.html?q=win_over#null)
    enabling [`sparklyr`](https://spark.rstudio.com/) users to write
    window computations. Also see,
    [`dbplyr::window_order`/`dbplyr::window_frame`](https://dbplyr.tidyverse.org/reference/window_order.html?q=window_fr#ref-usage).
    `tidier`’s `mutate` wraps this functionality via uniform syntax
    across dataframes and remote tbls.

2.  [`tidypyspark`](https://talegari.github.io/tidypyspark/_build/html/index.html)
    python package implements `mutate` style window computation API for
    pyspark.

## Installation

- dev: `remotes::install_github("talegari/tidier")`
- cran: `install.packages("tidier")`

## Acknowledgements

`tidier` package is deeply indebted to three amazing packages and people
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

3.  [`dbplyr`](https://cran.r-project.org/package=dbplyr):

<!-- -->

    Wickham H, Girlich M, Ruiz E (2023). _dbplyr: A 'dplyr' Back End
      for Databases_. R package version 2.3.2,
      <https://CRAN.R-project.org/package=dbplyr>.
