
<!-- README.md is generated from README.Rmd. Please edit that file -->

# tidier

<!-- badges: start -->
<!-- badges: end -->

`tidier` package provides ‘[Apache Spark](https://spark.apache.org/)’
style window aggregation for R dataframes via
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
                 .by = Month,
                 .frame = c(lubridate::days(7), # 7 days before current row
                            lubridate::days(-1) # do not include current row
                            ),
                 .index = date_col
                 )
#> # A tibble: 122 × 8
#>    Month Ozone Solar.R  Wind  Temp   Day date_col   avg_temp_over_last_week
#>    <int> <int>   <int> <dbl> <int> <int> <date>                       <dbl>
#>  1     6    NA     332  13.8    80    14 1973-06-14                    87.2
#>  2     5    28      NA  14.9    66     6 1973-05-06                    66  
#>  3     5     6      78  18.4    57    18 1973-05-18                    65.2
#>  4     8    45     212   9.7    79    24 1973-08-24                    76.5
#>  5     5    36     118   8      72     2 1973-05-02                   NaN  
#>  6     9    24     238  10.3    68    19 1973-09-19                    73  
#>  7     9    16     201   8      82    20 1973-09-20                    71.7
#>  8     6    NA     186   9.2    84     4 1973-06-04                    72.5
#>  9     8    78      NA   6.9    86     4 1973-08-04                    81.3
#> 10     8   168     238   3.4    81    25 1973-08-25                    76.5
#> # … with 112 more rows
```

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

2.  [`tidypyspark`](https://talegari.github.io/tidypyspark/_build/html/index.html)
    python package implements `mutate` style window computation API for
    pyspark.

## Installation

-   dev: `remotes::install_github("talegari/tidier")`
-   cran: `install.packages("tidier")`

## Acknowledgements

`tidier` package is deeply indebted to two amazing packages and people
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
