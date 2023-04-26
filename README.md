
<!-- README.md is generated from README.Rmd. Please edit that file -->

# tidier

<!-- badges: start -->
<!-- badges: end -->

`tidier` package provides ‘[Apache Spark](https://spark.apache.org/)’
style window aggregation for R dataframes via
‘[mutate](https://dplyr.tidyverse.org/reference/mutate.html)’ in
‘[dplyr](https://dplyr.tidyverse.org/index.html)’ flavour.

## Example

``` r
set.seed(101)
air_df = airquality %>%
  # create date column
  dplyr::mutate(date_col = as.Date(paste("1973",
                                         stringr::str_pad(Month,
                                                          width = 2,
                                                          side = "left",
                                                          pad = "0"
                                                          ),
                                         stringr::str_pad(Day,
                                                          width = 2,
                                                          side = "left",
                                                          pad = "0"
                                                          ),
                                         sep = "-"
                                         )
                                  )
                ) %>%
  # create gaps by removing some days
  dplyr::slice_sample(prop = 0.8) %>% 
  tibble::as_tibble()

air_df
#> # A tibble: 122 × 7
#>    Ozone Solar.R  Wind  Temp Month   Day date_col  
#>    <int>   <int> <dbl> <int> <int> <int> <date>    
#>  1    10     264  14.3    73     7    12 1973-07-12
#>  2    NA     127   8      78     6    26 1973-06-26
#>  3    16      77   7.4    82     8     3 1973-08-03
#>  4    14     191  14.3    75     9    28 1973-09-28
#>  5    NA     138   8      83     6    30 1973-06-30
#>  6    NA      98  11.5    80     6    28 1973-06-28
#>  7   122     255   4      89     8     7 1973-08-07
#>  8    47      95   7.4    87     9     5 1973-09-05
#>  9    23     220  10.3    78     9     8 1973-09-08
#> 10    NA     286   8.6    78     6     1 1973-06-01
#> # … with 112 more rows
```

**Create a new column with average temp over last seven days in the same
month**.

``` r
air_df %>% 
  # compute mean temperature over last seven days in the same month
  mutate(avg_temp_over_last_week = mean(Temp, na.rm = TRUE),
         .order_by = Day,
         .by = Month,
         .frame = c(lubridate::days(7), # 7 days before current row
                    lubridate::days(-1) # do not include current row
                    ),
         .index = date_col
         )
#> 
#> Attaching package: 'purrr'
#> The following object is masked from 'package:testthat':
#> 
#>     is_null
#> The following object is masked from 'package:magrittr':
#> 
#>     set_names
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

## Acknowledgements

`tidier` package is deeply indebted to two amazing packages and people
behind it.

1.  [`dplyr`](https://cran.r-project.org/package=dplyr): Hadley wickham
2.  [`slider`](https://cran.r-project.org/package=slider): Davis Vaughan

## Installation

-   dev: `remotes::install_github("talegari/tidier")`
-   cran: `install.packages("tidier")`

------------------------------------------------------------------------
