
test_that("basic mutate", {
  res = iris %>%
    mutate(sl_pl_1 = Sepal.Length + 1)

  testthat::expect(inherits(res, "data.frame"), "not a df")

  res = iris %>%
    mutate_(sl_pl_1 = Sepal.Length + 1)

  testthat::expect(inherits(res, "data.frame"), "not a df")
})

test_that("order_by without by", {
  # simple order_by
  res = iris %>%
    mutate(sl_pl_1 = cumsum(Sepal.Length),
           .order_by = Petal.Width
           ) %>%
    dplyr::arrange(Petal.Width)

  testthat::expect(inherits(res, "data.frame"), "not a df")

  # involved order_by
  res = iris %>%
    mutate(sl_pl_1 = cumsum(Sepal.Length),
            .order_by = c(desc(Petal.Width), Sepal.Length)
            ) %>%
    dplyr::arrange(desc(Petal.Width), Sepal.Length)

  testthat::expect(inherits(res, "data.frame"), "not a df")

  res = iris %>%
    mutate_(sl_pl_1 = cumsum(Sepal.Length),
            .order_by = "Petal.Width"
            ) %>%
    dplyr::arrange(Petal.Width)

  testthat::expect(inherits(res, "data.frame"), "not a df")

  res = iris %>%
    mutate_(sl_pl_1 = cumsum(Sepal.Length),
            .order_by = c("Petal.Width", "Sepal.Length"),
            .desc = c(TRUE, FALSE)
            ) %>%
    dplyr::arrange(desc(Petal.Width), Sepal.Length)

  testthat::expect(inherits(res, "data.frame"), "not a df")
})

test_that("order_by with by", {

  res = iris %>%
    dplyr::mutate(rn = dplyr::row_number()) %>%
    mutate(sl_cumsum = cumsum(Sepal.Length),
           .order_by = c(Petal.Width, rn),
           .by = c(Species)
           ) %>%
    dplyr::select(-rn) %>%
    dplyr::slice_min(n = 3, Petal.Width, by = Species)

  testthat::expect(inherits(res, "data.frame"), "not a df")

  res = iris %>%
    dplyr::mutate(rn = dplyr::row_number()) %>%
    mutate_(sl_cumsum = cumsum(Sepal.Length),
            .order_by = c("Petal.Width", "rn"),
            .by = c("Species")
            ) %>%
    dplyr::select(-rn) %>%
    dplyr::slice_min(n = 3, Petal.Width, by = Species)

  testthat::expect(inherits(res, "data.frame"), "not a df")
})

test_that("order_by, with by, with frame", {

  res = iris %>%
    mutate(sl_mean = mean(Sepal.Length),
            .frame = c(Inf, 0),
            .order_by = Petal.Width,
            .by = c(Petal.Length, Sepal.Width)
            ) %>%
    dplyr::slice_min(n = 3, Petal.Width, by = Species)

  testthat::expect(inherits(res, "data.frame"), "not a df")

  res = iris %>%
    mutate_(sl_mean = mean(Sepal.Length),
            .frame = c(Inf, 0),
            .order_by = "Petal.Width",
            .by = c("Species", "Petal.Length")
            ) %>%
    dplyr::slice_min(n = 3, Petal.Width, by = Species)

  testthat::expect(inherits(res, "data.frame"), "not a df")
})

test_that("order_by, with by, with frame, with index", {

  res = airquality %>%
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
  # compute mean temperature over last seven days in the same month
  mutate(avg_temp_over_last_week = mean(Temp, na.rm = TRUE),
         .order_by = Day,
         .by = Month,
         .frame = c(lubridate::days(7), # 7 days before current row
                    lubridate::days(-1) # do not include current row
                    ),
         .index = date_col
         )
  testthat::expect(inherits(res, "data.frame"), "not a df")

  res = airquality %>%
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
  # compute mean temperature over last seven days in the same month
  mutate_(avg_temp_over_last_week = mean(Temp, na.rm = TRUE),
         .order_by = "Day",
         .by = "Month",
         .frame = c(lubridate::days(7), # 7 days before current row
                    lubridate::days(-1) # do not include current row
                    ),
         .index = "date_col"
         )
  testthat::expect(inherits(res, "data.frame"), "not a df")
})

test_that("order_by, with by, with frame, same column name", {

  res = iris %>%
    mutate(Sepal.Length = mean(Sepal.Length),
            .frame = c(Inf, 0),
            .order_by = Petal.Width,
            .by = c(Petal.Length, Sepal.Width)
            ) %>%
    dplyr::slice_min(n = 3, Petal.Width, by = Species)

  testthat::expect(inherits(res, "data.frame"), "not a df")

  res = iris %>%
    mutate_(Sepal.Length = mean(Sepal.Length),
            .frame = c(Inf, 0),
            .order_by = "Petal.Width",
            .by = c("Species", "Petal.Length")
            ) %>%
    dplyr::slice_min(n = 3, Petal.Width, by = Species)

  testthat::expect(inherits(res, "data.frame"), "not a df")
})

test_that("order_by, with by, with frame, same column name", {

  res = iris %>%
    mutate(Sepal.Length = mean(Sepal.Length),
            .frame = c(Inf, 0),
            .order_by = Petal.Width,
            .by = c(Petal.Length, Sepal.Width)
            ) %>%
    dplyr::slice_min(n = 3, Petal.Width, by = Species)

  testthat::expect(inherits(res, "data.frame"), "not a df")

  res = iris %>%
    mutate_(Sepal.Length = mean(Sepal.Length),
            .frame = c(Inf, 0),
            .order_by = "Petal.Width",
            .by = c("Species", "Petal.Length")
            ) %>%
    dplyr::slice_min(n = 3, Petal.Width, by = Species)

  testthat::expect(inherits(res, "data.frame"), "not a df")
})

test_that("order_by, with by, with frame, with index, with same column", {

  res = airquality %>%
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
  # compute mean temperature over last seven days in the same month
  mutate(Temp = mean(Temp, na.rm = TRUE),
         .order_by = Day,
         .by = Month,
         .frame = c(lubridate::days(7), # 7 days before current row
                    lubridate::days(-1) # do not include current row
                    ),
         .index = date_col
         )
  testthat::expect(inherits(res, "data.frame"), "not a df")

  res = airquality %>%
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
  # compute mean temperature over last seven days in the same month
  mutate_(Temp = mean(Temp, na.rm = TRUE),
         .order_by = "Day",
         .by = "Month",
         .frame = c(lubridate::days(7), # 7 days before current row
                    lubridate::days(-1) # do not include current row
                    ),
         .index = "date_col"
         )
  testthat::expect(inherits(res, "data.frame"), "not a df")
})

test_that("compare mutate df vs sb", {

  res_df =
    airquality %>%
    # create date column as character
    dplyr::mutate(date_col =
                    as.character(lubridate::make_date(1973, Month, Day))
                  ) %>%
    tibble::as_tibble() %>%
    mutate(avg_temp = mean(Temp),
           .by = Month,
           .order_by = c(date_col),
           .frame = c(3, 3)
           ) %>%
    dplyr::select(Ozone, Solar.R, Wind, Temp, Month, Day, date_col, avg_temp)

  res_db =
    airquality %>%
    # create date column as character
    dplyr::mutate(date_col =
                    as.character(lubridate::make_date(1973, Month, Day))
                  ) %>%
    tibble::as_tibble() %>%
    # as `tbl_lazy`
    dbplyr::memdb_frame() %>%
    mutate(avg_temp = mean(Temp),
           .by = Month,
           .order_by = c(date_col),
           .frame = c(3, 3)
           ) %>%
    dplyr::collect() %>%
    dplyr::select(Ozone, Solar.R, Wind, Temp, Month, Day, date_col, avg_temp)

  res_db_ =
    airquality %>%
    # create date column as character
    dplyr::mutate(date_col =
                    as.character(lubridate::make_date(1973, Month, Day))
                  ) %>%
    tibble::as_tibble() %>%
    # as `tbl_lazy`
    dbplyr::memdb_frame() %>%
    mutate_(avg_temp = mean(Temp),
           .by = "Month",
           .order_by = "date_col",
           .frame = c(3, 3)
           ) %>%
    dplyr::collect() %>%
    dplyr::select(Ozone, Solar.R, Wind, Temp, Month, Day, date_col, avg_temp)

  testthat::expect_true(all.equal(res_df, res_db))
  testthat::expect_true(all.equal(res_df, res_db_))
})
