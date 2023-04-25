
test_that("basic mutate", {
  res = iris %>%
    mutate(sl_pl_1 = Sepal.Length + 1)

  testthat::expect(inherits(res, "data.frame"))

  res = iris %>%
    mutate_(sl_pl_1 = Sepal.Length + 1)

  testthat::expect(inherits(res, "data.frame"))
})

test_that("order_by without by", {
  # simple order_by
  res = iris %>%
    mutate(sl_pl_1 = cumsum(Sepal.Length),
           .order_by = Petal.Width
           ) %>%
    dplyr::arrange(Petal.Width)

  testthat::expect(inherits(res, "data.frame"))

  # involved order_by
  res = iris %>%
    mutate(sl_pl_1 = cumsum(Sepal.Length),
            .order_by = c(desc(Petal.Width), Sepal.Length)
            ) %>%
    dplyr::arrange(desc(Petal.Width), Sepal.Length)

  testthat::expect(inherits(res, "data.frame"))

  res = iris %>%
    mutate_(sl_pl_1 = cumsum(Sepal.Length),
            .order_by = "Petal.Width"
            ) %>%
    dplyr::arrange(Petal.Width)

  testthat::expect(inherits(res, "data.frame"))

  res = iris %>%
    mutate_(sl_pl_1 = cumsum(Sepal.Length),
            .order_by = c("Petal.Width", "Sepal.Length"),
            .desc = c(TRUE, FALSE)
            ) %>%
    dplyr::arrange(desc(Petal.Width), Sepal.Length)

  testthat::expect(inherits(res, "data.frame"))
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

  testthat::expect(inherits(res, "data.frame"))

  res = iris %>%
    dplyr::mutate(rn = dplyr::row_number()) %>%
    mutate_(sl_cumsum = cumsum(Sepal.Length),
            .order_by = c("Petal.Width", "rn"),
            .by = c("Species")
            ) %>%
    dplyr::select(-rn) %>%
    dplyr::slice_min(n = 3, Petal.Width, by = Species)

  testthat::expect(inherits(res, "data.frame"))
})

test_that("order_by, with by, with frame", {

  res = iris %>%
    mutate(sl_mean = mean(Sepal.Length),
            .frame = c(Inf, 0),
            .order_by = Petal.Width,
            .by = c(Petal.Length, Sepal.Width)
            ) %>%
    dplyr::slice_min(n = 3, Petal.Width, by = Species)

  testthat::expect(inherits(res, "data.frame"))

  res = iris %>%
    mutate_(sl_mean = mean(Sepal.Length),
            .frame = c(Inf, 0),
            .order_by = "Petal.Width",
            .by = c("Species", "Petal.Length")
            ) %>%
    dplyr::slice_min(n = 3, Petal.Width, by = Species)

  testthat::expect(inherits(res, "data.frame"))
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
  testthat::expect(inherits(res, "data.frame"))

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
  testthat::expect(inherits(res, "data.frame"))
})
