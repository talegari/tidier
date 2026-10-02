test_that("basic mutate", {
  res = iris %>%
    mutate(sl_pl_1 = Sepal.Length + 1)

  expect_true(inherits(res, "data.frame"))
  expect_equal(res$sl_pl_1, iris$Sepal.Length + 1)
})

test_that("order_by without by", {
  res = iris %>%
    mutate(sl_cumsum = cumsum(Sepal.Length),
           .order_by = Petal.Width
           )

  expect_true(inherits(res, "data.frame"))
})

test_that("multiple order by columns when frame is not specified", {
  res = iris %>%
    mutate(sl_cumsum = cumsum(Sepal.Length),
           .order_by = c(Petal.Width, Sepal.Length),
           .by = Species
           )

  expect_true(inherits(res, "data.frame"))
})

test_that("order_by should be a single column when range_between frame is specified", {
  expect_error(
    mtcars %>%
      mutate(s = sum(mpg),
             .by = cyl,
             .order_by = c(gear, qsec),
             .frame = range_between(5, 4)
             )
  )
})

test_that("parallel execution across many groups using tidyr::nest and furrr", {

  future::plan(future::sequential) # use sequential in tests for reliability

  res = iris %>%
    tidyr::nest(.by = Species) %>%
    dplyr::mutate(
      data = furrr::future_map(data, ~ .x %>%
                          mutate(
                            sl_mean = mean(Sepal.Length),
                            .order_by = Petal.Width,
                            .frame = rows_between(2, 2)
                          )
                       )
    ) %>%
    tidyr::unnest(data)

  expect_true(inherits(res, "data.frame"))
  expect_true("sl_mean" %in% colnames(res))
})

test_that("compare mutate output with duckdb backend", {

  con = DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  dplyr::copy_to(con,
                 mtcars %>% dplyr::mutate(rn = dplyr::row_number()),
                 name = "mtcars_tbl",
                 overwrite = TRUE
                 )

  # Test 1: rows_between (1 preceding and 1 following)
  query1 = "
    select *,
           sum(mpg) over (partition by cyl
                          order by gear
                          rows between 1 preceding and 1 following) as s
    from mtcars_tbl
    order by rn
  "
  res_db1 = DBI::dbGetQuery(con, query1) %>% tibble::as_tibble()

  res_tidier1 = mtcars %>%
    mutate(s = sum(mpg),
           .by = cyl,
           .order_by = gear,
           .frame = rows_between(1, 1)
           ) %>%
    dplyr::mutate(rn = dplyr::row_number()) %>%
    dplyr::select(names(res_db1))

  expect_equal(res_tidier1$s, res_db1$s)

  # Test 2: rows_between (unbounded preceding and 1 following)
  query2 = "
    select *,
           sum(mpg) over (partition by cyl
                          order by gear
                          rows between unbounded preceding and 1 following) as s
    from mtcars_tbl
    order by rn
  "
  res_db2 = DBI::dbGetQuery(con, query2) %>% tibble::as_tibble()

  res_tidier2 = mtcars %>%
    mutate(s = sum(mpg),
           .by = cyl,
           .order_by = gear,
           .frame = rows_between(Inf, 1)
           ) %>%
    dplyr::mutate(rn = dplyr::row_number()) %>%
    dplyr::select(names(res_db2))

  expect_equal(res_tidier2$s, res_db2$s)

  # Test 3: range_between (5 preceding and 4 following on numeric qsec)
  query3 = "
    select *,
           sum(mpg) over (partition by cyl
                          order by qsec
                          range between 5 preceding and 4 following) as s
    from mtcars_tbl
    order by rn
  "
  res_db3 = DBI::dbGetQuery(con, query3) %>% tibble::as_tibble()

  res_tidier3 = mtcars %>%
    mutate(s = sum(mpg),
           .by = cyl,
           .order_by = qsec,
           .frame = range_between(5, 4)
           ) %>%
    dplyr::mutate(rn = dplyr::row_number()) %>%
    dplyr::select(names(res_db3))

  expect_equal(res_tidier3$s, res_db3$s)
})
