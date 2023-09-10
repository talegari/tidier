# mutate_ ----------------------------------------------------------------------

#' @name mutate_
#' @title Drop-in replacement for \code{\link[dplyr]{mutate}}
#' @description Provides supercharged version of \code{\link[dplyr]{mutate}}
#'   with `group_by`, `order_by` and aggregation over arbitrary window frame
#'   around a row for dataframes and lazy (remote) `tbl`s of class `tbl_lazy`.
#' @seealso mutate
#' @details A window function returns a value for every input row of a dataframe
#'   or `lazy_tbl` based on a group of rows (frame) in the neighborhood of the
#'   input row. This function implements computation over groups (`partition_by`
#'   in SQL) in a predefined order (`order_by` in SQL) across a neighborhood of
#'   rows (frame) defined by a (up, down) where
#'
#'   - up/down are number of rows before and after the corresponding row
#'
#'   - up/down are interval objects (ex: `c(days(2), days(1))`).
#'   Interval objects are currently supported for dataframe only. (not
#'   `tbl_lazy`)
#'
#'   This implementation is inspired by spark's [window
#'   API](https://www.databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html).
#'
#'   **Implementation Details**:
#'
#'   For dataframe input:
#'
#'   - Iteration per row over the window is implemented using the versatile
#'   [`slider`](https://cran.r-project.org/package=slider).
#'
#'   - Application of a window aggregation can be optionally run in parallel
#'   over multiple groups (see argument `.by`) by setting a
#'   [future](https://cran.r-project.org/package=future) parallel backend. This
#'   is implemented using [furrr](https://cran.r-project.org/package=furrr)
#'   package.
#'
#'   - function subsumes regular usecases of \code{\link[dplyr]{mutate}}
#'
#'   For `tbl_lazy` input:
#'
#'   - Uses `dbplyr::window_order` and `dbplyr::window_frame` to translate to
#'   `partition_by` and window frame specification.
#'
#' @param x (`data.frame` or `tbl_lazy`)
#' @param ... expressions to be passed to \code{\link[dplyr]{mutate}}
#' @param .by (character vector, optional: Yes) Columns to group by
#' @param .order_by (string, optional: Yes) Columns to order by
#' @param .frame (vector, optional: Yes) Vector of length 2 indicating the
#'   number of rows to consider before and after the current row. When argument
#'   `.index` is provided (typically a column of type date or datetime), before
#'   and after can be
#'   [interval](https://lubridate.tidyverse.org/reference/interval.html)
#'   objects. See examples. When input is `tbl_lazy`, only number of rows as
#'   vector of length 2 is supported.
#' @param .index (string, optional: Yes, default: NULL) index column. This is
#'   supported when input is a dataframe only.
#' @param .desc (flag, default: FALSE) Whether to order in descending order
#' @param .complete (flag, default: FALSE) This will be passed to
#'   `slider::slide` / `slider::slide_vec`. Should the function be evaluated on
#'   complete windows only? If FALSE or NULL, the default, then partial
#'   computations will be allowed. This is supported when input is a dataframe
#'   only.
#' @return `data.frame` or `tbl_lazy`
#' @importFrom magrittr %>%
#' @importFrom utils tail
#'
#' @examples
#' library("magrittr")
#' # example 1 (simple case with dataframe)
#' # Using iris dataset,
#' # compute cumulative mean of column `Sepal.Length`
#' # ordered by `Petal.Width` and `Sepal.Width` columns
#' # grouped by `Petal.Length` column
#'
#' iris %>%
#'   tidier::mutate_(sl_mean = mean(Sepal.Length),
#'                   .order_by = c("Petal.Width", "Sepal.Width"),
#'                   .by = "Petal.Length",
#'                   .frame = c(Inf, 0),
#'                   ) %>%
#'   dplyr::slice_min(n = 3, Petal.Width, by = Species)
#'
#' # example 2 (detailed case with dataframe)
#' # Using a sample airquality dataset,
#' # compute mean temp over last seven days in the same month for every row
#'
#' set.seed(101)
#' airquality %>%
#'   # create date column
#'   dplyr::mutate(date_col = lubridate::make_date(1973, Month, Day)) %>%
#'   # create gaps by removing some days
#'   dplyr::slice_sample(prop = 0.8) %>%
#'   dplyr::arrange(date_col) %>%
#'   # compute mean temperature over last seven days in the same month
#'   tidier::mutate_(avg_temp_over_last_week = mean(Temp, na.rm = TRUE),
#'                   .order_by = "Day",
#'                   .by = "Month",
#'                   .frame = c(lubridate::days(7), # 7 days before current row
#'                             lubridate::days(-1) # do not include current row
#'                             ),
#'                   .index = "date_col"
#'                   )
#' # example 3
#' airquality %>%
#'    # create date column as character
#'    dplyr::mutate(date_col =
#'                    as.character(lubridate::make_date(1973, Month, Day))
#'                  ) %>%
#'    tibble::as_tibble() %>%
#'    # as `tbl_lazy`
#'    dbplyr::memdb_frame() %>%
#'    mutate_(avg_temp = mean(Temp),
#'            .by = "Month",
#'            .order_by = "date_col",
#'            .frame = c(3, 3)
#'            ) %>%
#'    dplyr::collect() %>%
#'    dplyr::select(Ozone, Solar.R, Wind, Temp, Month, Day, date_col, avg_temp)
#' @export
mutate_ = function(x,
                   ...,
                   .by,
                   .order_by,
                   .frame,
                   .index,
                   .desc = FALSE,
                   .complete = FALSE
                   ){
  checkmate::assert_multi_class(x, c("data.frame", "tbl_lazy"))

  if (inherits(x, "data.frame")){
    # capture expressions --------------------------------------------------------
    ddd = rlang::enquos(...)

    # assertions -----------------------------------------------------------------
    order_by_is_missing = missing(.order_by)
    by_is_missing       = missing(.by)
    frame_is_missing    = missing(.frame)
    index_is_missing    = missing(.index)

    if (!order_by_is_missing) {
      checkmate::assert_character(.order_by,
                                  unique = TRUE,
                                  any.missing = FALSE,
                                  min.len = 1
                                  )
      checkmate::assert_subset(.order_by, choices = colnames(x))
      checkmate::assert_logical(.desc, any.missing = FALSE, min.len = 1)
      len_desc = length(.desc)
      checkmate::assert(len_desc == length(.order_by) || len_desc == 1)
    }

    if (!by_is_missing) {
      checkmate::assert_character(.by,
                                  unique = TRUE,
                                  any.missing = FALSE,
                                  min.len = 1,
                                  )
      checkmate::assert_subset(.by, choices = colnames(x))
    }

    if (!frame_is_missing) {
      checkmate::assert(length(.frame) == 2)
      checkmate::assert(inherits(.frame, c("numeric", "Period")))
      checkmate::assert_true(all(class(.frame[[1]]) == class(.frame[[2]])))
      if (!index_is_missing) {
        checkmate::assert_string(.index)
        checkmate::assert_subset(.index, choices = colnames(x))
      } else {
        .index = NULL
      }
    }

    # order before mutate --------------------------------------------------------
    if (!order_by_is_missing){

      if (len_desc == 1){
        .desc = rep(.desc, length(.order_by))
      }
      row_order = do.call(order,
                          c(lapply(.order_by, function(.x) x[[.x]]),
                            list(decreasing = .desc)
                            )
                          )
      x_copy = x[row_order, ]

    } else {
      x_copy = x
    }

    # mutate core operation ---------------------------------------------------
    # for cran checks
    data__ = NULL
    slide_output__ = NULL

    if (by_is_missing){
      # without groups ----
      if (frame_is_missing){
        # simple mutate without slide
        x_copy = dplyr::mutate(x_copy, !!!ddd)
      } else {
        # without groups and with slide
        if (index_is_missing){
          x_copy = x_copy %>%
            dplyr::mutate(slide_output__ =
                            slider::slide(
                              x_copy,
                              .f = ~ as.list(dplyr::summarise(.x, !!!ddd)),
                              .before = .frame[1],
                              .after  = .frame[2],
                              .complete = .complete
                              )
                          ) %>%
            remove_common_nested_columns(slide_output__) %>%
            tidyr::unnest_wider(slide_output__)
        } else {
          x_copy = x_copy %>%
            dplyr::mutate(slide_output__ =
                            slider::slide_index(
                              x_copy,
                              .f = ~ as.list(dplyr::summarise(.x, !!!ddd)),
                              .i = x_copy[[.index]],
                              .before = .frame[1],
                              .after  = .frame[2],
                              .complete = .complete
                              )
                          ) %>%
            remove_common_nested_columns(slide_output__) %>%
            tidyr::unnest_wider(slide_output__)
        }
      }
    } else {
      # with groups ----
      if (frame_is_missing){
        # groupby mutate
        x_copy = x_copy %>%
          dplyr::group_by(dplyr::across(dplyr::all_of(.by))) %>%
          dplyr::mutate(!!!ddd) %>%
          dplyr::ungroup()

      } else {
        # with groups and with slide
        fun_per_chunk = function(chunk, ...){
          if (index_is_missing) {
            out = chunk %>%
              dplyr::mutate(slide_output__ =
                                slider::slide(
                                  chunk,
                                  .f = ~ as.list(dplyr::summarise(.x, !!!ddd)),
                                  .before = .frame[1],
                                  .after  = .frame[2],
                                  .complete = .complete
                                  )
                            )
          } else {
            out = chunk %>%
              dplyr::mutate(slide_output__ =
                                slider::slide_index(
                                  chunk,
                                  .f = ~ as.list(dplyr::summarise(.x, !!!ddd)),
                                  .i  = chunk[[.index]],
                                  .before = .frame[1],
                                  .after  = .frame[2],
                                  .complete = .complete
                                  )
                            )
          }

          # remove groupby columns (if they exist)
          for (acol in .by){
            out[[acol]] = NULL
          }

          return(out)
        }

        x_copy = x_copy %>%
          tidyr::nest(data__ = dplyr::everything(),
                      .by = dplyr::all_of(.by)
                      ) %>%
          dplyr::ungroup() %>%
          dplyr::mutate(data__ = furrr::future_map(data__, fun_per_chunk)) %>%
          tidyr::unnest(data__) %>%
          remove_common_nested_columns(slide_output__) %>%
          tidyr::unnest_wider(slide_output__)

      }
    }

    # reorder the output before return -------------------------------------------
    if (!order_by_is_missing){ x_copy = x_copy[order(row_order), ] }
    res = x_copy
  } else {
    # capture expressions -----------------------------------------------------
    ddd = rlang::enquos(...)

    # assertions --------------------------------------------------------------
    if (!missing(.index)) {
      stop(paste0("When input is `tbl_lazy`,",
                  " `.index` argument is not supported.",
                  " `.index` should missing"
                  )
           )
    }

    if (!missing(.complete)) {
      stop(paste0("When input is `tbl_lazy`,",
                  " `.complete` argument is not supported.",
                  " `.complete` should be missing"
                  )
           )
    }

    order_by_is_missing = missing(.order_by)
    by_is_missing       = missing(.by)
    frame_is_missing    = missing(.frame)

    # declare res -------------------------------------------------------------
    res = x

    # group by before mutate -------------------------------------------------
    if (!by_is_missing) {
      res = dplyr::group_by(res, dplyr::pick(dplyr::all_of(.by)))
    }

    # apply frame before mutate -----------------------------------------------
    if (!frame_is_missing) {
      checkmate::assert(length(.frame) == 2)
      checkmate::assert_numeric(.frame)

      res = dbplyr::window_frame(res, from = -.frame[1], to = .frame[2])
    }

    # order before mutate -----------------------------------------------------
    if (!order_by_is_missing){
      checkmate::assert_string(.order_by)
      checkmate::assert_subset(.order_by, colnames(x))
      checkmate::assert_flag(.desc)

      if (.desc){
        res = dbplyr::window_order(res, dplyr::desc(!!rlang::sym(.order_by)))
      } else {
        res = dbplyr::window_order(res, !!rlang::sym(.order_by))
      }
    }

    # core mutate operation ---------------------------------------------------
    res = dplyr::mutate(res, !!!ddd)
    res = dplyr::ungroup(res)

  }
  # return ---------------------------------------------------------------------
  return(res)
}

# mutate -----------------------------------------------------------------------

#' @name mutate
#' @title Drop-in replacement for \code{\link[dplyr]{mutate}}
#' @description Provides supercharged version of \code{\link[dplyr]{mutate}}
#'   with `group_by`, `order_by` and aggregation over arbitrary window frame
#'   around a row for dataframes and lazy (remote) `tbl`s of class `tbl_lazy`.
#' @seealso mutate_
#' @details A window function returns a value for every input row of a dataframe
#'   or `lazy_tbl` based on a group of rows (frame) in the neighborhood of the
#'   input row. This function implements computation over groups (`partition_by`
#'   in SQL) in a predefined order (`order_by` in SQL) across a neighborhood of
#'   rows (frame) defined by a (up, down) where
#'
#'   - up/down are number of rows before and after the corresponding row
#'
#'   - up/down are interval objects (ex: `c(days(2), days(1))`).
#'   Interval objects are currently supported for dataframe only. (not
#'   `tbl_lazy`)
#'
#'   This implementation is inspired by spark's [window
#'   API](https://www.databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html).
#'
#'   **Implementation Details**:
#'
#'   For dataframe input:
#'
#'   - Iteration per row over the window is implemented using the versatile
#'   [`slider`](https://cran.r-project.org/package=slider).
#'
#'   - Application of a window aggregation can be optionally run in parallel
#'   over multiple groups (see argument `.by`) by setting a
#'   [future](https://cran.r-project.org/package=future) parallel backend. This
#'   is implemented using [furrr](https://cran.r-project.org/package=furrr)
#'   package.
#'
#'   - function subsumes regular usecases of \code{\link[dplyr]{mutate}}
#'
#'   For `tbl_lazy` input:
#'
#'   - Uses `dbplyr::window_order` and `dbplyr::window_frame` to translate to
#'   `partition_by` and window frame specification.
#'
#' @param x (`data.frame` or `tbl_lazy`)
#' @param ... expressions to be passed to \code{\link[dplyr]{mutate}}
#' @param .by (expression, optional: Yes) Columns to group by
#' @param .order_by (expression, optional: Yes) Columns to order by
#' @param .frame (vector, optional: Yes) Vector of length 2 indicating the
#'   number of rows to consider before and after the current row. When argument
#'   `.index` is provided (typically a column of type date or datetime), before
#'   and after can be
#'   [interval](https://lubridate.tidyverse.org/reference/interval.html)
#'   objects. See examples. When input is `tbl_lazy`, only number of rows as
#'   vector of length 2 is supported.
#' @param .index (expression, optional: Yes, default: NULL) index column. This
#'   is supported when input is a dataframe only.
#' @param .complete (flag, default: FALSE) This will be passed to
#'   `slider::slide` / `slider::slide_vec`. Should the function be evaluated on
#'   complete windows only? If FALSE or NULL, the default, then partial
#'   computations will be allowed. This is supported when input is a dataframe
#'   only.
#' @return `data.frame` or `tbl_lazy`
#' @importFrom magrittr %>%
#' @importFrom utils tail
#'
#' @examples
#' library("magrittr")
#' # example 1 (simple case with dataframe)
#' # Using iris dataset,
#' # compute cumulative mean of column `Sepal.Length`
#' # ordered by `Petal.Width` and `Sepal.Width` columns
#' # grouped by `Petal.Length` column
#'
#' iris %>%
#'   mutate(sl_mean = mean(Sepal.Length),
#'          .order_by = c(Petal.Width, Sepal.Width),
#'          .by = Petal.Length,
#'          .frame = c(Inf, 0),
#'          ) %>%
#'   dplyr::slice_min(n = 3, Petal.Width, by = Species)
#'
#' # example 2 (detailed case with dataframe)
#' # Using a sample airquality dataset,
#' # compute mean temp over last seven days in the same month for every row
#'
#' set.seed(101)
#' airquality %>%
#'   # create date column
#'   dplyr::mutate(date_col = lubridate::make_date(1973, Month, Day)) %>%
#'   # create gaps by removing some days
#'   dplyr::slice_sample(prop = 0.8) %>%
#'   dplyr::arrange(date_col) %>%
#'   # compute mean temperature over last seven days in the same month
#'   tidier::mutate(avg_temp_over_last_week = mean(Temp, na.rm = TRUE),
#'                  .order_by = Day,
#'                  .by = Month,
#'                  .frame = c(lubridate::days(7), # 7 days before current row
#'                             lubridate::days(-1) # do not include current row
#'                             ),
#'                  .index = date_col
#'                  )
#' # example 3
#' airquality %>%
#'    # create date column as character
#'    dplyr::mutate(date_col =
#'                    as.character(lubridate::make_date(1973, Month, Day))
#'                  ) %>%
#'    tibble::as_tibble() %>%
#'    # as `tbl_lazy`
#'    dbplyr::memdb_frame() %>%
#'    mutate(avg_temp = mean(Temp),
#'           .by = Month,
#'           .order_by = date_col,
#'           .frame = c(3, 3)
#'           ) %>%
#'    dplyr::collect() %>%
#'    dplyr::select(Ozone, Solar.R, Wind, Temp, Month, Day, date_col, avg_temp)
#' @export
mutate = function(x,
                  ...,
                  .by,
                  .order_by,
                  .frame,
                  .index,
                  .complete = FALSE
                  ){

  checkmate::assert_multi_class(x, c("data.frame", "tbl_lazy"))

  if (inherits(x, "data.frame")){
    # capture expressions ----------------------------------------------------
    ddd = rlang::enquos(...)

    # assertions --------------------------------------------------------------
    order_by_is_missing = missing(.order_by)
    by_is_missing       = missing(.by)
    frame_is_missing    = missing(.frame)
    index_is_missing    = missing(.index)

    if (!by_is_missing) {
      by = rlang::enexpr(.by)

      if (rlang::is_call(by)){
        # case: starts with 'c'
        first_thing = by[[1]]
        if (! (rlang::as_string(first_thing) == "c")) {
          stop("expression to .by is not parsable")
        }

        by_str = lapply(by, identity) %>%
          tail(-1) %>%
          vapply(rlang::as_string, character(1))

      } else {
        # case: direct columns
        by_str = rlang::as_string(by)
      }
    }

    if (!frame_is_missing) {
      checkmate::assert(length(.frame) == 2)
      checkmate::assert(inherits(.frame, c("numeric", "Period")))
      checkmate::assert_true(all(class(.frame[[1]]) == class(.frame[[2]])))
      if (!index_is_missing) {
        .index = rlang::as_string(rlang::enexpr(.index))
      } else {
        .index = NULL
      }
    }

    # `.complete` is TRUE or FALSE (same as NULL)
    checkmate::check_flag(.complete, null.ok = TRUE)
    if (is.null(.complete)){
      .complete = FALSE
    }

    # order before mutate ----------------------------------------------------
    if (!order_by_is_missing){
      order_by = rlang::enexpr(.order_by)

      if (rlang::is_call(order_by)){
        # case: starts with 'c' or 'desc'
        first_thing = order_by[[1]]
        if(! (rlang::as_string(first_thing) %in% c("c", "desc"))) {
          stop("expression to arrange is not parsable")
        }

        if (first_thing == "c"){
          res = x %>%
            dplyr::mutate(rn_ = dplyr::row_number()) %>%
            dplyr::arrange(!!!tail(lapply(order_by, identity), -1))

        } else {
          # proto: desc(Sepal.Length)
          res = x %>%
            dplyr::mutate(rn_ = dplyr::row_number()) %>%
            dplyr::arrange(!!order_by)
        }
      } else {
        # case: direct columns
        res = x %>%
          dplyr::mutate(rn_ = dplyr::row_number()) %>%
          dplyr::arrange(!!order_by)
      }

      row_order = res[["rn_"]]
      res[["rn_"]] = NULL

    } else {
      # order is null
      res = x
    }

    # mutate core operation --------------------------------------------------
    # for cran checks
    data__ = NULL
    slide_output__ = NULL

    if (by_is_missing){
      # without groups ----
      if (frame_is_missing){
        # simple mutate without slide
        res = dplyr::mutate(res, !!!ddd)
      } else {
        # without groups and with slide
        if (index_is_missing){
          res = res %>%
            dplyr::mutate(slide_output__ =
                            slider::slide(
                              res,
                              .f = ~ as.list(dplyr::summarise(.x, !!!ddd)),
                              .before = .frame[1],
                              .after  = .frame[2],
                              .complete = .complete
                              )
                          ) %>%
            remove_common_nested_columns(slide_output__) %>%
            tidyr::unnest_wider(slide_output__)
        } else {
          res = res %>%
            dplyr::mutate(slide_output__ =
                            slider::slide_index(
                              res,
                              .f = ~ as.list(dplyr::summarise(.x, !!!ddd)),
                              .i = res[[.index]],
                              .before = .frame[1],
                              .after  = .frame[2],
                              .complete = .complete
                              )
                          ) %>%
            remove_common_nested_columns(slide_output__) %>%
            tidyr::unnest_wider(slide_output__)
        }
      }
    } else {
      # with groups ----
      if (frame_is_missing){
        # groupby mutate
        res = res %>%
          dplyr::group_by(dplyr::pick({{.by}})) %>%
          dplyr::mutate(!!!ddd) %>%
          dplyr::ungroup()

      } else {
        # with groups and with slide
        fun_per_chunk = function(chunk, ...){
          if (index_is_missing) {
            out = chunk %>%
              dplyr::mutate(slide_output__ =
                                slider::slide(
                                  chunk,
                                  .f = ~ as.list(dplyr::summarise(.x, !!!ddd)),
                                  .before = .frame[1],
                                  .after  = .frame[2],
                                  .complete = .complete
                                  )
                            )
          } else {
            out = chunk %>%
              dplyr::mutate(slide_output__ =
                                slider::slide_index(
                                  chunk,
                                  .f = ~ as.list(dplyr::summarise(.x, !!!ddd)),
                                  .i  = chunk[[.index]],
                                  .before = .frame[1],
                                  .after  = .frame[2],
                                  .complete = .complete
                                  )
                            )
          }

          # remove groupby columns (if they exist)
          for (acol in by_str){
            out[[acol]] = NULL
          }
          return(out)
        }

        res = res %>%
          tidyr::nest(data__ = dplyr::everything(),
                      .by = {{.by}}
                      ) %>%
          dplyr::ungroup() %>%
          dplyr::mutate(data__ = furrr::future_map(data__, fun_per_chunk)) %>%
          tidyr::unnest(data__) %>%
          remove_common_nested_columns(slide_output__) %>%
          tidyr::unnest_wider(slide_output__)

      }
    }
  } else {
    # capture expressions -----------------------------------------------------
    ddd = rlang::enquos(...)

    # assertions --------------------------------------------------------------
    if (!missing(.index)) {
      stop(paste0("When input is `tbl_lazy`,",
                  " `.index` argument is not supported.",
                  " `.index` should missing"
                  )
           )
    }

    if (!missing(.complete)) {
      stop(paste0("When input is `tbl_lazy`,",
                  " `.complete` argument is not supported.",
                  " `.complete` should be missing"
                  )
           )
    }

    order_by_is_missing = missing(.order_by)
    by_is_missing       = missing(.by)
    frame_is_missing    = missing(.frame)

    # declare res -------------------------------------------------------------
    res = x

    # group by before mutate -------------------------------------------------
    if (!by_is_missing) {
      res = dplyr::group_by(res, dplyr::pick({{.by}}))
    }

    # apply frame before mutate -----------------------------------------------
    if (!frame_is_missing) {
      checkmate::assert(length(.frame) == 2)
      checkmate::assert_numeric(.frame)

      res = dbplyr::window_frame(res, from = -.frame[1], to = .frame[2])
    }

    # order before mutate -----------------------------------------------------
    if (!order_by_is_missing){
      order_by = rlang::enexpr(.order_by)

      if (rlang::is_call(order_by)){
        # case: starts with 'c' or 'desc'
        first_thing = order_by[[1]]
        if(! (rlang::as_string(first_thing) %in% c("c", "desc"))) {
          stop("expression to arrange is not parsable")
        }

        if (first_thing == "c"){
          res = dbplyr::window_order(res,
                                     !!!tail(lapply(order_by, identity), -1)
                                     )

        } else {
          # proto: desc(Sepal.Length)
          res = dbplyr::window_order(res, !!order_by)
        }
      } else { # not call
        # case: direct columns
        res = dbplyr::window_order(res, !!order_by)
      }
    }

    # core mutate operation ---------------------------------------------------
    res = dplyr::mutate(res, !!!ddd)
    res = dplyr::ungroup(res)
  }

  return(res)
}

# remove_common_nested_columns ----
#' @name remove_common_nested_columns
#' @title Remove non-list columns when same are present in a list column
#' @description Remove non-list columns when same are present in a list column
#' @param df input dataframe
#' @param list_column Name or expr of the column which is a list of named lists
#' @return dataframe
remove_common_nested_columns = function(df, list_column){

  # we assume that all dfs in list_column have identical column names
  new_names = df %>%
    dplyr::slice(1) %>%
    dplyr::pull({{ list_column }}) %>%
    `[[`(1) %>%
    names()

  common_names = intersect(new_names, colnames(df))

  if (length(common_names) >= 1){
    for (a_common_name in common_names){
      df[[a_common_name]] = NULL
    }
  }

  return(df)
}


