# mutate_ ----

#' @name mutate_
#' @title Drop-in replacement for \code{\link[dplyr]{mutate}}
#' @description Provides supercharged version of \code{\link[dplyr]{mutate}}
#'   with `group_by`, `order_by` and aggregation over arbitrary window frame
#'   around a row. This function allows some arguments to be passed as strings
#'   instead of expressions.
#' @seealso mutate
#' @details A window function returns a value for every input row of a dataframe
#'   based on a group of rows (frame) in the neighborhood of the input row. This
#'   function implements computation over groups (`partition_by` in SQL) in a
#'   predefined order (`order_by` in SQL) across a neighborhood of rows (frame)
#'   defined by a (up, down) where
#'
#'   - up/down are number of rows before and after the corresponding row
#'
#'   - up/down are interval objects (ex: `c(days(2), days(1))`)
#'
#'   This implementation is inspired by spark's [window
#'   API](https://www.databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html).
#'
#'
#'   Implementation Details:
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
#' @param x (data.frame)
#' @param ... expressions to be passed to \code{\link[dplyr]{mutate}}
#' @param .by (character vector, optional: Yes) columns to group by
#' @param .order_by (character vector, optional: Yes) columns to order by
#' @param .frame (vector, optional: Yes) Vector of length 2 indicating the
#'   number of rows to consider before and after the current row. When argument
#'   `.index` is provided (typically a column of type date or datetime), before
#'   and after can be
#'   [interval](https://lubridate.tidyverse.org/reference/interval.html)
#'   objects. See examples.
#' @param .index (string, optional: Yes) name of index column
#' @param .desc (logical_vector, default: FALSE) bool or logical vector of same
#'   length as `.order_by`.
#' @return data.frame
#' @importFrom magrittr %>%
#' @importFrom utils tail
#' @export
#'
#' @examples
#' library("magrittr")
#' # example 1
#' # Using iris dataset,
#' # compute cumulative mean of column `Sepal.Length`
#' # ordered by `Petal.Width` and `Sepal.Width` columns
#' # grouped by `Petal.Length` column
#'
#' iris %>%
#'   mutate_(sl_mean = mean(Sepal.Length),
#'           .order_by = c("Petal.Width", "Sepal.Width"),
#'           .by = "Petal.Length",
#'           .frame = c(Inf, 0),
#'           ) %>%
#'   dplyr::slice_min(n = 3, Petal.Width, by = Species)
#'
#' # example 2
#' # Using a sample airquality dataset,
#' # compute mean temp over last seven days in the same month for every row
#'
#' airquality %>%
#'   # create date column
#'   dplyr::mutate(date_col = as.Date(paste("1973",
#'                                          stringr::str_pad(Month,
#'                                                           width = 2,
#'                                                           side = "left",
#'                                                           pad = "0"
#'                                                           ),
#'                                          stringr::str_pad(Day,
#'                                                           width = 2,
#'                                                           side = "left",
#'                                                           pad = "0"
#'                                                           ),
#'                                          sep = "-"
#'                                          )
#'                                   )
#'                 ) %>%
#'   # create gaps by removing some days
#'   dplyr::slice_sample(prop = 0.8) %>%
#'   # compute mean temperature over last seven days in the same month
#'   mutate_(avg_temp_over_last_week = mean(Temp, na.rm = TRUE),
#'           .order_by = "Day",
#'           .by = "Month",
#'           .frame = c(lubridate::days(7), # 7 days before current row
#'                      lubridate::days(-1) # do not include current row
#'                      ),
#'           .index = "date_col"
#'           )
#' @export

mutate_ = function(x, ..., .by, .order_by, .frame, .index, .desc = FALSE){

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

  # mutate core operation ------------------------------------------------------
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
                            .after  = .frame[2]
                            )
                        ) %>%
          tidyr::unnest_wider(slide_output__)
      } else {
        x_copy = x_copy %>%
          dplyr::mutate(slide_output__ =
                          slider::slide_index(
                            x_copy,
                            .f = ~ as.list(dplyr::summarise(.x, !!!ddd)),
                            .i = x_copy[[.index]],
                            .before = .frame[1],
                            .after  = .frame[2]
                            )
                        ) %>%
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
                                .after  = .frame[2]
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
                                .after  = .frame[2]
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
        tidyr::unnest_wider(slide_output__)

    }
  }

  # reorder the output before return -------------------------------------------
  if (!order_by_is_missing){ x_copy = x_copy[order(row_order), ] }
  # return ---------------------------------------------------------------------
  return(x_copy)
}

# mutate ----

#' @name mutate
#' @title Drop-in replacement for \code{\link[dplyr]{mutate}}
#' @description Provides supercharged version of \code{\link[dplyr]{mutate}}
#'   with `group_by`, `order_by` and aggregation over arbitrary window frame
#'   around a row.
#' @seealso mutate_
#' @details A window function returns a value for every input row of a dataframe
#'   based on a group of rows (frame) in the neighborhood of the input row. This
#'   function implements computation over groups (`partition_by` in SQL) in a
#'   predefined order (`order_by` in SQL) across a neighborhood of rows (frame)
#'   defined by a (up, down) where
#'
#'   - up/down are number of rows before and after the corresponding row
#'
#'   - up/down are interval objects (ex: `c(days(2), days(1))`)
#'
#'   This implementation is inspired by spark's [window
#'   API](https://www.databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html).
#'
#'
#'   Implementation Details:
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
#' @param x (data.frame)
#' @param ... expressions to be passed to \code{\link[dplyr]{mutate}}
#' @param .by (expression, optional: Yes) columns to group by
#' @param .order_by (expression, optional: Yes) columns to order by
#' @param .frame (vector, optional: Yes) Vector of length 2 indicating the
#'   number of rows to consider before and after the current row. When argument
#'   `.index` is provided (typically a column of type date or datetime), before
#'   and after can be
#'   [interval](https://lubridate.tidyverse.org/reference/interval.html)
#'   objects. See examples.
#' @param .index (expression, optional: Yes) index column
#' @return data.frame
#' @importFrom magrittr %>%
#' @importFrom utils tail
#'
#' @export
#'
#' @examples
#' library("magrittr")
#' # example 1
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
#' # example 2
#' # Using a sample airquality dataset,
#' # compute mean temp over last seven days in the same month for every row
#'
#' airquality %>%
#'   # create date column
#'   dplyr::mutate(date_col = as.Date(paste("1973",
#'                                          stringr::str_pad(Month,
#'                                                           width = 2,
#'                                                           side = "left",
#'                                                           pad = "0"
#'                                                           ),
#'                                          stringr::str_pad(Day,
#'                                                           width = 2,
#'                                                           side = "left",
#'                                                           pad = "0"
#'                                                           ),
#'                                          sep = "-"
#'                                          )
#'                                   )
#'                 ) %>%
#'   # create gaps by removing some days
#'   dplyr::slice_sample(prop = 0.8) %>%
#'   # compute mean temperature over last seven days in the same month
#'   mutate(avg_temp_over_last_week = mean(Temp, na.rm = TRUE),
#'          .order_by = Day,
#'          .by = Month,
#'          .frame = c(lubridate::days(7), # 7 days before current row
#'                     lubridate::days(-1) # do not include current row
#'                     ),
#'          .index = date_col
#'          )
#' @export
mutate = function(x, ..., .by, .order_by, .frame, .index){

  # capture expressions --------------------------------------------------------
  ddd = rlang::enquos(...)

  # assertions -----------------------------------------------------------------
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

  # order before mutate --------------------------------------------------------
  if (!order_by_is_missing){
    order_by = rlang::enexpr(.order_by)

    if (rlang::is_call(order_by)){
      # case: starts with 'c' or 'desc'
      first_thing = order_by[[1]]
      if(! (rlang::as_string(first_thing) %in% c("c", "desc"))) {
        stop("expression to arrange is not parsable")
      }

      if (first_thing == "c"){
        x_copy = x %>%
          dplyr::mutate(rn_ = dplyr::row_number()) %>%
          dplyr::arrange(!!!tail(lapply(order_by, identity), -1))

      } else {
        # proto: desc(Sepal.Length)
        x_copy = x %>%
          dplyr::mutate(rn_ = dplyr::row_number()) %>%
          dplyr::arrange(!!order_by)
      }
    } else {
      # case: direct columns
      x_copy = x %>%
        dplyr::mutate(rn_ = dplyr::row_number()) %>%
        dplyr::arrange(!!order_by)
    }

    row_order = x_copy[["rn_"]]
    x_copy[["rn_"]] = NULL

  } else {
    # order is missing
    x_copy = x
  }

  # mutate core operation ------------------------------------------------------
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
                            .after  = .frame[2]
                            )
                        ) %>%
          tidyr::unnest_wider(slide_output__)
      } else {
        x_copy = x_copy %>%
          dplyr::mutate(slide_output__ =
                          slider::slide_index(
                            x_copy,
                            .f = ~ as.list(dplyr::summarise(.x, !!!ddd)),
                            .i = x_copy[[.index]],
                            .before = .frame[1],
                            .after  = .frame[2]
                            )
                        ) %>%
          tidyr::unnest_wider(slide_output__)
      }
    }
  } else {
    # with groups ----
    if (frame_is_missing){
      # groupby mutate
      x_copy = x_copy %>%
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
                                .after  = .frame[2]
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
                                .after  = .frame[2]
                                )
                          )
        }

        # remove groupby columns (if they exist)
        for (acol in by_str){
          out[[acol]] = NULL
        }
        return(out)
      }

      x_copy = x_copy %>%
        tidyr::nest(data__ = dplyr::everything(),
                    .by = {{.by}}
                    ) %>%
        dplyr::ungroup() %>%
        dplyr::mutate(data__ = furrr::future_map(data__, fun_per_chunk)) %>%
        tidyr::unnest(data__) %>%
        tidyr::unnest_wider(slide_output__)

    }
  }

  # reorder the output before return -------------------------------------------
  if (!order_by_is_missing){
    x_copy = x_copy[row_order, ]
  }
  # return ---------------------------------------------------------------------
  return(x_copy)
}

