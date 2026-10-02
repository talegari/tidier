
#' @name rows_between
#' @title Create a frame indicating rows between window
#' @description Create a frame indicating rows between window
#' @param before Number of rows before
#' @param after Number of rows after
#' @return object of class rows_between_frame, frame
#' @seealso [rows_between()], [range_between()], [mutate()]
#' @export
rows_between = function(before, after){

  res = list("before" = before, "after" = after)
  class(res) = c("rows_between_frame", "frame", class(res))
  return(res)
}

#' @name range_between
#' @title Create a frame indicating range between window
#' @description Create a frame indicating range between window
#' @param before range of rows before
#' @param after range of rows after
#' @return object of class range_between_frame, frame
#' @seealso [rows_between()], [range_between()], [mutate()]
#' @export
range_between = function(before, after){

  res = list("before" = before, "after" = after)
  class(res) = c("range_between_frame", "frame", class(res))
  return(res)
}

#' @name remove_common_nested_columns
#' @title Remove non-list columns when same are present in a list column
#' @description Remove non-list columns when same are present in a list column
#' @param df input dataframe
#' @param list_column Name or expr of the column which is a list of named lists
#' @return dataframe
#' @keywords internal
remove_common_nested_columns = function(df, list_column){

  lc = rlang::as_name(rlang::enquo(list_column))
  new_names = names(df[1,][[lc]])
  common_names = intersect(new_names, colnames(df))
  if (length(common_names) > 0){
    df = dplyr::select(df, -dplyr::all_of(common_names))
  }

  return(df)
}

#' @name mutate
#' @title Drop-in replacement for [dplyr::mutate]
#' @description Provides supercharged version of [dplyr::mutate]
#'   with `.by` (group by), `.order_by` and `.frame` aggregation over arbitrary
#'   window frame
#' @details A window function returns a value for every input row of a dataframe
#'   based on a group of rows (frame) in the neighborhood of the input row. This
#'   function implements computation over groups (`partition_by` in SQL) in a
#'   predefined order (`order_by` in SQL) across a neighborhood of rows (frame)
#'   defined by
#'
#'   - `rows_between`: Number of rows before and after the corresponding row.
#'                     Example: `c(2, 1)`
#'
#'   - `range_between`: Range is numeric or interval objects
#'                      Example: `c(days(2), days(1))`
#'
#'   This implementation is inspired by spark's [window
#'   API](https://www.databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html).
#'   The output has the same row order as the input independent of the
#'   `order_by`.
#'
#' @param x (`data.frame`_
#' @param ... expressions to be passed to [dplyr::mutate]
#' @param .by (expression, optional: Yes) Columns to group by
#' @param .order_by (expression, optional: Yes) Columns to order by
#' @param .frame (vector, optional: Yes) Object of class `frame` created by one
#'   of these functions: `rows_between`, `range_between`
#' @param .complete (flag, default: FALSE) passed to [slider::slide]
#' @return `data.frame`
#' @importFrom magrittr %>%
#' @importFrom utils tail
#' @importFrom rlang abort
#' @seealso [rows_between()], [range_between()], [mutate()]
#'
#' @examples
#' library("magrittr") # for pipe
#' # example 1: rows between
#' # Using iris dataset,
#' # compute cumulative mean of column `Sepal.Length`
#' # ordered by `Petal.Width` and `Sepal.Width` columns
#' # grouped by `Petal.Length` column
#'
#' iris %>%
#'   mutate(sl_mean = mean(Sepal.Length),
#'          .order_by = c(Petal.Width, Sepal.Width),
#'          .by = Petal.Length,
#'          .frame = rows_between(Inf, 0),
#'          ) %>%
#'   dplyr::slice_min(n = 3, Petal.Width, by = Species)
#'
#' # example 2: range between
#' # Using a sample airquality dataset,
#' # compute mean temp over last seven days in the same month for every row
#'
#' set.seed(101)
#' airquality %>%
#'   # create date column
#'   dplyr::mutate(date_col = lubridate::make_date(1973, Month, Day)) %>%
#'   # create gaps by removing some days
#'   dplyr::slice_sample(prop = 0.8) %>%
#'   # compute mean temperature over last seven days in the same month
#'   tidier::mutate(avg_temp_over_last_week = mean(Temp, na.rm = TRUE),
#'                  .order_by = date_col,
#'                  .by = Month,
#'                  .frame = range_between(
#'                             lubridate::days(7), # 7 days before current row
#'                             lubridate::days(-1) # do not include current row
#'                             )
#'                  )
#'
#' # example 3: custom function / modeling over window frame
#' fit_lm_safe = function(df, formula, min_rows = 2) {
#'   if (is.null(df) || nrow(df) < min_rows) {
#'     return(NULL)
#'   }
#'
#'   tryCatch(
#'     lm(formula, data = df),
#'     error = function(e) NULL
#'   )
#' }
#'
#' mtcars %>%
#'   mutate(s = list(fit_lm_safe(pick(everything()), mpg ~ .)),
#'          .by = c(cyl, vs),
#'          .order_by = qsec,
#'          .frame = range_between(-1, 3),
#'          .complete = FALSE
#'          ) %>%
#'   head(10)
#'
#' \dontrun{
#' # example 4: parallel execution across many groups using tidyr::nest and furrr
#' future::plan(future::multisession(workers = 2))
#'
#' iris %>%
#'   tidyr::nest(.by = Species) %>%
#'   dplyr::mutate(
#'     data = furrr::future_map(data, ~ .x %>%
#'                         mutate(
#'                           sl_mean = mean(Sepal.Length),
#'                           .order_by = Petal.Width,
#'                           .frame = rows_between(2, 2)
#'                         )
#'                      )
#'   ) %>%
#'   tidyr::unnest(data)
#' }
#' @export
mutate = function(x,
                  ...,
                  .by,
                  .order_by,
                  .frame,
                  .complete = FALSE
                  ){

  # ==== prepare before core mutate ============================================
  # Note: Stage input dataframe, validate arguments, capture expressions,
  # parse grouping specs (.by), and pre-sort rows based on .order_by
  # while recording original row numbers in rn__.

  checkmate::assert_class(x, "data.frame")
  if (inherits(x, "grouped_df")){
    abort(c("`x` should not be a grouped.",
            "i" = "Provide grouping spec using `.by` arg.")
          )
  }

  if (any(vapply(colnames(x), \(y) endsWith(y, "__"), logical(1)))){
    abort("Column names should not end with two underscores.")
  }

  # capture expressions (creates: ddd) -----------------------------------------
  ddd = rlang::enquos(...)

  # assertions (creates: *_is_missing) -----------------------------------------
  order_by_is_missing = missing(.order_by)
  by_is_missing       = missing(.by)
  frame_is_missing    = missing(.frame)

  # handle by (creates: by, by_str) --------------------------------------------
  # direct column spec like: species
  # two or more columns like: c(species, species2)
  if (!by_is_missing) {
    by = rlang::enexpr(.by)

    if (rlang::is_call(by)) {
      # case: starts with 'c'
      first_thing = rlang::as_string(by[[1]])
      if (!first_thing == "c"){
        abort(c("`.by` is not parsable or wrong format",
                  "i" = "Either a single column ( ex: `species` ) or ",
                  "i" = "A set of columns ( ex: `c(sepal_length, sepal_width)` )"
                 ))
      }

      by_str =
        lapply(by, identity) %>%
        tail(-1) %>%
        vapply(rlang::as_string, character(1))

    } else {
      # case: direct column
      by_str = rlang::as_string(by)
    }

    res = x
  }

  # handle frame ---------------------------------------------------------------
  # creates: res, frame_obj, order_by, order_by_str, frame_type
  if (!frame_is_missing){

    # Has to come from `rows_between` or `range_between`
    checkmate::assert_multi_class(
      .frame,
      c("rows_between_frame", "range_between_frame")
      )

    frame_type = ifelse(inherits(.frame, "rows_between_frame"),
                        "rows_between",
                        "range_between"
                        )

    # frame requires order
    if (order_by_is_missing){
      abort("`.order_by` is required when `.frame` is specified.")
    }

    order_by = rlang::enexpr(.order_by)

    # rows_between: can have one or more columns to order by
    # rows_between: rows to be ordered before mutate operation
    if (frame_type == "rows_between"){

      if (rlang::is_call(order_by)){
        # case: starts with 'c' or 'desc'
        first_thing = order_by[[1]]
        if (! (rlang::as_string(first_thing) %in% c("c", "desc"))) {
          abort(c("`.order_by` is not parsable",
                  "i" = "Either a single column (ex: `species`) or ",
                  "i" = "A set of columns (ex: `c(sepal_length, sepal_width)`)"
                 ))
        }

        # sorting rows
        if (first_thing == "c"){
          res =
            x %>%
            dplyr::mutate(rn__ = dplyr::row_number()) %>%
            dplyr::arrange(!!!tail(lapply(order_by, identity), -1))

        } else {
          # proto: desc(Sepal.Length)
          res =
            x %>%
            dplyr::mutate(rn__ = dplyr::row_number()) %>%
            dplyr::arrange(!!order_by)
        }
      } else {
        # case: direct columns
        res =
          x %>%
          dplyr::mutate(rn__ = dplyr::row_number()) %>%
          dplyr::arrange(!!order_by)
      }

      frame_obj = .frame
    }

    # range_between: can have one column to order by, may have desc wrapped
    # range_between: rows NOT to be ordered before mutate operation
    if (frame_type == "range_between"){

      if (rlang::is_call(order_by)){
        # case: starts with 'desc'
        first_thing = rlang::as_string(order_by[[1]])
        if (! (length(order_by) == 2 && first_thing == "desc") ){
          abort(c("`.order_by` is not parsable or wrong format",
                  "i" = "Should be a single column",
                  "i" = "ex: `species`, `desc(species)`"
                  ))
        }

        order_by_str = rlang::as_string(order_by[[2]])

        res =
          x %>%
          dplyr::mutate(rn__ = dplyr::row_number()) %>%
          dplyr::arrange(!!order_by)

      } else {
        # case: direct column
        first_thing = "not_important"
        order_by_str = rlang::as_string(order_by)

        res =
          x %>%
          dplyr::mutate(rn__ = dplyr::row_number()) %>%
          dplyr::arrange(!!order_by)
      }

      frame_obj = .frame

      is_desc = (first_thing == "desc")
      if (is_desc){
        frame_obj[[1]] = .frame[[2]]
        frame_obj[[2]] = .frame[[1]]
      }
    }
  }

  # handle order_by when frame is missing (creates: res) -----------------------
  if (frame_is_missing && !order_by_is_missing){

    order_by = rlang::enexpr(.order_by)
    if (rlang::is_call(order_by)){

      # case: starts with 'c' or 'desc'
      first_thing = rlang::as_string(order_by[[1]])
      if (! (first_thing %in% c("c", "desc"))) {
        abort(c("`.order_by` is not parsable",
                "i" = "Either a single column ( ex: `species` ) or ",
                "i" = "A set of columns ( ex: `c(sepal_length, sepal_width)` )"
               ))
      }

      # sorting rows
      if (first_thing == "c"){
        res =
          x %>%
          dplyr::mutate(rn__ = dplyr::row_number()) %>%
          dplyr::arrange(!!!tail(lapply(order_by, identity), -1))

      } else {
        # proto: desc(Sepal.Length)
        res =
          x %>%
          dplyr::mutate(rn__ = dplyr::row_number()) %>%
          dplyr::arrange(!!order_by)
      }
    } else {
      # case: direct column
      res =
        x %>%
        dplyr::mutate(rn__ = dplyr::row_number()) %>%
        dplyr::arrange(!!order_by)
    }
  }

  # handle complete ------------------------------------------------------------
  # `.complete` is TRUE or FALSE (same as NULL)
  checkmate::check_flag(.complete, null.ok = TRUE)
  if (is.null(.complete)) .complete = FALSE

  # ==== core mutate operation =================================================
  # Note: Execute standard mutate, grouped mutate, or sliding window frame
  # computations (rows_between or range_between via slider) with unnesting,
  # and restore original row order.

  # for cran checks
  slide_output__ = NULL

  # make sure res exists -------------------------------------------------------
  if (!rlang::env_has(nms = "res", inherit = FALSE)){
    res = x
  }

  # store row order in `rn__` and not in res -----------------------------------
  rn__ = NULL
  if ("rn__" %in% colnames(res)) {
    rn__ = dplyr::pull(res, rn__)
    res  = dplyr::select(res, -rn__)
  }

  # simple mutate -- no groups, no frame ---------------------------------------
  # since frame is missing, res is already ordered according to order_by if
  # order_by is not missing
  if (by_is_missing && frame_is_missing){
    res = dplyr::mutate(res, !!!ddd)
  }

  # mutate with groups -- no frame ---------------------------------------------
  if (!by_is_missing && frame_is_missing){
    res = dplyr::mutate(res, !!!ddd, .by = dplyr::all_of(by_str))
  }

  # mutate with frame ----------------------------------------------------------
  if (!frame_is_missing){

    if (frame_type == "rows_between"){
      if (by_is_missing){
        res =
          res %>%
          dplyr::mutate(slide_output__ =
            slider::slide(dplyr::pick(dplyr::everything()),
                          .f = ~ as.list(dplyr::summarise(.x, !!!ddd)),
                          .before = frame_obj[[1]],
                          .after  = frame_obj[[2]],
                          .complete = .complete
                          )
                        ) %>%
          remove_common_nested_columns(slide_output__) %>%
          tidyr::unnest_wider(slide_output__)

      } else {
        res =
          res %>%
          dplyr::mutate(slide_output__ =
            slider::slide(dplyr::pick(dplyr::everything()),
                          .f = ~ as.list(dplyr::summarise(.x, !!!ddd)),
                          .before = frame_obj[[1]],
                          .after  = frame_obj[[2]],
                          .complete = .complete
                          ),
            .by = dplyr::all_of(by_str)
            ) %>%
          remove_common_nested_columns(slide_output__) %>%
          tidyr::unnest_wider(slide_output__)
      }
    }

    if (frame_type == "range_between"){
      if (by_is_missing){
        res =
          res %>%
          dplyr::mutate(slide_output__ =
            slider::slide_index(
              dplyr::pick(dplyr::everything()),
              .f = ~ as.list(dplyr::summarise(.x, !!!ddd)),
              .i = dplyr::pick(dplyr::all_of(order_by_str))[[order_by_str]],
              .before = frame_obj[[1]],
              .after  = frame_obj[[2]],
              .complete = .complete
              )
            ) %>%
          remove_common_nested_columns(slide_output__) %>%
          tidyr::unnest_wider(slide_output__)

      } else {
        res =
          res %>%
          dplyr::mutate(slide_output__ =
            slider::slide_index(
              dplyr::pick(dplyr::everything()),
              .f = ~ as.list(dplyr::summarise(.x, !!!ddd)),
              .i = dplyr::pick(dplyr::all_of(order_by_str))[[order_by_str]],
              .before = frame_obj[[1]],
              .after  = frame_obj[[2]],
              .complete = .complete
              ),
            .by = dplyr::all_of(by_str)
          ) %>%
          remove_common_nested_columns(slide_output__) %>%
          tidyr::unnest_wider(slide_output__)
      }
    }
  }

  # setting the order right if order_by operated -------------------------------
    if (!is.null(rn__)) res = dplyr::arrange(res, !!rn__)

  # return ---------------------------------------------------------------------
  return(res)
}
