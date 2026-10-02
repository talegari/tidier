# tidier 0.3.0

Multiple breaking changes:

* Support for remote tbls is removed as dbplyr 2.6.0 supports by, order by, and frame (noting that dbplyr 2.6.0 supports rows between, but not range between).
* `mutate` loses the `index` argument to match the exact semantics of Spark SQL.
* `mutate` is single-threaded. Users are advised to use the `tidyr::nest` + parallel map (`furrr::future_map`) + `tidyr::unnest` pattern when there are many groups and parallelization is necessary.

Enhancements:

* Accuracy of the implementation is tested against duckdb.

# tidier 0.2.0

* `tidier`'s mutate now supports same syntax over 'dbplyr' tbls. 

# tidier 0.1.0 (on github: 2023-06-01)

* Exposed slider's `.complete` argument in `tidier::mutate`
* bugfix: `mutate` can now modify a column (same name) in sliding operation. 

# tidier 0.0.1

* Added a `NEWS.md` file to track changes to the package.
