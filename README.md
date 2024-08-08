# call_reporter_duckdb_superset
---

This is a simple project using [dbt-duckdb](https://github.com/duckdb/dbt-duckdb) and [Apache Superset](https://superset.apache.org/) to build an all-in-one Modern Data Stack which can be used on your laptop without Cloud Stacks.

It's inspired by this article: [Modern Data Stack in a Box with DuckDB](https://duckdb.org/2022/10/12/modern-data-stack-in-a-box.html).

In this particular implementation, we're exposing NCUA call report data.

![Modern Data Stack in a Box](resources/mds-in-a-box.jpg "Modern Data Stack in a Box")

# Requirements

- [Docker](https://docs.docker.com/engine/install/)
- [Docker Compose](https://docs.docker.com/compose/install/)

# How to access

The superset dashboard is accessible [here](http://ec2-34-229-168-221.compute-1.amazonaws.com).

# TODO
[ ] Remove all the JaffleShop stuff.
[ ] Create proper transformed views with dbt.
[ ] Import more NCUA files.