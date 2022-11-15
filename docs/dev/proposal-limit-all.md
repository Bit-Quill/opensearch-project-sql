With changes specified in this document, SQL plugin clients will always be aware if SQL plugin returned all documents that match a given query and when it does not, how to retrieve all documents.

Both SQL plugin and OpenSearch limit how many results are returned when paging is not used. However, SQL plugin does not communicate to the client when these limits are hit. This leaves clients with the incorrect impression that all matching data was returned.

This document describes syntax changes for SQL. The same behaviour can be added to PPL but the syntax will be different.

# Use Cases

The following use cases should notify users when incomplete data is returned.
* opensearchsql-cli
* workbench
* observability plugin
* JDBC driver
* ODBC driver

# Definitions
* `totalSize` &mdash; number of documents that match given query.
* `index.max_result_window` &mdash; maximum number of documents that OpenSearch will return without a scroll request. A cluster setting.
* `query.size_limit` &mdash; maximum number of rows that SQL plugin will return without a cursor request. A SQL plugin setting.

# Assumptions
1. Query is not an aggregation query.
2. `query.size_limit < index.max_result_window`

# Desired behaviour

## Without cursor request, without LIMIT clause

```sql
SELECT <...> FROM <...> WHERE <...> 
```
SQL Plugin returns up to `query.size_limit` rows. 

[Additional](#more-data-available-response) information is included when `query.size_limit < totalSize`

## Without cursor request, with LIMIT clause

```sql
SELECT <...> FROM <...> WHERE <...> LIMIT X 
```
Returns up to `X` number of rows.

If `X` is less than `index.max_window_size` all rows are returned.

If `X` is greater than `index.max_window_size`, only first `X` rows are returned and response indicates that more data is [available](#more-data-available-response).

```sql
SELECT <...> FROM <...> WHERE <...> LIMIT X OFFSET Y
```

If `X + Y < index.max_window_size` return all the rows without additional information.

If `index.max_window_size < X + Y` returns `index.max_window_size` rows with additional [information](#more-data-available-response).

```sql
SELECT <...> FROM <...> WHERE <...> LIMIT ALL
```
Overrides `query.size_limit` and returns all available rows up to `index.max_window_size`. Includes more [available response](#more-data-available-response) if applicable.

```sql
SELECT <...> FROM <...> WHERE <...> LIMIT ALL OFFSET Y
```
Overrides `query.size_limit` and returns all available rows up to `index.max_window_size`. Includes more [available response](#more-data-available-response) if applicable.

More data available will be included even when `index.max_window_size < totalSize` but `totalSize - Y < index.max_window_size` -- in OpenSearch, `index.max_window_size` limits how many documents OpenSearch considers not how many are returned. To return `X` documents, while skipping `Y`, `X + Y` documents need to be considered.

## With cursor request

Cursor behaviour will be described in a follow-up document.

# More Data Available Response

Client software needs to notify the end-user or adjust its request when SQL plugin does not return all matching rows because `totalSize` is greater than `query.size_limit`.

The following additions are sufficient for client software to detect such cases and optionally adjust their query:
1. SQL plugin response will include `totalSize` property when the plugin detects that more documents match the query than it is configured to return.
2. It will include `'fetchAllMethod' : 'limitAll'` when all documents can be returned using `LIMIT ALL` clause &mdash; i.e. when `query.size_limit < totalSize < index.max_result_window `
3. It will include `'fetchAllMethod' : 'cursor'` when `index.max_result_window < totalSize` and all documents can only be returned using a cursor request.

opensearchsql-cli, workbench, or observability plugin can present this information to end-user and, optionally, generate an alternative query.

JDBC and ODBC drivers can return this information to their clients.

# Additional Information

https://github.com/opensearch-project/sql/issues/703 -- discusses addition of `LIMIT ALL` behaviour.
