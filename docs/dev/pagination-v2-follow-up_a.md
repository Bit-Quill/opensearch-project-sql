# Pagination in v2 SQL Engine

Implementing pagination in the OpenSearch SQL plugin can be partitioned along several dimensions:
1. SQL query type,
1. Size of queried dataset,
1. Number of open cursors,
1. SQL node load balancing and failover.

## SQL Query Type
The two main query types are filter-and-project and aggregation queries. 

V1 engine only supports cursors for filter-and-project queries.

Therefore, discussion of pagination in V2 engine will also be limited to filter-and-project queries.

Pagination of aggregation queries can be considered after V1 engine is deprecated.

## Size of Queried Dataset

OpenSearch provides several data retrival APIs that are optimized for different use cases.

At this time, SQL plugin concerns only uses simple search API and scroll API.

Simple search API will only return at most `window_size` number of documents where `window_size` is an index setting.

Scroll API requests will return all documents but can incur high memory costs on OpenSearch coordination node.

This document uses *under window_size* for scenarios that can be implemented with simple search API and
*over window_size* for scenarios that require scroll API to implement.

## Number of Open Cursors
In V1 engine, number of open cursors is limited by how many open scroll requests OpenSearch supports. All V1 cursor context is captured by combination of cursor id returned to the client and scroll context maintained by OpenSearch node. SQL node does not need to do any resource management.

This is not possible in V2 engine. It supports queries that require in-memory processing, such as `SELECT fieldA, fieldA*3 FROM indexA`. Therefore, it needs to maintain some context for each cursor. Therefore, it will need manage number of open cursors.

In the simplest case, each node supports only *one open cursor*. This is invaluable for development but minimum viable release will need to support *multiple open cursors*.

### SQL Node Maximum Open Cursors Specification
This section specifies behaviour of a SQL plugin node regarding multiple open cursors.

1. Each cursor will have a keep alive timeout.
1. Each node will allow a maxmimum number of open cursors.
1. When a node reached maximum number of open cursors and a client requests a new cursor, the client will recieve an error response.
1. Each page request refreshes the keep alive timeout.
1. When a cursor's keep alive timeout expires, the SQL plugin will delete the cursor context and clean up corresponding OpenSearch resources.
1. Requests with expired cursor id  will recieve similar error response to requests with invalid cursor id.
1. Cursor keep alive timeout is set to the  `plugins.sql.cursor.keep_alive` SQL plugin setting.
1. Maximum number of open cursors is defined by `plugins.sql.cursor.max_open` SQL plugin setting.

Note that logically `plugins.sql.cursor.max_open` governs the size of "cursor resource pool." 
While each cursor is bound a to a single SQL plug node, this is a per-node setting.
It will become a cluster setting if we add shared cursor context as described in the following section. 


## SQL Node Load Balancing
V1 SQL engine supports *sql node load balancing* -- a cursor request can be routed to any SQL node in a cluster.

This is trivial in V1 engine because of context-free nature of V1 cursors but needs to be implemented for V2 cursors.

Implementing this for V2 cursors will make cursor context shared between all nodes in the cluster.
Any node that recieves a SQL request with a cursor id will then responsible for updating the shared cursor context.

This adds significant complexity. In discussion with Amazon team it was flagged as not necessary to deprecate V1 engine.

# Delivery Breakdown

## Phase 1 - Minimum V1 Parity
1. filter-and-project queries, under window_size, one open cursor, single coordination node.
1. filter-and-project queries, under window_size, multiple open cursors, single coordination node.
1. filter-and-project queries, over window_size, one open cursor, single coordination node.
1. filter-and-project queries, over window_size, multiple open cursors, single coordination node.

## Phase 3 - SQL Node Load Balancing

1. filter-and-project queries, under window_size, one open cursor, sql node load balancing. 
1. filter-and-project queries, over window_size, one open cursor, sql node load balancing.
1. filter-and-project queries, under window_size, multiple open cursors, sql node load balancing.
1. filter-and-project queries, over window_size, multiple open cursors, sql node load balancing

## Phase 4 - Aggregation Queries
Pagination of aggregation queries will be considered separately. V1 engine does not support this therefore its not necessary to deperecate it.