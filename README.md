# read-anomaly
A bug reproducer for the AWS DynamoDB service

## Purpose
This reproducer demonstrates a read anomaly that can occur when querying a partition key during concurrent write transactions.

```
READ-COMMITTED
Read-committed isolation ensures that read operations always return committed values for an item - the read will never present a view to the item representing a state from a transactional write which did not ultimately succeed. Read-committed isolation does not prevent modifications of the item immediately after the read operation.

The isolation level is read-committed between any transactional operation and any read operation that involves multiple standard reads (BatchGetItem, Query, or Scan). If a transactional write updates an item in the middle of a BatchGetItem, Query, or Scan operation, the subsequent part of the read operation returns the newly committed value (with ConsistentRead) or possibly a prior committed value (eventually consistent reads).

Source: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html
```

The Query operation uses the read-committed isolation level and can return prior committed values.  

However, under concurrent transactions, it can sometimes return ***empty results*** — even though many committed items exist that should be returned.<br/>
See `ReadAnomaly.java`, lines 173–177.

## Prerequisites
- Java version 21 (or higher)
- An AWS account
- AWS CLI set up with a profile(s) for SSO login

## DynamoDB Setup
Create a new table with: 
- Name: **test_table**
- Partition key: **id** (type: String)
- Sort key: **version** (type: Number)

Leave all other settings as default.

> These names can be configured in `ReadAnomaly.java`, lines 31–33.

## Configure `ReadAnomaly.java`
- Edit lines 28–29 to enter your profile name and select the correct AWS region.
- Optional: On line 30, configure the number of *children* that will concurrently run write transactions.
- Optional: On line 31, choose the consistency level of the query request.

> Note: A small number of *children* will not create enough concurrent transactions to trigger the read anomaly. Try using 30 to 50 *children*.

## Run

Log in to AWS:
```
> aws sso login
```

Start the application:
```
> gradlew run
```

Typical output:
```
Setting up the database with 1 parent and 30 children...
Database setup completed, verifying initial state...
Running the test...
Read anomaly for id: parent - abort task
Read anomaly for id: parent - abort task
Test run completed, verifying results...
Expected id: parent, version: 24 to exist
```

> If you want to run the application again, don't forget to purge your table of all data.

## Explanation

For brevity, let's assume we have configured the application to use 2 children.

In the first phase, the application will set up and verify the initial state of the database, which will look like this:

| id | version | action
|---|---|---
| parent | 0 | create
| child-0 | 0 | create
| child-1 | 0 | create

In the second phase, the application creates one thread per child. All those threads are then started simultaneously to maximize the number of concurrently running transactions.

Each thread will attempt to write two new rows into the table within a transaction — one for the parent and one for the child. It will read the current version of each row and then create the next version.

Example:<br/>
If the latest version of the `parent` is 1 and the child's ID is `child-0`, then the thread will attempt to write:

| id | version | action
|---|---|---
| parent | 2 | child-0
| child-0 | 1 | parent

Only one thread will create a row for a particular child, so no contention occurs there.<br/>
Every thread will attempt to create a new row for the parent, so contention is expected!

DynamoDB can abort a transaction by throwing a *TransactionCanceledException* with a reason such as *TransactionConflict* or *ConditionalCheckFailed*. The first occurs when multiple transactions include the same row, and the second when a transaction contains stale data.

Regardless of the reason, the thread will retry the transaction by first reading the current version of the parent and then creating the next version. Eventually, this will succeed for all threads.

To read the current version of the parent or one of the children, this query is used:

```java
final var request = QueryRequest.builder()
		.tableName(TABLE)
		.keyConditionExpression("%s = :id".formatted(ID))
		.expressionAttributeValues(Map.of(":id", fromS(id)))
		.scanIndexForward(false)
		.limit(1)
		.consistentRead(CONSISTENT_READ)
		.build();
```

> By scanning backwards and limiting the number of rows to 1, we will get the latest committed (or possibly a prior committed) version.

In the final phase of the application, the end state of the table is verified. The parent should have 1 + CHILDS versions, and each child should have 2 versions.
