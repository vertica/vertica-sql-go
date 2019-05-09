# vertica-sql-go

[![License](https://img.shields.io/badge/License-Apache%202.0-orange.svg)](https://opensource.org/licenses/Apache-2.0)

vertica-sql-go is a native Go adapter for the Vertica (http://www.vertica.com) database.

Please check out [release notes](https://github.com/vertica/vertica-sql-go/releases) to learn about the latest improvements.

vertica-sql-go is currently in alpha stage; it has been tested for functionality and has a very basic test suite. Please use with caution, and feel free to submit issues and/or pull requests (Read up on our [contributing guidelines](#contributing-guidelines)).

vertica-sql-go has been tested with Vertica 9.2.0+ and Go 1.11.2.

## Release Notes

* As this driver is still in alpha stage, we reserve the right to break APIs and change functionality until it has been stablilized.


## Installation

Source code for vertica-sql-go can be found at:

    https://github.com/vertica/vertica-sql-go

Alternatively you can use the 'go get' variant to install the package into your local Go environment.

```sh
go get github.com/vertica/vertica-sql-go
```


## Usage

As this library is written to Go's SQL standard, usage is compliant with its methods and behavioral expectations.

### Importing

First ensure that you have the library checked out in your standard Go hierarchy and import it.

```Go
import (
    "context"
    "database/sql"
    "github.com/vertica/vertica-sql-go"
)
```

### Setting the Log Level

The vertica-sql-go driver supports multiple log levels, as defined in the following table

| Log Level (int) | Log Level Name | Description |
|-----------------|----------------|-------------|
| 0               | TRACE          | Show function calls, plus all below |
| 1               | DEBUG          | Show low-level functional operations, plus all below |
| 2               | INFO           | Show important state information, plus all below |
| 3               | WARN           | (default) Show non-breaking abnormalities, plus all below |
| 4               | ERROR          | Show breaking errors, plus all below |
| 5               | FATAL          | Show process-breaking errors |
| 6               | NONE           | Disable all log messages |

and they can be set programatically by calling the logger global level itself
```Go
logger.SetLogLevel(logger.DEBUG)
```
or by setting the environment variable VERTICA_SQL_GO_LOG_LEVEL to one of the integer values in the table above. This must be done before the process using the driver has started as the global log level will be read from here on start-up.


### Creating a connection

```Go
connDB, err := sql.Open("vertica", myDBConnectString)
```
where *myDBConnectString* is of the form:

```
vertica://(user):(password)@(host):(port)/(database)?(queryArgs)
```
Currently supported query arguments are:

| Query Argument | Description | Values |
|----------------|-------------|--------|
| use_prepared_statements    | whether to use client-side query interpolation or server-side argument binding | 1 = (default) use server-side bindings |
|                |             | 0 = user client side interpolation **(LESS SECURE)** |
| tlsmode            | the ssl/tls policy for this connection | 'none' (default) = don't use SSL/TLS for this connection |
|                |                                    | 'server' = server must support SSL/TLS, but skip verification **(INSECURE!)** |
|                |                                    | 'server-strict' = server must support SSL/TLS |

To ping the server and validate a connection (as the connection isn't necessarily created at that moment), simply call the *PingContext()* method.

```Go
ctx := context.Background()

err = connDB.PingContext(ctx)
```

If there is an error in connection, the error result will be non-nil and contain a description of whatever problem occurred.

### Performing a simple query

Performing a simple query is merely a matter of using that connection to create a query and iterate its results.
Here is an example of a query that should always work.

```Go
rows, err := connDB.QueryContext(ctx, "SELECT * FROM v_monitor.cpu_usage LIMIT 5")

defer rows.Close()
```

**IMPORTANT** : Just as with connections, you should always Close() the results cursor once you are done with it. It's often easier to just defer the closure, for convenience.

### Performing a query with arguments

This is done in a similar manner on the client side.

```Go
rows, err := connDB.QueryContext(ctx, "SELECT name FROM MyTable WHERE id=?", 21)
```

Behind the scenes, this will be handled in one of two ways, based on whether or not you requested client interpolation in the connection string.

With client interpolation enabled, the client library will create a new query string with the arguments already in place, and submit it as a simple query.

With client interpolation disabled (default), the client library will use the full server-side parse(), describe(), bind(), execute() cycle.

### Reading query result rows.

As outlined in the GoLang specs, reading the results of a query is done via a loop, bounded by a .next() iterator.

```Go
for rows.Next() {
    var nodeName string
    var startTime string
    var endTime string
    var avgCPU float64

    rows.Scan(&nodeName, &startTime, &endTime, &avgCPU)

    // Use these values for something here.
}
```

If you need to examine the names of the columns, simply access the Columns() operator of the rows object.

```Go
columnNames, _ := rows.Columns()

for _, columnName := range columnNames {
        // use the column name here.
}
```

### Performing a simple execute call

This is very similar to a simple query, but has a slightly different result type. A simple execute() might look like this:

```Go
res, err = connDB.ExecContext(ctx, "DROP TABLE IF EXISTS MyTable")
```

In this instance, *res* will contain information (such as 'rows affected') about the result of this execution.

### Performing an execute with arguments

This, again, looks very similar to the query-with-arguments use case and is subject to the same effects of client-side interpolation.

```Go
res, err := connDB.ExecContext(
        ctx,
        "INSERT INTO MyTable VALUES (?)", 21)
```

### Server-side prepared statements

**IMPORTANT** : Vertica does not support executing a command string containing multiple statements using server-side prepared statements.

If you wish to reuse queries or executions, you can prepare them once and supply arguments only.

```Go
// Prepare the query.
stmt, err := connDB.PrepareContext(ctx, "SELECT id FROM MyTable WHERE name=?")

// Execute it with this argument.
rows, err = stmt.Query("Joe Perry")
```

**NOTE** : Please note that this method is subject to modification by the 'interpolate' setting. If the client side interpolation is requested, the statement will simply be stored on the client and interpolated with arguments each time it's used. If not using client side interpolation (default), the statement will be parsed and described on the server as expected.

### Transactions

The vertica-sql-go driver supports basic transactions as defined by the GoLang standard.

```Go
// Define the options for this transaction state
opts := &sql.TxOptions{
    Isolation: sql.LevelDefault,
    ReadOnly:  false,
}

// Begin the transaction.
tx, err := connDB.BeginTx(ctx, opts)
```
```Go
// You can either commit it.
err = tx.Commit()
```
```Go
// Or roll it back.
err = tx.Rollback()
```
The following transaction isolation levels are supported:

 * sql.LevelReadUncommitted <sup><b>&#8224;</b></sup>
 * sql.LevelReadCommitted
 * sql.LevelSerializable
 * sql.LevelRepeatableRead <sup><b>&#8224;</b></sup>
 * sql.LevelDefault

 The following transaction isolation levels are unsupported:

 * sql.LevelSnapshot
 * sql.LevelLinearizable

 <b>&#8224;</b> Although Vertica supports the grammars for various transaction isolation levels, some are internally promoted to stronger isolation levels.


## License

Apache 2.0 License, please see `LICENSE` for details.

## Contributing guidelines

Have a bug or an idea? Please see `CONTRIBUTING.md` for details.

## Acknowledgements

We would like to thank the creators and contributors of the vertica-python library, and memebers of the Vertica team, for their help in understanding the wire protocol.
