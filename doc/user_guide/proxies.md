## AAF Proxies

The Advanced Analytics Framework (AAF) uses _Object Proxies_ to manage temporary objects.

An _Object Proxy_
* Encapsulates a temporary object
* Provides a reference enabling using the object, i.e. its name incl. the database schema or the path in the BucketFS
* Ensures the object is removed when leaving the current scope, e.g. the Query Handler.

All Object Proxies are derived from class `exasol.analytics.query_handler.context.proxy.object_proxy.ObjectProxy`:
* `BucketFSLocationProxy` encapsulates a location in the BucketFS
* `DBObjectNameProxy` encapsulates a database object, e.g. a table

