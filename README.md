# queue-pool - Pooling for synchronised access to multiple buffered queued.

[![Build Status](https://drone.io/github.com/japettyjohn/queue-pool/status.png)](https://drone.io/github.com/japettyjohn/queue-pool/latest)
[![GoDoc](https://godoc.org/github.com/japettyjohn/queue-pool?status.svg)](https://godoc.org/github.com/japettyjohn/queue-pool)

queue-pool was made originally for multiple buffered queues where records 
may be shared between queues but one record could only be provided once
by any given queue. This can still be done, but that a record is only
provided once is dependent on your use.
