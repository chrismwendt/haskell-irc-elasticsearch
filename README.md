# Overview

This program indexes all IRC chat logs under some `archive` directory (not included) into Elasticsearch. It uses `conduit` to cleanly separate the producer (parsing messages from files) and consumer (indexing messages into Elasticsearch). It also makes good use of `buffer` from the `stm-conduit` package to decouple the performance of the producer and consumer.

Here is a screenshot of the performance with different numbers of documents per HTTP request:

![](/performance.png)

# TODO

- Handle multiple identical messages from the same user within the same second (the checksum will be the same and only one document will show up in Elasticsearch)
