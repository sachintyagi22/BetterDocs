#Query to fetch repositories

```
curl 'http://172.16.12.201:9201/repository/_search?size=750' --data '{"query":{"match_all":{}},"sort":[{"stargazersCount":{"order":"desc"}}]}'
```

#Search by import

```
curl 'http://172.16.12.201:9201/betterdocs/_search?size=50' --data '{"query":{"bool":{"must":[{"term":{"custom.tokens.importName":"java.nio.MappedByteBuffer"}}],"must_not":[],"should":[]}},"sort":[{"score":{"order":"desc"}}]}'
```

#Search by import - with wildcard

```
curl 'http://172.16.12.201:9201/betterdocs/_search?size=50' --data '{"query":{"bool":{"must":[{"wildcard":{"custom.tokens.importName":"*FileChannel"}},{"wildcard":{"custom.tokens.importName":"*MappedByteBuffer"}}],"must_not":[],"should":[]}},"sort":[{"score":{"order":"desc"}}]}'
```

#Query to fetch file

```
curl 'http://172.16.12.201:9201/sourcefile/_search?size=1' --data '{"query":{"term":{"typesourcefile.fileName":"apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/commitlog/CommitLogSegment.java"}}}'
```

#Query to fetch frequently used methods

```
curl 'http://172.16.12.201:9201/fpgrowth/patterns/_search?size=10' --data '{"query":{"filtered":{"query":{"bool":{"must":[{"term":{"body":"java.nio.channels.FileChannel"}}]}},"filter":{"and":{"filters":[{"term":{"length":"1"}}],"_cache":true}}}},"sort":[{"freq":{"order":"desc"}},"_score"]}'
```

#Query to fetch documentation

```
curl 'http://192.168.2.28/api/java/nio/channels/FileChannel.html'
```