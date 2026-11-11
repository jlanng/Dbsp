# The service's scale rig

Two real `ServiceNode` processes on the Kafka backend, a reference table of wide rows
shared by twenty pipelines, a few of them holding large state, a steady input — and then
what production does to it: a cold standby, `kill -9` of the leader, a takeover from
under a healthy leader, and one node started from nothing. It measures; it asserts only
that each step happened. The numbers it has produced, and what was changed because of
them, are in [docs/design/enrichment-service.md](../../docs/design/enrichment-service.md)
(*As built*, S6, *Scale and a production-shaped Kafka*).

```
docker compose --profile connectors up -d
dotnet build -c Release benchmarks/Incremental.Service.Scale
cd benchmarks/Incremental.Service.Scale/bin/Release/net10.0
dotnet Incremental.Service.Scale.dll run --rows 1000000 --state-keys 1000000 \
    --broker-container incremental-kafka-1 --out /tmp/scale-1m.json
```

Run the built binary, not `dotnet run`: the rig starts its nodes from the same directory
and kills them itself. A run at one million rows takes about three minutes and deletes
its topics when it ends (`--keep-topics` leaves them).

## What a run is

| | |
|---|---|
| the table | `parties`: `--rows` rows of 21 columns, about 520 bytes of JSON each, every string its own object, one secondary index; written through the table's writer in versions of `--load-batch` (20,000) |
| a second table | `parties-feed`: 2,000 rows filled by the `kafka` loader from a topic, so the loader's consumer is in every run |
| the pipelines | `--pipelines` (20) from a plugin the first leader imports and the other node gets from the blob topic. `--joins` (2) are `order-enrichment`: orders as keyed upserts joined to the party — two state entries per order. `--aggregates` (2) are `party-exposure`: a running total per party. The rest are `order-lookup`: enrich on arrival, no state, reading from the end of the topic |
| the state | `--state-keys` orders produced before anything is measured; each stateful pipeline reads them all |
| the input | `--rate` (2,000) orders a second, each a new revision of an order the stateful pipelines hold, from the first steady second to the end of the run — through every kill |
| table churn | `--churn` (500) rows a second rewritten on the leader, five versions a second |
| the nodes | server GC, eight heaps (`SCALE_GC_HEAPS`), DATAS off; `TakeoverAfter` `--takeover-after-ms` (3,000), heartbeat 1 s; twelve state partitions, four row partitions per table |

Then, in order:

1. the first node leads an empty deployment;
2. the table is loaded — time, rows a second, bytes on the broker (`--broker-container`
   names the container to ask; without it the byte counts are left out), heap after a
   full collection;
3. the pipelines are deployed with nothing to read — heap again: what twenty circuits add
   to one copy of the rows;
4. the state is filled — time, heap, bytes;
5. a second node starts cold and becomes warm — time, heap, working set;
6. `--steady-seconds` of steady input — the standby's state and table lag, sampled
   every second;
7. the leader is killed with `kill -9` — detection, every stage of the takeover, each
   pipeline's start, and the first record in every pipeline's sink after the kill;
8. the dead node comes back as a cold standby and becomes warm;
9. it takes over from under the healthy leader — the same stages, and how long each
   sink went without a record (the old leader writes until it is fenced, so "the first
   record after the command" is its own);
10. everything is killed and a third node starts with an empty cache and nobody to
    follow — time to lead and to the first record in every sink.

`--single` stops after step 4, kills the node and does step 10: for a table this
machine cannot hold twice.

## Reading the numbers

- `heap` is the managed heap after a forced, compacting collection: what is live.
  `rss` is the working set at that moment, which with server GC is well above the
  heap and also holds the Kafka client's buffers: about twice it on the standby
  (3.6 GB for 1.8 GB live at a million orders of state), and on the leader 2.7 to 3.6
  times it (7.8 GB for 2.2 GB live at a million rows, 12.5 GB for 4.7 GB at three
  million). A node needs the working set: size a container from `rss`, not from `heap`.
- `first-sink-write` is measured where a reader of the sink would see it: the arrival of
  the first record that was produced after the moment in question.
- `stages` are the takeover's own clock (`NodeStatus.Leadership.Kafka.LastTakeover` and
  `NodeStatus.LastTakeover`); `detection` is from the kill to the standby deciding to
  take over, which is `TakeoverAfter`.
- `pipeline-start` is `PipelineStatus.LastStart`: `built while standing by` says the
  takeover started a circuit the standby already had.

The run stops, rather than push the machine into swap, when less than 1.5 GB of memory
is available. On a 30 GB machine with the broker on it, two nodes fit at three million
rows and one node at five.

## Another broker

```
# three brokers, replication factor 3, min.insync.replicas 2
docker compose --profile kafka3 up -d
dotnet Incremental.Service.Scale.dll run --bootstrap localhost:9192,localhost:9292,localhost:9392 \
    --replication 3 --rows 200000 --state-keys 200000
```

### A broker that refuses clients it does not know

The `kafka-sasl` profile is one broker on `localhost:9492` that accepts only
SASL/OAUTHBEARER and denies whatever no ACL allows. `--oauth <principal>` makes every
node authenticate as that principal with a token supplied **only** through the refresh
handlers on `KafkaLogOptions.ConfigureProducer`, `ConfigureConsumer` and `ConfigureAdmin`
— the client configuration names the mechanism and carries no credential. That is the
shape of MSK's IAM authentication. A client the service created without the connection's
handler would never authenticate, and the run would stop there; a token lives
`--oauth-lifetime-seconds` (20), so a run also proves that every client refreshes it.

```
docker compose --profile kafka-sasl up -d
./grant-acls.sh node scalesasl1 rig          # from benchmarks/Incremental.Service.Scale
dotnet Incremental.Service.Scale.dll run --bootstrap localhost:9492 --prefix scalesasl1 \
    --oauth node --oauth-rig rig --rows 100000 --state-keys 100000 --steady-seconds 60
```

When done: `docker compose rm -sf kafka-sasl` (and `kafka3-1 kafka3-2 kafka3-3`) —
`docker compose --profile … down` would stop Redis and the connector stack too.

`grant-acls.sh` is the permission set a node needs, as Kafka ACLs, with the reason for
each; the guide's *On Amazon MSK* has the same as IAM actions. The rig's own clients
(it creates the topics, produces the input and reads the sinks) run as `--oauth-rig`,
so the node's principal holds exactly what the script grants it.
