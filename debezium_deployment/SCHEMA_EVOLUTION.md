# Surviving a source `ALTER TABLE`: Postgres → Debezium → Schema Registry → Flink

This is the measured walkthrough behind `./deploy.sh evolve`. The question it
answers: when someone adds, renames or drops a column on `impressions.events`,
what happens at each hop, what is refused, and does the Flink job keep
running?

## The layers

| hop | what carries the schema | who enforces what |
| --- | --- | --- |
| Postgres → Debezium | pgoutput *relation messages* in the WAL (column names/types at that WAL position) plus a JDBC catalog lookup for nullability/defaults | `REPLICA IDENTITY FULL` so deletes carry the row |
| Debezium → Kafka | `io.confluent.connect.avro.AvroConverter`: one Avro record schema per table shape, registered under `cdc.impressions.events-value`; each message carries the schema **id** (5 bytes) not the schema | the converter registers a new version whenever the Connect schema changes |
| Schema Registry | subject → ordered versions; compatibility mode `BACKWARD` (set globally and per subject by `deploy.sh register`) | rejects a new version an existing *reader* could not use on old data (HTTP 409); the connector task then fails rather than publish |
| Kafka → Flink | `'format' = 'avro-confluent'`: Flink fetches the **writer** schema by id from the registry; the **reader** schema is derived from the table DDL in `cdc_sql.source_ddl` | Avro schema resolution: reader fields absent from the writer take their default (`null`), writer fields absent from the reader are skipped |

`BACKWARD` is the right default for a CDC topic with independent consumers:
it guarantees a consumer on the newest schema can read everything already in
the topic, so consumers upgrade first and producers follow. Debezium's own
output cooperates: every nullable column becomes an optional Avro field with
`default: null`, which is exactly what BACKWARD needs for ADD and DROP.

## What `./deploy.sh evolve` did (measured, 2026-09-04)

`schema_changes.sql` runs three ALTERs, each followed by an INSERT (Debezium
registers a new version only when it *emits* a record with the new shape, so
an ALTER with no DML after it produces nothing).

```
Schema versions BEFORE:
Subject cdc.impressions.events-value: versions [1]
  v1 (id 2): event_id:long default=0, user_id:string, impression_id:string, page_type:int,
             event_type:string, event_date:int, event_hour:int, event_minute:int,
             event_second:int, created_at:string default=..., updated_at:string default=...,
             __deleted:string? default=null, __op:string? default=null,
             __table:string? default=null, __source_ts_ms:long? default=null

Applying schema_changes.sql ...
ALTER TABLE / INSERT 0 1 / ALTER TABLE / ALTER TABLE / INSERT 0 1 / ALTER TABLE / INSERT 0 1

Schema versions AFTER:
Subject cdc.impressions.events-value: versions [1,2,3,4]
  v2 (id 7): ... event_hour:int, event_minute:int? default=null, event_second:int? default=null,
             ..., referrer:string? default=null, __deleted ...
  v3 (id 8): ... event_hour:int, minute_of_hour:int? default=null, event_second:int? default=null,
             ..., referrer:string? default=null, ...
  v4 (id 9): ... event_hour:int, minute_of_hour:int? default=null,
             ..., referrer:string? default=null, ...

Connector task state:
  connector: RUNNING
  task 0: RUNNING
```

Reading the three transitions:

1. **`ADD COLUMN referrer VARCHAR(200)` → v2.** `referrer` appears as
   `string? default=null`. BACKWARD accepts it (a reader on v2 reading a v1
   record fills `referrer` with `null`). Flink's DDL does not declare
   `referrer`, so the reader schema skips it; the job does not notice.
2. **`RENAME COLUMN event_minute TO minute_of_hour` → v3.** Avro has no
   rename: v3 drops `event_minute` and adds `minute_of_hour`, both optional.
   BACKWARD accepts because the dropped field had a default in the *reader's*
   view and the added one has one too. Flink still declares `event_minute INT`;
   from v3 on it resolves to `NULL` instead of failing. Had the renamed column
   kept `NOT NULL` with no default, the new field would have been a *required*
   int and the registry would have refused it (see below), which is why the
   script drops `NOT NULL` first.
3. **`DROP COLUMN event_second` → v4.** Accepted for the same reason; Flink's
   declared `event_second INT` reads `NULL`. The aggregate needs only
   `page_type`, `impression_id`, `__op` and `__source_ts_ms`, none of which
   moved, so the counts keep flowing.

An observation worth knowing about Debezium: **v2 already shows
`event_minute` and `event_second` as optional**, before they were renamed or
dropped. The three ALTERs ran within milliseconds; Debezium takes column
*names and types* from the WAL relation message at each position (correct),
but *nullability* from a live JDBC catalog lookup at processing time, by which
point `event_minute` no longer existed and `event_second` was gone, so both
were treated as optional. Harmless here (optional is the safe direction), but
it means the registered schema is not a faithful snapshot of the table at that
WAL position when DDL is applied in quick succession.

## What the gate refuses (`./deploy.sh evolve-incompatible`, measured)

Registering the latest schema plus a **required** field with no default:

```
POST http://localhost:8085/subjects/cdc.impressions.events-value/versions
HTTP 409
"Schema being registered is incompatible with an earlier schema for subject
 \"cdc.impressions.events-value\", details: [{errorType:'READER_FIELD_MISSING_DEFAULT_VALUE',
 description:'The field 'mandatory_no_default' at path '/fields/15' in the new schema has no
 default value and is missing in the old schema' ...}, {compatibility: 'BACKWARD'}]"
Versions are unchanged: [1]
```

The same rejection hits Debezium if the source gets a `NOT NULL` column with
no default and no nullable step (the connector task goes `FAILED` with the
409 in its trace; `./deploy.sh connectors` shows it). That is the gate doing
its job: the incompatible shape never reaches the topic, and the fix is on
the producer side (add a default, or make it nullable), not on every consumer.

## Running it yourself

```bash
cd debezium_deployment
./deploy.sh up local                     # Zookeeper, Kafka, Postgres, Connect (+Avro converter), Schema Registry, Kafka UI
./deploy.sh register                     # Avro connector; sets BACKWARD on the subject
./deploy.sh exec-sql sample_changes.sql  # first records -> version 1
./deploy.sh schemas                      # subjects and fields

cd ../flink_deployment
./deploy.sh up local
./deploy.sh submit cdc_impressions.py local      # avro-confluent -> upsert-kafka
../debezium_deployment/deploy.sh consume cdc.impressions.page_type_counts local   # in another shell

cd ../debezium_deployment
./deploy.sh evolve                       # ADD / RENAME / DROP; versions 1 -> 4; connector stays RUNNING
./deploy.sh evolve-incompatible          # the 409
./deploy.sh compat                       # show mode; `compat FULL` etc. to change it
```

`./deploy.sh register json` registers the old JSON-converter connector
(`connectors/postgres-source-json.json`) for the `CDC_FORMAT=json` path,
which has no registry and therefore no gate: a renamed column simply shows up
as a missing key and Flink's `json.ignore-parse-errors` hides the damage.
That contrast is the reason for the registry.

## What Flink did (measured, 2026-09-08)

The streaming job (`cdc_impressions.py`, `avro-confluent` source,
`upsert-kafka` sink) was running while `./deploy.sh evolve` applied the three
ALTERs. It never restarted or failed:

```
=== flink job after evolve ===
48a6d3e1644a90922757b0aededcde9a : insert-into_default_catalog.default_database.page_type_counts (RUNNING)
Subject cdc.impressions.events-value: versions [1,2,3,4]
```

and it kept emitting windows for records written under the new schema
versions (the three `evolve` inserts land in the 15:58 window, three more
v4-shaped inserts in 16:00):

```
page_type=1 window=15:52:00 .. 15:53:00 events=3  impressions=1   <- v1 (snapshot + sample_changes)
page_type=2 window=15:52:00 .. 15:53:00 events=6  impressions=2
page_type=3 window=15:52:00 .. 15:53:00 events=11 impressions=2
page_type=1 window=15:58:00 .. 15:59:00 events=1  impressions=1   <- v2 record (ADD COLUMN)
page_type=2 window=15:58:00 .. 15:59:00 events=1  impressions=1   <- v3 record (RENAME)
page_type=3 window=15:58:00 .. 15:59:00 events=1  impressions=1   <- v4 record (DROP)
page_type=1 window=16:00:00 .. 16:01:00 events=2  impressions=1   <- v4-shaped inserts
page_type=2 window=16:00:00 .. 16:01:00 events=1  impressions=1
```

A bounded read of the same topic through the same reader schema (Flink SQL
client, `scan.bounded.mode = latest-offset`) shows the resolution per record:

```
| event_id | page_type | event_hour | event_minute | event_second | __op |
|        1 |         1 |         10 |           30 |           15 |    r |   v1: both present
|      ... |           |            |              |              |      |
|       20 |         3 |         16 |           20 |           25 |    c |
|       21 |         1 |          8 |            5 |           10 |    c |   v2: ADD COLUMN referrer -> ignored, both still present
|       22 |         2 |          9 |       <NULL> |           20 |    c |   v3: RENAME event_minute -> NULL, event_second present
|       23 |         3 |         10 |       <NULL> |       <NULL> |    c |   v4: DROP event_second -> both NULL
|       24 |         1 |         12 |       <NULL> |       <NULL> |    c |
|      ... |           |            |              |              |      |
27 rows in set
```

Exactly the behaviour the Avro rules predict: a writer field the reader does
not declare (`referrer`, `minute_of_hour`) is skipped; a reader field the
writer no longer has (`event_minute` from v3, `event_second` from v4) takes
its `null` default. The aggregate reads only `page_type`, `impression_id`,
`__op` and `__source_ts_ms`, none of which changed, so the counts are
unaffected. Had the query depended on `event_minute`, the failure mode would
have been silent NULLs, not an error; that is what the registry's
compatibility gate and the DDL-as-contract are for.

Two things the run needed that are not obvious from the code:

- **Windows close on watermarks, not on wall-clock.** The v1 window emitted
  nothing until the `evolve` inserts advanced the event-time watermark past
  its end; the 16:00 window needed one more insert for the same reason. A
  consumer that sees an empty output topic right after start-up is not
  broken, it is waiting for the next event.
- **The Flink image must be built for `linux/amd64`** even on Apple Silicon
  (`flink_deployment/docker-compose.yaml` pins it): `apache-flink 1.18.1`
  drags in `apache-beam 2.48` / `pemja`, which have no arm64 Linux wheels
  and fail to build from source inside the image.

Verified live: registry and connector on 2026-09-04; the Flink job across
versions 1 to 4, the count windows and the per-record resolution above on
2026-09-08.
