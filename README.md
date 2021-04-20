# Overview

BLC Redis reshard utility.

## Getting started

### Docker

* `docker run --rm -ti balaclavalab/blc-redis-reshard:0.1.1 -u redis://localhost:7000`

### Docker (building and running)

* Build with `./gradlew dockerBuildImage`
* Open build image `docker run --entrypoint bash -ti <imageId>`
* Use `/redis-reshard-<version>/bin/blc-redis-reshard` 

### Gradle

* Build with `./gradlew installDist`
* Go to `./blc-redis-reshard/build/install/blc-redis-reshard/bin`
* Use `./blc-redis-reshard` 

### Examples:

* `./blc-redis-reshard -u redis://localhost:7000`
```
Cluster partitions:
RedisClusterNode [uri=redis://127.0.0.1:7003, nodeId='0d91955745748afed9e979a9f63febc45004a43c', connected=true, slaveOf='null', pingSentTimestamp=0, pongReceivedTimestamp=1618141099078, configEpoch=2, flags=[MASTER], aliases=[], slot count=0]
RedisClusterNode [uri=redis://127.0.0.1:7002, nodeId='3cdd41a318401aeda4e8485e9f25bfcbcef3e692', connected=true, slaveOf='null', pingSentTimestamp=0, pongReceivedTimestamp=1618141097000, configEpoch=3, flags=[MYSELF, MASTER], aliases=[], slot count=0]
RedisClusterNode [uri=redis://127.0.0.1:7000, nodeId='a38b18dc275fed7270022a63b3dfff9b63609e25', connected=true, slaveOf='null', pingSentTimestamp=0, pongReceivedTimestamp=1618141097981, configEpoch=1, flags=[MASTER], aliases=[], slot count=0]
RedisClusterNode [uri=redis://127.0.0.1:7001, nodeId='b464a325eed917991edb620adea6ea58e62687ca', connected=true, slaveOf='null', pingSentTimestamp=0, pongReceivedTimestamp=1618141096891, configEpoch=0, flags=[MASTER], aliases=[], slot count=0]

Current cluster slots:
None

Desired cluster slots:
Slots 0-4095 should be on node 0d91955745748afed9e979a9f63febc45004a43c
Slots 4096-8191 should be on node 3cdd41a318401aeda4e8485e9f25bfcbcef3e692
Slots 8192-12287 should be on node a38b18dc275fed7270022a63b3dfff9b63609e25
Slots 12288-16383 should be on node b464a325eed917991edb620adea6ea58e62687ca

!!! Commit flag (--yes) was not set so this is just preview !!!
```

* `./blc-redis-reshard -u redis://localhost:7000 --assign`
```
Cluster partitions:
...

Current cluster slots:
None

Desired cluster slots:
Slots 0-4095 should be on node 0d91955745748afed9e979a9f63febc45004a43c
Slots 4096-8191 should be on node 3cdd41a318401aeda4e8485e9f25bfcbcef3e692
Slots 8192-12287 should be on node a38b18dc275fed7270022a63b3dfff9b63609e25
Slots 12288-16383 should be on node b464a325eed917991edb620adea6ea58e62687ca

Redis cluster is empty, assigning desired slots...
Adding slots 0-4095 for node 0d91955745748afed9e979a9f63febc45004a43c
Adding slots 4096-8191 for node 3cdd41a318401aeda4e8485e9f25bfcbcef3e692
Adding slots 8192-12287 for node a38b18dc275fed7270022a63b3dfff9b63609e25
Adding slots 12288-16383 for node b464a325eed917991edb620adea6ea58e62687ca
Done

!!! Commit flag (--yes) was not set so this is just preview !!!

```

* `./blc-redis-reshard -u redis://localhost:7000 --assign --yes` -- same as above but will actually commit job

* `./blc-redis-reshard -u redis://localhost:7000 -e b464a325eed917991edb620adea6ea58e62687ca` -- exclude node from resharding
```
Cluster partitions:
...

Current cluster slots:
Slots 0-4095 are on node b464a325eed917991edb620adea6ea58e62687ca
Slots 4096-8191 are on node 0d91955745748afed9e979a9f63febc45004a43c
Slots 8192-12287 are on node 3cdd41a318401aeda4e8485e9f25bfcbcef3e692
Slots 12288-16383 are on node a38b18dc275fed7270022a63b3dfff9b63609e25

Desired cluster slots:
Slots 0-5460 should be on node 0d91955745748afed9e979a9f63febc45004a43c
Slots 5461-10921 should be on node 3cdd41a318401aeda4e8485e9f25bfcbcef3e692
Slots 10922-16383 should be on node a38b18dc275fed7270022a63b3dfff9b63609e25

!!! Commit flag (--yes) was not set so this is just preview !!!

```

* `./blc-redis-reshard -u redis://localhost:7000 -e b464a325eed917991edb620adea6ea58e62687ca,a38b18dc275fed7270022a63b3dfff9b63609e25` -- exclude two nodes from resharding
```
Cluster partitions:
...

Current cluster slots:
Slots 0-4095 are on node b464a325eed917991edb620adea6ea58e62687ca
Slots 4096-8191 are on node 0d91955745748afed9e979a9f63febc45004a43c
Slots 8192-12287 are on node 3cdd41a318401aeda4e8485e9f25bfcbcef3e692
Slots 12288-16383 are on node a38b18dc275fed7270022a63b3dfff9b63609e25

Desired cluster slots:
Slots 0-8191 should be on node 0d91955745748afed9e979a9f63febc45004a43c
Slots 8192-16383 should be on node 3cdd41a318401aeda4e8485e9f25bfcbcef3e692

!!! Commit flag (--yes) was not set so this is just preview !!!

```

* `./blc-redis-reshard -u redis://localhost:7000 -e b464a325eed917991edb620adea6ea58e62687ca,a38b18dc275fed7270022a63b3dfff9b63609e25 --reshard --yes` -- actually do everything
```
Cluster partitions:
...

Current cluster slots:
Slots 0-4095 are on node 0d91955745748afed9e979a9f63febc45004a43c
Slots 4096-8191 are on node 3cdd41a318401aeda4e8485e9f25bfcbcef3e692
Slots 8192-12287 are on node b464a325eed917991edb620adea6ea58e62687ca
Slots 12288-16383 are on node a38b18dc275fed7270022a63b3dfff9b63609e25

Desired cluster slots:
Slots 0-8191 should be on node 0d91955745748afed9e979a9f63febc45004a43c
Slots 8192-16383 should be on node 3cdd41a318401aeda4e8485e9f25bfcbcef3e692

Checking if all slots are assigned to desired nodes...
Slot 4096 is not on desired node, currently on 3cdd41a318401aeda4e8485e9f25bfcbcef3e692, but should be on 0d91955745748afed9e979a9f63febc45004a43c
Moving keys in slot 4096 to new node, total key count: 65
Moving keys in slot 4096 to new node, key count: 65
Slot 4097 is not on desired node, currently on 3cdd41a318401aeda4e8485e9f25bfcbcef3e692, but should be on 0d91955745748afed9e979a9f63febc45004a43c
Moving keys in slot 4097 to new node, total key count: 54
Moving keys in slot 4097 to new node, key count: 54
...
Slot 16383 is not on desired node, currently on a38b18dc275fed7270022a63b3dfff9b63609e25, but should be on 3cdd41a318401aeda4e8485e9f25bfcbcef3e692
Moving keys in slot 16383 to new node, total key count: 49
Moving keys in slot 16383 to new node, key count: 49
Done
```

* `./blc-redis-reshard -u redis://localhost:7000 --reshard`
```
Cluster partitions:
...

Current cluster slots:
Slots 0-8191 are on node 0d91955745748afed9e979a9f63febc45004a43c
Slots 8192-16383 are on node 3cdd41a318401aeda4e8485e9f25bfcbcef3e692

Desired cluster slots:
Slots 0-4095 should be on node 0d91955745748afed9e979a9f63febc45004a43c
Slots 4096-8191 should be on node 3cdd41a318401aeda4e8485e9f25bfcbcef3e692
Slots 8192-12287 should be on node b464a325eed917991edb620adea6ea58e62687ca
Slots 12288-16383 should be on node a38b18dc275fed7270022a63b3dfff9b63609e25

Checking if all slots are assigned to desired nodes...
Slot 4096 is not on desired node, currently on 0d91955745748afed9e979a9f63febc45004a43c, but should be on 3cdd41a318401aeda4e8485e9f25bfcbcef3e692
Moving keys in slot 16381 to new node, total key count: 68
...
Slot 16383 is not on desired node, currently on 3cdd41a318401aeda4e8485e9f25bfcbcef3e692, but should be on a38b18dc275fed7270022a63b3dfff9b63609e25
Moving keys in slot 16383 to new node, total key count: 49
Done

!!! Commit flag (--yes) was not set so this is just preview !!!
```

* `./blc-redis-reshard -u redis://localhost:7000 --reshard --yes` -- actually do everything
* `./blc-redis-reshard -u redis://localhost:7000 --t --yes` -- writes 1M test keys (`set N N` useful for testing)
* `./blc-redis-reshard -u redis://localhost:7000 --dt --yes` -- deletes test keys

### Usage reference

```
usage: blc-redis-reshard [-a] [-dt] [-e <arg>] [-mb <arg>] [-r] [-t] [-tk <arg>] [-u <arg>] [-y]
BLC Redis reshard utility
 -a,--assign                      Perform unassigned slot assignment
 -dt,--deleteTestData             Delete test data to cluster (for testing)
 -e,--excludeNodeIds <arg>        Exclude node ids from balancing
 -mb,--migrationBatchSize <arg>   Migration batch size (default 1000)
 -r,--reshard                     Perform reshard
 -t,--writeTestData               Write test data to cluster (for testing)
 -tk,--testDataKeysCount <arg>    How many test keys write to db (default 1000000, for testing)
 -u,--uri <arg>                   Redis to connect to (e.g. Redis://localhost)
 -y,--yes                         Do actual operations
```