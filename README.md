<!---
Copyright 2015 Apache Software Foundation.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

![TorcDB](graphics/TorcDBLogo_v02.png)

Introduction
============
TorcDB is an implementation of the TinkerPop graph database API on RAMCloud,
and stands for "**T**inkerPop **O**n **R**AM**C**loud". 

Setup Instructions
==================
* After cloning the repository, update and initialize submodules:
```
git submodule update --init --recursive
```
* Build RAMCloud:
```
cd RAMCloud/
make -j8 DEBUG=no
```
* Install ramcloud client library:
```
ln -s $(pwd)/obj/libramcloud.so ~/local/lib/libramcloud.so
```
* Build RAMCloud java library:
```
cd bindings/java/
./gradlew
```
* Install RAMCloud java library in local maven repository:
```
mvn install:install-file -Dfile=./build/libs/ramcloud.jar -DgroupId=edu.stanford -DartifactId=ramcloud -Dversion=1.0 -Dpackaging=jar
```
* Build TorcDB:
```
cd ../../..
mvn install -DskipTests
```
* Use this in your POM file:
```
<dependency>
  <groupId>net.ellitron.torc</groupId>
  <artifactId>torc</artifactId>
  <version>1.0.0</version>
</dependency>
```

Running LDBC SNB Validation
===========================
* Download and build the LDBC driver:
```
git clone git@github.com:ldbc/ldbc_driver.git
cd ldbc_driver/
mvn install -DskipTests
```
* Download and extract the LDBC SNB Interactive validation dataset:
```
git clone git@github.com:ldbc/ldbc_snb_interactive_validation.git
cd ldbc_snb_interactive_validation/neo4j
mkdir readwrite_neo4j--validation_set
tar -xvzf readwrite_neo4j--validation_set.tar.gz -C readwrite_neo4j--validation_set
```
* Configure validation by editing
  `readwrite_neo4j--ldbc_driver_config--db_validation.properties` (Pro tip: In
  vim, use `:r!pwd` to insert path of current working directory to make editing
  the paths easier):
```
# -------------------------------------
# -------------------------------------
# ----- LDBC Driver Configuration -----
# -------------------------------------
# -------------------------------------

# TODO: uncomment this and point it to where it wants to be pointed
database=net.ellitron.ldbcsnbimpls.interactive.torc.TorcDb

# TODO: uncomment this and point it to where it wants to be pointed
# e.g. /path/to/validation_set/validation_params.csv
validate_database=/path/to/ldbc_snb_interactive_validation/neo4j/readwrite_neo4j--validation_set/validation_set/validation_params.csv

# TODO: uncomment this and point it to where it wants to be pointed
# /path/to/validation_set/
ldbc.snb.interactive.parameters_dir=/path/to/ldbc_snb_interactive_validation/neo4j/readwrite_neo4j--validation_set/validation_set/

# TODO: uncomment this and point it to where it wants to be pointed
# path specifying where to write the benchmark results file
# STRING
# COMMAND: -rd/--results_dir
results_dir=/path/to/ldbc_driver/results
```
* Download and build TorcDB implementation of LDBC SNB Interactive workload:
```
git clone git@github.com:PlatformLab/ldbc-snb-impls.git
cd ldbc-snb-impls/
mvn install -DskipTests
cd snb-interactive-torc/
mvn compile assembly:single
```
* Start a RAMCloud cluster (in a new window). The following starts a cluster
  with 4 servers, each with 1GB of memory, and 8GB of backup capacity, and with
  a replication factor of 3:
```
cd /path/to/TorcDB/RAMCloud
./scripts/cluster.py -s 4 -r 3 --masterArgs="--totalMasterMemory 1024 --segmentFrames 1024" -v
```
* Load the validation dataset into RAMCloud. Change parameters accordingly. The
  following assumes the coordinator is located at 
  `basic+udp:host=192.168.1.110,port=12246`:
```
cd /path/to/ldbc-snb-impls/snb-interactive-torc/

mvn exec:java -Dexec.mainClass="net.ellitron.ldbcsnbimpls.interactive.torc.util.GraphLoader" -Dexec.args="--coordLoc basic+udp:host=192.168.1.110,port=12246 --masters 4 --graphName ldbcsnbval01 --numLoaders 1 --loaderIdx 0 --numThreads 1 --txSize 32 --reportInt 2 --reportFmt OFDT nodes /path/to/ldbc_snb_interactive_validation/neo4j/readwrite_neo4j--validation_set/validation_set/"

mvn exec:java -Dexec.mainClass="net.ellitron.ldbcsnbimpls.interactive.torc.util.GraphLoader" -Dexec.args="--coordLoc basic+udp:host=192.168.1.110,port=12246 --masters 4 --graphName ldbcsnbval01 --numLoaders 1 --loaderIdx 0 --numThreads 1 --txSize 32 --reportInt 2 --reportFmt OFDT edges /path/to/ldbc_snb_interactive_validation/neo4j/readwrite_neo4j--validation_set/validation_set/"

mvn exec:java -Dexec.mainClass="net.ellitron.ldbcsnbimpls.interactive.torc.util.GraphLoader" -Dexec.args="--coordLoc basic+udp:host=192.168.1.110,port=12246 --masters 4 --graphName ldbcsnbval01 --numLoaders 1 --loaderIdx 0 --numThreads 1 --txSize 32 --reportInt 2 --reportFmt OFDT props /path/to/ldbc_snb_interactive_validation/neo4j/readwrite_neo4j--validation_set/validation_set/"
```
* Run validation against TorcDB:
```
cd /path/to/ldbc_driver
java -cp target/jeeves-0.3-SNAPSHOT.jar:/path/to/ldbc-snb-impls/snb-interactive-torc/target/snb-interactive-torc-0.1.0-jar-with-dependencies.jar com.ldbc.driver.Client -p coordinatorLocator basic+udp:host=192.168.1.110,port=12246 -p graphName ldbcsnbval01 -P /path/to/ldbc_snb_interactive_validation/neo4j/readwrite_neo4j--ldbc_driver_config--db_validation.properties
```
