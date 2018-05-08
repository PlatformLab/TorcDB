#!/bin/bash
mvn exec:java -Dlog4j.configuration="file:$PWD/src/main/resources/log4j.properties" -Dexec.mainClass="net.ellitron.torc.util.OpTester" -Dexec.args="$*"
