#!/bin/bash

# Copyright 2015 Apache Software Foundation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script is a launch script for the MeasurementClient main class, meant to 
# be handed to the RAMCloud/script/cluster.py --client argument for execution on 
# multiple machines. 

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

java -cp ${SCRIPTDIR}/../../../target/ramcloud-gremlin-1.0-SNAPSHOT-jar-with-dependencies.jar \
org.ellitron.tinkerpop.gremlin.ramcloud.measurement.MeasurementClient \
$@