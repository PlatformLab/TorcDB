/* Copyright (c) 2018-2019 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
package net.ellitron.torc.util;

import net.ellitron.torc.*;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import org.docopt.Docopt;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A utility for running individual operations on TorcDB, like getting the
 * neighbors of a vertex, or the properties of a vertex, and returning the
 * results as well as timing the performance. Configuration parameters are
 * specified in a jproperties config file, which includes all parameters for
 * connecting to an existing graph stored in a RAMCloud cluster. Various
 * parameters at the command line control not just what kind of traversal to
 * perform, but what kinds of statistics are desired.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class OpTester {
  private static final String doc =
      "OpTester: A utility for running individual operations on TorcDB.\n"
      + "\n"
      + "Usage:\n"
      + "  OpTester [options] edges <vIdUpperLong> <vIdLowerLong> <direction> <edgeLabel> <neighborLabel>\n"
      + "  OpTester (-h | --help)\n"
      + "  OpTester --version\n"
      + "\n"
      + "Options:\n"
      + "  --config=<file>      OpTester configuration file\n"
      + "                       [default: ./config/optester.properties].\n"
      + "  --repeat=<n>         How many times to repeat the op. If n > 1\n"
      + "                       then normal op result output will be\n"
      + "                       surpressed to show only the op timing\n"
      + "                       information\n"
      + "                       [default: 1].\n"
      + "  --timeUnits=<unit>   Unit of time in which to report timings\n"
      + "                       (SECONDS, MILLISECONDS, MICROSECONDS,\n"
      + "                       NANOSECONDS) [default: MILLISECONDS].\n"
      + "  -h --help            Show this screen.\n"
      + "  --version            Show version.\n"
      + "\n";

  public static void main(String[] args) throws Exception {
    Map<String, Object> opts =
        new Docopt(doc).withVersion("OpTester 1.0").parse(args);

    // Get values of general options.
    int repeatCount = Integer.decode((String) opts.get("--repeat"));
    String timeUnits = (String) opts.get("--timeUnits");

    // Load properties from the configuration file.
    String configFilename = (String) opts.get("--config");
    Properties prop = new Properties();
    prop.load(new FileInputStream(configFilename));

    Map<String, String> config = new HashMap<>();
    prop.stringPropertyNames().stream()
        .forEach((propName) -> {
          if (propName.equals("coordinatorLocator")) {
            config.put(TorcGraph.CONFIG_COORD_LOCATOR, 
                prop.getProperty(propName));
          } else if (propName.equals("graphName")) {
            config.put(TorcGraph.CONFIG_GRAPH_NAME, 
                prop.getProperty(propName));
          } else {
            config.put(propName, prop.getProperty(propName));
          }
        });

    TorcGraph graph = TorcGraph.open(config);

    Long[] timings = new Long[repeatCount];

    if ((Boolean) opts.get("edges")) {
      Long vIdUpper = Long.decode((String) opts.get("<vIdUpperLong>"));
      Long vIdLower = Long.decode((String) opts.get("<vIdLowerLong>"));
     
      Direction dir;
      switch ((String) opts.get("<direction>")) {
        case "IN":
          dir = Direction.IN;
          break;
        case "OUT":
          dir = Direction.OUT;
          break;
        default:
          throw new IllegalArgumentException("Illegal <direction> value. Optionsaare IN, OUT."); 
      }

      String edgeLabel = (String) opts.get("<edgeLabel>");
      String neighborLabel = (String) opts.get("<neighborLabel>");

      long startTime, endTime;
      for (int i = 0; i < repeatCount; i++) {
        startTime = System.nanoTime();

        TorcVertex vertex = new TorcVertex(graph, 
            new UInt128(vIdUpper, vIdLower));

        Iterator<Edge> edges = vertex.edges(dir, 
            new String[] {edgeLabel},
            new String[] {neighborLabel});

        graph.tx().rollback();

        endTime = System.nanoTime();
        timings[i] = endTime - startTime;

        if (repeatCount == 1) {
          edges.forEachRemaining((e) -> {
            System.out.println(e.toString());
          });
        }
      }
    }

    Arrays.sort(timings);

    long sum = 0;
    long min = Long.MAX_VALUE;
    long max = 0;
    for (int i = 0; i < timings.length; i++) {
      sum += timings[i];

      if (timings[i] < min) {
        min = timings[i];
      }

      if (timings[i] > max) {
        max = timings[i];
      }
    }

    long mean = sum / repeatCount;

    int p25 = (int) (0.25 * (float) repeatCount);
    int p50 = (int) (0.50 * (float) repeatCount);
    int p75 = (int) (0.75 * (float) repeatCount);
    int p90 = (int) (0.90 * (float) repeatCount);
    int p95 = (int) (0.95 * (float) repeatCount);
    int p99 = (int) (0.99 * (float) repeatCount);

    long nanosPerTimeUnit;

    switch (timeUnits) {
      case "NANOSECONDS":
        nanosPerTimeUnit = 1;
        break;
      case "MICROSECONDS":
        nanosPerTimeUnit = 1000;
        break;
      case "MILLISECONDS":
        nanosPerTimeUnit = 1000000;
        break;
      case "SECONDS":
        nanosPerTimeUnit = 1000000000;
        break;
      default:
        throw new IllegalArgumentException(
            "Unrecognized time unit: " + timeUnits);
    }

    System.out.println();
    System.out.println(String.format(
        "Op Stats:\n"
        + "  Units:            %s\n"
        + "  Count:            %d\n"
        + "  Min:              %d\n"
        + "  Max:              %d\n"
        + "  Mean:             %d\n"
        + "  25th Percentile:  %d\n"
        + "  50th Percentile:  %d\n"
        + "  75th Percentile:  %d\n"
        + "  90th Percentile:  %d\n"
        + "  95th Percentile:  %d\n"
        + "  99th Percentile:  %d\n",
        timeUnits,
        repeatCount,
        min / nanosPerTimeUnit,
        max / nanosPerTimeUnit,
        mean / nanosPerTimeUnit,
        timings[p25] / nanosPerTimeUnit,
        timings[p50] / nanosPerTimeUnit,
        timings[p75] / nanosPerTimeUnit,
        timings[p90] / nanosPerTimeUnit,
        timings[p95] / nanosPerTimeUnit,
        timings[p99] / nanosPerTimeUnit));

    graph.close();
  }

}
