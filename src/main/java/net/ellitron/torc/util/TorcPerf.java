/* 
 * Copyright (C) 2018-2018 Stanford University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.ellitron.torc.util;

import net.ellitron.torc.*;

import edu.stanford.ramcloud.*;
import edu.stanford.ramcloud.ClientException.*;

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
import java.io.FileReader;
import java.io.FileWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.Math.*;
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
 * A performance profiling tool for TorcDB. Takes an input configuration file
 * that specifies the tests to run and the parameter ranges over which to run
 * them. 
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class TorcPerf {
  private static final String doc =
      "TorcPerf: A performance profiling tool for TorcDB.\n"
      + "\n"
      + "Usage:\n"
      + "  TorcPerf [options] COORDINATOR\n"
      + "  TorcPerf (-h | --help)\n"
      + "  TorcPerf --version\n"
      + "\n"
      + "Options:\n"
      + "  --config=<f>         TorcPerf configuration file\n"
      + "                       [default: ./config/torcperf.cfg].\n"
      + "  --replicas=<r>       Number of replicas in RAMCloud cluster.\n"
      + "                       [default: 3].\n"
      + "  --dpdkPort=<p>       If using DPDK, which port to use.\n"
      + "                       [default: -1].\n"
      + "  -h --help            Show this screen.\n"
      + "  --version            Show version.\n"
      + "\n";

  public static void main(String[] args) throws Exception {
    Map<String, Object> opts =
        new Docopt(doc).withVersion("TorcPerf 1.0").parse(args);

    String coordinatorLocator = (String) opts.get("COORDINATOR");
    int replicas = Integer.decode((String) opts.get("--replicas"));
    int dpdkPort = Integer.decode((String) opts.get("--dpdkPort"));

    RAMCloud client = new RAMCloud(coordinatorLocator, "foo", dpdkPort);

    // Default values for experiment properties.
    int segment_range_start = 2048;
    int segment_range_end = 2048;
    int segment_points = 1;
    String segment_points_mode = "linear";
    int list_max_size = 1000;

    // Load properties from the configuration file.
    String cfgFilename = (String) opts.get("--config");
    
    try (BufferedReader br = new BufferedReader(new FileReader(cfgFilename))) {
      while (true) {
        boolean foundCfg = false;
        String line;
        String op = "none"; 
        while ((line = br.readLine()) != null) {
          if (line.charAt(0) == '[') {
            op = line.substring(1, line.lastIndexOf(']'));
            foundCfg = true;
          } else if (line.charAt(0) == '#') {
            // skip
            continue;
          } else if (line.length() == 0) {
            if (foundCfg == false)
              continue;
            else
              break;
          } else {
            String varName = line.substring(0, line.indexOf(' '));
            
            if (varName.equals("segment_range_start")) {
              String varValue = line.substring(line.lastIndexOf(' ') + 1, 
                  line.length());
              int varIntValue = Integer.decode(varValue);
              segment_range_start = varIntValue;
            } else if (varName.equals("segment_range_end")) {
              String varValue = line.substring(line.lastIndexOf(' ') + 1, 
                  line.length());
              int varIntValue = Integer.decode(varValue);
              segment_range_end = varIntValue;
            } else if (varName.equals("segment_points")) {
              String varValue = line.substring(line.lastIndexOf(' ') + 1, 
                  line.length());
              int varIntValue = Integer.decode(varValue);
              segment_points = varIntValue;
            } else if (varName.equals("segment_points_mode")) {
              String varValue = line.substring(line.lastIndexOf(' ') + 1, 
                  line.length());
              segment_points_mode = varValue;
            } else if (varName.equals("list_max_size")) {
              String varValue = line.substring(line.lastIndexOf(' ') + 1, 
                  line.length());
              int varIntValue = Integer.decode(varValue);
              list_max_size = varIntValue;
            } else {
              System.out.println(String.format("ERROR: Unknown parameter: %s\n", 
                    varName));
              return;
            }
          }
        }

        if (foundCfg == false) {
          System.out.println("End of experiments");
          return;
        }

        List<Integer> segment_sizes = new ArrayList<>(); 

        if (segment_points > 1) {
          if (segment_points_mode.equals("linear")) {
            int step_size = 
              (segment_range_end - segment_range_start) / (segment_points - 1);

            for (int i = segment_range_start; i <= segment_range_end; i += step_size) 
              segment_sizes.add(i);
          } else if (segment_points_mode.equals("geometric")) {
            double c = Math.pow(10, Math.log10((double)segment_range_end/(double)segment_range_start) / (double)(segment_points - 1));
            for (int i = segment_range_start; i <= segment_range_end; i *= c)
              segment_sizes.add(i);
          } else {
            System.out.println(String.format("ERROR: Unknown points mode: %s\n", segment_points_mode));
            return;
          }
        } else {
          segment_sizes.add(segment_range_start);
        }

        if (op.equals("TorcEdgeList_PrependAndRead")) {
          for (int ss_idx = 0; ss_idx < segment_sizes.size(); ss_idx++) {
            int segment_size = segment_sizes.get(ss_idx);
            System.out.println(String.format("Prepend And Read Test: segment_size: %dB", segment_size));

            FileWriter datFile = new FileWriter(String.format("prependAndRead.ss_%d.lms_%s.rf_%d.csv", segment_size, list_max_size, replicas));

            long tableId = client.createTable("PrependAndReadTest");

            datFile.write(String.format("%12s %12s %12s\n", 
                  "Elements",
                  "Prepend",
                  "Read"));

            UInt128 baseVertexId = new UInt128(42);

            byte[] keyPrefix = TorcHelper.getEdgeListKeyPrefix(
                baseVertexId, 
                "hasCreator", 
                TorcEdgeDirection.DIRECTED_IN,
                "Comment");

            List<Long> prependLatency = new ArrayList<>();
            List<Long> readLatency = new ArrayList<>();
            long startTime, endTime;
            for (int i = 0; i < list_max_size; i++) {
              startTime = System.nanoTime();
              RAMCloudTransaction rctx = new RAMCloudTransaction(client);

              UInt128 neighborId = new UInt128(i);

              boolean newList = TorcEdgeList.prepend(
                  rctx,
                  tableId,
                  keyPrefix,
                  neighborId, 
                  new byte[] {},
                  segment_size,
                  0);

              boolean success = rctx.commit();
              rctx.close();

              endTime = System.nanoTime();

              prependLatency.add(endTime - startTime);

              if (!success) {
                System.out.println("ERROR: Prepend element transaction failed");
                return;
              }

              startTime = System.nanoTime();
              rctx = new RAMCloudTransaction(client);

              List<TorcEdge> list = TorcEdgeList.read(
                  rctx,
                  tableId,
                  keyPrefix,
                  null, 
                  baseVertexId,
                  "hasCreator", 
                  TorcEdgeDirection.DIRECTED_IN);

              success = rctx.commit();
              rctx.close();

              endTime = System.nanoTime();

              readLatency.add(endTime - startTime);

              if (!success) {
                System.out.println("ERROR: Read element transaction failed");
                return;
              }

              datFile.write(String.format("%12d %12.1f %12.1f\n",
                    i+1,
                    prependLatency.get(prependLatency.size()-1)/1000.0,
                    readLatency.get(readLatency.size()-1)/1000.0));
              datFile.flush();
            }

            datFile.close();

            client.dropTable("PrependAndReadTest");
          }
        } else {
          System.out.println(String.format("ERROR: Unknown operation: %s", op));
          return;
        }
      }
    }
  }

}
