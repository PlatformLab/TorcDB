/*
 * Copyright 2015 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.ellitron.torc;

import edu.stanford.ramcloud.*;
import edu.stanford.ramcloud.ClientException.*;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class TorcEdgeListTest {

  RAMCloud client;

  public TorcEdgeListTest() {
    this.client = null;
  }

  @Before
  public void before() throws Exception {
    String coordLoc = System.getProperty("ramcloudCoordinatorLocator");
    if (coordLoc == null)
      throw new Exception("No RAMCloud coordinator specified. Please specify with -DramcloudCoordinatorLocator=<locator_string>");

    this.client = new RAMCloud(coordLoc);
  }

  @Test
  public void constructor_example() {
    System.out.println("Hello World!");
  }

  @After
  public void after() throws Exception {
    System.out.println("tearDown()");
  }
}
