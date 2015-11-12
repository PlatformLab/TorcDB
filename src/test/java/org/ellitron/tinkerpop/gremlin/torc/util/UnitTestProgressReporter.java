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
package org.ellitron.tinkerpop.gremlin.torc.util;

import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

/**
 *
 * @author ellitron
 */
public class UnitTestProgressReporter extends RunListener {
    private static String lastClassName = "";
    
    @Override
    public void testStarted(Description description) throws Exception {
        if (!description.getClassName().equals(lastClassName)) {
            lastClassName = description.getClassName();
            System.out.println("Test Class: " + lastClassName);
        }
        
        System.out.println("Test Method: " + description.getMethodName());
    }
    
    @Override
    public void testAssumptionFailure(Failure failure) {
        System.out.println("Test Assumption Failure: " + failure.getMessage());
    }
    
    @Override
    public void testFailure(Failure failure) throws Exception {
        System.out.println("Test Failed: " + failure.getMessage());
    }
    
    @Override
    public void testIgnored(Description description) throws Exception {
        System.out.println("Test Ignored: " + description.getDisplayName());
    }
}
