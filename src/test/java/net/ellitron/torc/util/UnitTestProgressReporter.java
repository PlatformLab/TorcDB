/* Copyright (c) 2015-2019 Stanford University
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

import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

/**
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
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
