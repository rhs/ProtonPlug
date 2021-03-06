/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.amqp.dealer.util;

import java.util.concurrent.TimeUnit;

/**
 * @author Clebert Suconic
 */

public class FutureRunnable implements Runnable
{
   private final ReusableLatch latch;

   public FutureRunnable(final int initialIterations)
   {
      latch = new ReusableLatch(initialIterations);
   }

   public FutureRunnable()
   {
      this(0);
   }

   public void run()
   {
      latch.countDown();
   }

   public void countUp()
   {
      latch.countUp();
   }

   public void countDown()
   {
      latch.countDown();
   }

   public int getCount()
   {
      return latch.getCount();
   }

   public void await() throws InterruptedException
   {
      latch.await();
   }

   public boolean await(long timeWait, TimeUnit timeUnit) throws InterruptedException
   {
      return latch.await(timeWait, timeUnit);
   }

   public boolean await(long milliseconds) throws InterruptedException
   {
      return latch.await(milliseconds);
   }
}
