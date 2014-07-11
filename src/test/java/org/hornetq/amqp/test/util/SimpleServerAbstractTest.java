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

package org.hornetq.amqp.test.util;

import org.hornetq.amqp.test.AbstractJMSTest;
import org.hornetq.amqp.test.invm.InVMTestConnector;
import org.hornetq.amqp.test.minimalclient.Connector;
import org.hornetq.amqp.test.minimalclient.SimpleAMQPConnector;
import org.hornetq.amqp.test.minimalserver.DumbServer;
import org.hornetq.amqp.test.minimalserver.MinimalServer;
import org.junit.After;
import org.junit.Before;

/**
 * @author Clebert Suconic
 */

public class SimpleServerAbstractTest
{

   protected final boolean useSASL;
   protected final boolean useInVM;
   protected MinimalServer server = new MinimalServer();

   public SimpleServerAbstractTest(boolean useSASL, boolean useInVM)
   {
      this.useSASL = useSASL;
      this.useInVM = useInVM;
   }

   @Before
   public void setUp() throws Exception
   {
      DumbServer.clear();
      AbstractJMSTest.forceGC();
      if (!useInVM)
      {
         server.start("127.0.0.1", 5672, useSASL);
      }


   }

   @After
   public void tearDown() throws Exception
   {
      if (!useInVM)
      {
         server.stop();
      }
      DumbServer.clear();
   }

   protected Connector newConnector()
   {
      if (useInVM)
      {
         return new InVMTestConnector();
      }
      else
      {
         return new SimpleAMQPConnector();
      }
   }




}
