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

package org.hornetq.amqp.test;

import javax.jms.JMSException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.hornetq.amqp.dealer.AMQPClientConnection;
import org.hornetq.amqp.dealer.AMQPClientReceiver;
import org.hornetq.amqp.dealer.AMQPClientSender;
import org.hornetq.amqp.dealer.AMQPClientSession;
import org.hornetq.amqp.dealer.SASLPlain;
import org.hornetq.amqp.dealer.util.NettyWritable;
import org.hornetq.amqp.test.invm.InVMTestConnector;
import org.hornetq.amqp.test.minimalclient.Connector;
import org.hornetq.amqp.test.minimalclient.SimpleAMQPConnector;
import org.hornetq.amqp.test.minimalserver.DumbServer;
import org.hornetq.amqp.test.minimalserver.MinimalServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * @author Clebert Suconic
 */
@RunWith(Parameterized.class)
public class SimpleClientTest
{



   @Parameterized.Parameters(name = "sasl={0}, inVM={1}")
   public static Collection<Object[]> data()
   {
      List<Object[]> list = Arrays.asList(new Object[][]{
//         {Boolean.TRUE, Boolean.FALSE}, // TODO: Fix SASL and netty
         {Boolean.FALSE, Boolean.TRUE},
//         {Boolean.TRUE, Boolean.TRUE}, // TODO: Fix SASL and inVM
         {Boolean.FALSE, Boolean.FALSE}});

      System.out.println("Size = " + list.size());
      return list;
   }

   public SimpleClientTest(boolean useSASL, boolean useInVM)
   {
      this.useSASL = useSASL;
      this.useInVM = useInVM;
   }


   private final boolean useSASL;
   private final boolean useInVM;
   private MinimalServer server = new MinimalServer();

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


   @Test
   public void testSimple() throws Exception
   {
      Connector connector = newConnector();
      connector.start();
      AMQPClientConnection clientConnection = connector.connect("127.0.0.1", 5672);

      clientConnection.clientOpen(useSASL ? new SASLPlain("aa", "aa") : null);

      AMQPClientSession session = clientConnection.createClientSession();
      AMQPClientSender clientSender = session.createSender("Test", true);
      Properties props = new Properties();

      MessageImpl message = (MessageImpl) Message.Factory.create();

      Data value = new Data(new Binary(new byte[500]));

      message.setBody(value);
      clientSender.send(message);

      AMQPClientReceiver receiver = session.createReceiver("Test");

      receiver.flow(1000);

      message = (MessageImpl) receiver.receiveMessage(5, TimeUnit.SECONDS);

      System.out.println("Received message " + message.getBody());


   }

   private Connector newConnector()
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

   @Test
   public void testMessagesReceivedInParallel() throws Throwable
   {
      Connector connector1 = newConnector();
      connector1.start();
      final AMQPClientConnection clientConnection = connector1.connect("127.0.0.1", 5672);
      clientConnection.clientOpen(useSASL ? new SASLPlain("AA", "AA") : null);


      final AMQPClientConnection connectionConsumer = connector1.connect("127.0.0.1", 5672);
      connectionConsumer.clientOpen(useSASL ? new SASLPlain("AA", "AA") : null);


      final int numMessages = getNumberOfMessages();
      long time = System.currentTimeMillis();

      final ArrayList<Throwable> exceptions = new ArrayList<>();

      Thread t = new Thread(new Runnable()
      {
         @Override
         public void run()
         {
            try
            {
               AMQPClientSession sessionConsumer = connectionConsumer.createClientSession();
               AMQPClientReceiver receiver = sessionConsumer.createReceiver("Test");
               receiver.flow(numMessages);

               int received = 0;
               int count = numMessages;
               while (count > 0)
               {
                  if (received % 500 == 0 && received > 0)
                  {
                     receiver.flow(500);
                     System.out.println("Received " + received);
                  }
                  received++;

                  try
                  {
                     MessageImpl m = (MessageImpl) receiver.receiveMessage(5, TimeUnit.SECONDS);
                     Assert.assertNotNull("Could not receive message count=" + count + " on consumer", m);
                     count--;
                  }
                  catch (JMSException e)
                  {
                     break;
                  }
               }
            }
            catch (Throwable e)
            {
               exceptions.add(e);
               e.printStackTrace();
            }
         }
      });

      AMQPClientSession session = clientConnection.createClientSession();

      t.start();

      AMQPClientSender sender = session.createSender("Test", true);
      for (int i = 0; i < numMessages; i++)
      {
         MessageImpl message = (MessageImpl) Message.Factory.create();
         message.setBody(new Data(new Binary(new byte[5])));
         sender.send(message);
      }

      long taken = (System.currentTimeMillis() - time);
      System.out.println("taken on send = " + taken + " SASL = " + useSASL + " on simple client");
      t.join();

      for (Throwable e : exceptions)
      {
         throw e;
      }
      taken = (System.currentTimeMillis() - time);
      System.out.println("taken = " + taken + " SASL = " + useSASL + " on simple client");

   }



   @Test
   public void testMeasure()
   {

      long time = System.currentTimeMillis();
      for (int i = 0; i < 100000; i++)
      {

         if (i % 1000 == 0)
         {
            System.out.println("Decoded " + i);
         }
         ByteBuf buf = PooledByteBufAllocator.DEFAULT.heapBuffer(1024 * 1024);
         MessageImpl message = (MessageImpl) Message.Factory.create();
         message.setBody(new Data(new Binary(new byte[5])));

         Properties props = new Properties();
         props.setMessageId("Some String");
         props.setAbsoluteExpiryTime(new Date(System.currentTimeMillis()));
         message.setProperties(props);

         message.encode(new NettyWritable(buf));

         MessageImpl readMessage = (MessageImpl) Message.Factory.create();
         readMessage.decode(buf.array(), buf.arrayOffset() + buf.readerIndex(), buf.readableBytes());
         buf.release();
      }

      long total = System.currentTimeMillis() - time;


      System.out.println("Took " + total);


   }



   protected int getNumberOfMessages()
   {
      // TODO: I can't get passed 10K with netty
      if (useInVM)
      {
         return 100000;
      }
      else
      {
         return 100000;
      }
   }

}
