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

package org.hornetq.amqp.test.invm;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import org.hornetq.amqp.dealer.AMQPConnection;
import org.hornetq.amqp.dealer.protonimpl.server.ProtonServerConnectionImpl;
import org.hornetq.amqp.dealer.spi.ProtonConnectionSPI;
import org.hornetq.amqp.dealer.spi.ProtonSessionSPI;
import org.hornetq.amqp.dealer.util.ByteUtil;
import org.hornetq.amqp.dealer.util.DebugInfo;
import org.hornetq.amqp.test.minimalserver.MinimalSessionSPI;

/**
 * @author Clebert Suconic
 */

public class ProtonINVMSPI implements ProtonConnectionSPI
{

   AMQPConnection returningConnection;

   ProtonServerConnectionImpl serverConnection = new ProtonServerConnectionImpl(new ReturnSPI());

   final ExecutorService mainExecutor = Executors.newSingleThreadExecutor();

   final ExecutorService returningExecutor = Executors.newSingleThreadExecutor();

   public ProtonINVMSPI()
   {
      mainExecutor.execute(new Runnable()
      {
         public void run()
         {
            Thread.currentThread().setName("MainExecutor-INVM");
         }
      });
      returningExecutor.execute(new Runnable()
      {
         public void run()
         {
            Thread.currentThread().setName("ReturningExecutor-INVM");
         }
      });
   }

   @Override
   public Executor newSingleThreadExecutor()
   {
      return mainExecutor;
   }

   @Override
   public String[] getSASLMechanisms()
   {
      return new String[] {"PLAIN"};
   }

   @Override
   public void close()
   {
      mainExecutor.shutdown();
   }

   @Override
   public void output(final ByteBuf bytes, final ChannelFutureListener futureCompletion)
   {
      if (DebugInfo.debug)
      {
         ByteUtil.debugFrame("InVM->", bytes);
      }

      bytes.retain();
      mainExecutor.execute(new Runnable()
      {
         public void run()
         {
            try
            {
               if (DebugInfo.debug)
               {
                  ByteUtil.debugFrame("InVMDone->", bytes);
               }
               serverConnection.inputBuffer(bytes);
               try
               {
                  futureCompletion.operationComplete(null);
               }
               catch (Exception e)
               {
                  e.printStackTrace();
               }
            }
            finally
            {
               bytes.release();
            }
         }
      });
   }

   @Override
   public void setConnection(AMQPConnection connection)
   {
      returningConnection = connection;
   }

   @Override
   public AMQPConnection getConnection()
   {
      return returningConnection;
   }

   @Override
   public ProtonSessionSPI createSessionSPI(AMQPConnection connection)
   {
      return null;
   }

   class ReturnSPI implements ProtonConnectionSPI
   {
      @Override
      public Executor newSingleThreadExecutor()
      {
         return mainExecutor;
      }

      @Override
      public String[] getSASLMechanisms()
      {
         return new String[] {"PLAIN"};
      }

      @Override
      public void close()
      {

      }

      @Override
      public void output(final ByteBuf bytes, final ChannelFutureListener futureCompletion)
      {

         if (DebugInfo.debug)
         {
            ByteUtil.debugFrame("InVM<-", bytes);
         }


         bytes.retain();
         returningExecutor.execute(new Runnable()
         {
            public void run()
            {
               try
               {

                  if (DebugInfo.debug)
                  {
                     ByteUtil.debugFrame("InVM done<-", bytes);
                  }

                  returningConnection.inputBuffer(bytes);
                  try
                  {
                     futureCompletion.operationComplete(null);
                  }
                  catch (Exception e)
                  {
                     e.printStackTrace();
                  }

               }
               finally
               {
                  bytes.release();
               }
            }
         });
      }

      @Override
      public ProtonSessionSPI createSessionSPI(AMQPConnection connection)
      {
         return new MinimalSessionSPI();
      }

      @Override
      public void setConnection(AMQPConnection connection)
      {

      }

      @Override
      public AMQPConnection getConnection()
      {
         return null;
      }
   }
}
