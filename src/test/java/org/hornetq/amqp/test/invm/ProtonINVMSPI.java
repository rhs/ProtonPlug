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
import org.hornetq.amqp.dealer.AMQPConnection;
import org.hornetq.amqp.dealer.protonimpl.server.ProtonServerConnectionImpl;
import org.hornetq.amqp.dealer.spi.ProtonConnectionSPI;
import org.hornetq.amqp.dealer.spi.ProtonSessionSPI;
import org.hornetq.amqp.test.minimalserver.MinimalSessionSPI;

/**
 * @author Clebert Suconic
 */

public class ProtonINVMSPI implements ProtonConnectionSPI
{

   AMQPConnection returningConnection;

   ProtonServerConnectionImpl serverConnection = new ProtonServerConnectionImpl(new ReturnSPI());

   ExecutorService mainExecutor = Executors.newSingleThreadExecutor();

   ExecutorService returningExecutor = Executors.newSingleThreadExecutor();

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
   public void output(final ByteBuf bytes)
   {
      bytes.retain();
      mainExecutor.execute(new Runnable()
      {
         public void run()
         {
            try
            {
               serverConnection.inputBuffer(bytes);
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
      public void output(final ByteBuf bytes)
      {
         bytes.retain();
         returningExecutor.execute(new Runnable()
         {
            public void run()
            {
               try
               {
                  returningConnection.inputBuffer(bytes);
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
