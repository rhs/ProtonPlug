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

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

import io.netty.buffer.ByteBuf;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Collector;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.TransportResultFactory;
import org.hornetq.amqp.dealer.SASL;

/**
 * @author Clebert Suconic
 */

public abstract class ProtonTrio
{
   static ThreadLocal<Boolean> inDispatch = new ThreadLocal<>();

   private Sasl sasl;

   private Runnable saslCallback;

   protected final Transport transport = Proton.transport();

   protected final Connection connection = Proton.connection();

   protected final Collector collector = Proton.collector();

   protected final Object lock = new Object();

   private String username;

   private String password;

   private Executor executor;


   public ProtonTrio(Executor executor)
   {

      // TODO parameterize maxFrameSize
      transport.setMaxFrameSize(1024 * 1024);
      transport.bind(connection);
      connection.collect(collector);
      this.executor = executor;
   }

   public void setSaslCallback(Runnable runnable)
   {
      this.saslCallback = runnable;
   }

   public Transport getTransport()
   {
      return transport;
   }

   public Connection getConnection()
   {
      return connection;
   }


   final Runnable dispatchRunnable = new Runnable()
   {
      public void run()
      {
         dispatch();
      }
   };

   public String getUsername()
   {
      return username;
   }

   public String getPassword()
   {
      return password;
   }


   public Object getLock()
   {
      return lock;
   }

   public void createServerSasl(String... mechanisms)
   {
      sasl = transport.sasl();
      sasl.server();
      sasl.setMechanisms(mechanisms);
   }

   public void createClientSasl(SASL clientSASL)
   {
      if (clientSASL != null)
      {
         sasl = transport.sasl();
         sasl.setMechanisms(clientSASL.getName());
         byte[] initialSasl = clientSASL.getBytes();
         sasl.send(initialSasl, 0, initialSasl.length);
      }
   }


   public void close()
   {
      synchronized (lock)
      {
         connection.close();
         transport.close();
         dispatch();
      }
   }

   /**
    * this method will change the readerIndex on bytes to the latest read position
    */
   public void pump(ByteBuf bytes)
   {
      if (bytes.readableBytes() < 8)
      {
         return;
      }


      try
      {
         synchronized (lock)
         {

            final ByteBuffer input = transport.getInputBuffer();

            while (bytes.readerIndex() < bytes.writerIndex())
            {
               int remaining = input.remaining();
               if (remaining == 0)
               {
                  System.err.println("Buffer full!!!");
                  break;
               }
               int min = Math.min(remaining, bytes.readableBytes());
               ByteBuffer tmp = bytes.internalNioBuffer(bytes.readerIndex(), min);
               input.put(tmp);
               if (!processBuffer())
               {
                  System.err.println("DEBUG This.. Process Buffer returned false!!!!!!!!!!!!!");
                  break;
               }
               dispatch();
               bytes.readerIndex(bytes.readerIndex() + min);
            }
         }
      }
      finally
      {
         // After everything is processed we still need to check for more dispatches!
         dispatch();
      }
   }


   private boolean processBuffer()
   {
      if (transport.processInput() != TransportResultFactory.ok())
      {
         System.err.println("Couldn't process header!!!");
         connection.setCondition(new ErrorCondition(AmqpError.ILLEGAL_STATE, "Error processing header"));
         return false;
      }

      checkSASL();
      dispatch();
      return true;
   }

   private void checkSASL()
   {
      if (sasl != null && sasl.getRemoteMechanisms().length > 0)
      {

         byte[] dataSASL = new byte[sasl.pending()];
         sasl.recv(dataSASL, 0, dataSASL.length);

         if (sasl.getRemoteMechanisms()[0].equals("PLAIN"))
         {
            setUserPass(dataSASL);
         }

         // TODO: do the proper SASL authorization here
         // call an abstract method (authentication (bytes[])
         sasl.done(Sasl.SaslOutcome.PN_SASL_OK);
         sasl = null;
         if (saslCallback != null)
         {
            saslCallback.run();
         }
      }
   }

   /** It will only start a dispatch if it's not on the dispatch thread already */
   public void dispatchIfNeeded()
   {
      if (inDispatch.get() != null)
      {
         return;
      }
      dispatch();
   }

   public void dispatch()
   {


      if (inDispatch.get() != null)
      {
//         new Exception("Already in dispatch mode, using executor").printStackTrace();
         executor.execute(dispatchRunnable);
         return;
      }

      inDispatch.set(Boolean.TRUE);

      try
      {
         internalDispatch();
      }
      finally
      {
         inDispatch.set(null);
      }

   }

   protected void internalDispatch()
   {
      synchronized (lock)
      {
         try
         {
            Event ev;
            while ((ev = collector.peek()) != null)
            {
               dispatch(ev);
               collector.pop();
            }
         }
         catch (Exception e)
         {
            connection.setCondition(new ErrorCondition(AmqpError.INTERNAL_ERROR, e.getMessage()));
         }


         // forcing transport on every dispatch call
         onTransport(transport);
      }
   }


   protected void onRemoteState(Connection connection) throws Exception
   {
      if (connection.getRemoteState() == EndpointState.ACTIVE)
      {
         connection.open();
         connectionOpened(connection);
      }
      else if (connection.getRemoteState() == EndpointState.CLOSED)
      {
         connection.close();
         connectionClosed(connection);
      }
   }

   protected abstract void connectionOpened(Connection connection) throws Exception;

   protected abstract void connectionClosed(Connection connection) throws Exception;

   protected void onRemoteState(Session session)
   {
      if (session.getRemoteState() == EndpointState.ACTIVE)
      {
         session.open();
         sessionOpened(session);
      }
      else if (session.getRemoteState() == EndpointState.CLOSED)
      {
         session.close();
         sessionClosed(session);
      }
   }

   protected abstract void sessionOpened(Session session);

   protected abstract void sessionClosed(Session session);

   protected void onRemoteState(Link link)
   {
      if (link.getRemoteState() == EndpointState.ACTIVE)
      {
         link.open();
         linkOpened(link);
      }
      else if (link.getRemoteState() == EndpointState.CLOSED)
      {
         link.close();
         linkClosed(link);
      }
      else if (link.getLocalState() == EndpointState.ACTIVE)
      {
         linkActive(link);
      }
   }

   protected abstract void linkOpened(Link link);

   protected abstract void linkClosed(Link link);

   /**
    * Do we really need this? This used to be done with:
    * <p/>
    * <p/>
    * link = (LinkImpl) protonConnection.linkHead(ProtonProtocolManager.ACTIVE, ProtonProtocolManager.ANY_ENDPOINT_STATE);
    * while (link != null)
    * {
    * try
    * {
    * protonProtocolManager.handleActiveLink(link);
    * }
    * catch (HornetQAMQPException e)
    * {
    * link.setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
    * }
    * link = (LinkImpl) link.next(ProtonProtocolManager.ACTIVE, ProtonProtocolManager.ANY_ENDPOINT_STATE);
    * }
    */
   protected abstract void linkActive(Link link);

   protected void onLocalState(Connection connection)
   {
   }

   protected void onLocalState(Session session)
   {
   }

   protected void onLocalState(Link link)
   {
   }

   protected void onFlow(Link link)
   {

   }

   protected abstract void onDelivery(Delivery delivery);

   protected abstract void onTransport(Transport transport);

   private void dispatch(Event event) throws Exception
   {

      switch (event.getType())
      {
         case CONNECTION_REMOTE_OPEN:
         case CONNECTION_REMOTE_CLOSE:
            onRemoteState(event.getConnection());
            break;
         case CONNECTION_OPEN:
         case CONNECTION_CLOSE:
            onLocalState(event.getConnection());
            break;
         case SESSION_REMOTE_OPEN:
         case SESSION_REMOTE_CLOSE:
            onRemoteState(event.getSession());
            break;
         case SESSION_OPEN:
         case SESSION_CLOSE:
            onLocalState(event.getSession());
            break;
         case LINK_REMOTE_OPEN:
         case LINK_REMOTE_CLOSE:
            onRemoteState(event.getLink());
            break;
         case LINK_OPEN:
         case LINK_CLOSE:
            onLocalState(event.getLink());
            break;
         case LINK_FLOW:
            onFlow(event.getLink());
            break;
         case TRANSPORT:
            onTransport(event.getTransport());
            break;
         case DELIVERY:
            onDelivery(event.getDelivery());
            break;
      }
   }


   /**
    * this is to be used with SASL
    */
   private void setUserPass(byte[] data)
   {
      String bytes = new String(data);
      String[] credentials = bytes.split(Character.toString((char) 0));
      int offSet = 0;
      if (credentials.length > 0)
      {
         if (credentials[0].length() == 0)
         {
            offSet = 1;
         }

         if (credentials.length >= offSet)
         {
            username = credentials[offSet];
         }
         if (credentials.length >= (offSet + 1))
         {
            password = credentials[offSet + 1];
         }
      }
   }


}
