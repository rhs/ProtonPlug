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

package org.hornetq.amqp.dealer.spi;

import io.netty.buffer.ByteBuf;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.message.ProtonJMessage;
import org.hornetq.amqp.dealer.protonimpl.ProtonPlugSender;
import org.hornetq.amqp.dealer.protonimpl.ProtonSession;

/**
 * These are methods where the Proton Plug component will call your server
 * @author Clebert Suconic
 */

public interface ProtonSessionSPI
{

   void init(ProtonSession session, String user, String passcode, boolean transacted) throws Exception;

   void start();

   void onFlowConsumer(Object consumer, int credits);

   Object createSender(ProtonPlugSender protonSender, String queue, String filer, boolean browserOnly) throws Exception;

   void startSender(Object brokerConsumer) throws Exception;

   void createTemporaryQueue(String queueName) throws Exception;

   boolean queueQuery(String queueName) throws Exception;

   void closeSender(Object brokerConsumer) throws Exception;

   // This one can be a lot improved
   ProtonJMessage encodeMessage(Object message, int deliveryCount) throws Exception;

   Binary getCurrentTXID();

   String tempQueueName();

   void commitCurrentTX() throws Exception;

   void rollbackCurrentTX() throws Exception;

   void close();


   void ack(Object brokerConsumer, Object message);

   /**
    * @param brokerConsumer
    * @param message
    * @param updateCounts this identified if the cancel was because of a failure or just cleaning up the
    *                     client's cache.
    *                     in some implementations you could call this failed
    */
   void cancel(Object brokerConsumer, Object message, boolean updateCounts);


   void resumeDelivery(Object consumer);


   /**
    *
    * @param delivery
    * @param address
    * @param messageFormat
    * @param messageEncoded a Heap Buffer ByteBuffer (safe to convert into byte[])
    */
   void serverSend(Receiver receiver, Delivery delivery, String address, int messageFormat, ByteBuf messageEncoded) throws Exception;

}
