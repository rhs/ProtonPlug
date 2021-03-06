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

package org.hornetq.amqp.dealer;

import io.netty.buffer.ByteBuf;

/**
 * @author Clebert Suconic
 */

public interface AMQPConnection
{

   void close();

   boolean checkDataReceived();

   long getCreationTime();

   String getLogin();

   String getPasscode();

   /**
    * This is for the Remoting layer to push bytes on the AMQP Connection
    * The buffer readerIndex should be at the latest read byte after this method is called
    * @param buffer
    * @return
    */
   void inputBuffer(ByteBuf buffer);

   void flush();

   void setSaslCallback(Runnable runnable);

}
