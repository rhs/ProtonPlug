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

package org.hornetq.amqp.test.minimalclient;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.hornetq.amqp.dealer.AMQPClientConnection;
import org.hornetq.amqp.dealer.protonimpl.client.ProtonClientConnectionFactory;

/**
 * @author Clebert Suconic
 */

public class SimpleAMQPConnector implements Connector
{
   private Bootstrap bootstrap;

   public void start()
   {

      bootstrap = new Bootstrap();
      bootstrap.channel(NioSocketChannel.class);
      bootstrap.group(new NioEventLoopGroup(10));

      bootstrap.handler(
         new ChannelInitializer<Channel>()
         {
            public void initChannel(Channel channel) throws Exception
            {
            }
         }
      );
   }

   public AMQPClientConnection connect(String host, int port) throws Exception
   {
      SocketAddress remoteDestination = new InetSocketAddress(host, port);

      ChannelFuture future = bootstrap.connect(remoteDestination);

      future.awaitUninterruptibly();

      AMQPClientSPI clientConnectionSPI = new AMQPClientSPI(future.channel());

      final AMQPClientConnection connection = (AMQPClientConnection)ProtonClientConnectionFactory.getFactory().createConnection(clientConnectionSPI, false);

      future.channel().pipeline().addLast(
         new ChannelDuplexHandler()
         {

            @Override
            public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception
            {
               ByteBuf buffer = (ByteBuf) msg;
               connection.inputBuffer(buffer);
            }
         }
      );


      return connection;
   }
}
