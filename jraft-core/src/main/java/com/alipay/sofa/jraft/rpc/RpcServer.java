/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.rpc;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.rpc.impl.ConnectionClosedEventListener;

/**
 *
 * RPC有两种实现：
 * 1.grpc
 * 2.sofaRpc的bolt
 *
 * @author jiachun.fjc
 */
public interface RpcServer extends Lifecycle<Void> {

    /**
     * Register a conn closed event listener.
     *
     * @param listener the event listener.
     */
    void registerConnectionClosedEventListener(final ConnectionClosedEventListener listener);

    /**
     * Register user processor.
     * 注册请求处理器，当客户端发送请求过来时，被封装成data，服务端需要根据data的class类型来分配不同的处理器
     * 这里注册的处理器就是对应不同class类型的data
     *
     * @param processor the user processor which has a interest
     */
    void registerProcessor(final RpcProcessor<?> processor);

    /**
     *
     * @return bound port
     */
    int boundPort();
}
