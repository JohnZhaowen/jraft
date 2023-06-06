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
package com.alipay.sofa.jraft.example.election;

import com.alipay.sofa.jraft.entity.PeerId;

/**
 *
 * @author jiachun.fjc
 */
public class ElectionBootstrap {

    // Start elections by 3 instance. Note that if multiple instances are started on the same machine,
    // the first parameter `dataPath` should not be the same.
    public static void main(final String[] args) {
        if (args.length < 4) {
            System.out
                .println("Usage : java com.alipay.sofa.jraft.example.election.ElectionBootstrap {dataPath} {groupId} {serverId} {initConf}");
            System.out
                .println("Example: java com.alipay.sofa.jraft.example.election.ElectionBootstrap /tmp/server1 election_test 127.0.0.1:8081 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
            System.exit(1);
        }
        // 数据根路径
        final String dataPath = args[0];
        // 组 ID
        final String groupId = args[1];
        // 节点地址
        final String serverIdStr = args[2];
        // 初始节点列表
        final String initialConfStr = args[3];

        /**
         * 由上述实现可以看出在 Leader 选举场景下启动一个 JRaft 节点需要指定 4 个参数，包括：
         *
         * 数据存储根路径，用于存储日志、元数据，以及快照数据。
         * 组 ID，一个组可以看做是一个独立的 Raft 集群，JRaft 支持 MULTI-RAFT-GROUP。
         * 节点地址，即当前节点的 IP 和端口号。
         * 初始集群节点列表，即初始构成 JRaft 集群的节点列表。
         */

        // 节点初始化参数设置
        final ElectionNodeOptions electionOpts = new ElectionNodeOptions();
        electionOpts.setDataPath(dataPath);
        electionOpts.setGroupId(groupId);
        electionOpts.setServerAddress(serverIdStr);
        electionOpts.setInitialServerAddressList(initialConfStr);

        //ElectionNode 是整个启动示例的核心实现类
        final ElectionNode node = new ElectionNode();

        // 注册监听器，监听当前节点竞选 leader 成功或 stepdown
        node.addLeaderStateListener(new LeaderStateListener() {

            @Override
            public void onLeaderStart(long leaderTerm) {
                PeerId serverId = node.getNode().getLeaderId();
                String ip = serverId.getIp();
                int port = serverId.getPort();
                System.out.println("[ElectionBootstrap] Leader's ip is: " + ip + ", port: " + port);
                System.out.println("[ElectionBootstrap] Leader start on term: " + leaderTerm);
            }

            @Override
            public void onLeaderStop(long leaderTerm) {
                System.out.println("[ElectionBootstrap] Leader stop on term: " + leaderTerm);
            }
        });
        // 实现了初始化和启动单个 JRaft 节点的逻辑
        node.init(electionOpts);
    }
}
