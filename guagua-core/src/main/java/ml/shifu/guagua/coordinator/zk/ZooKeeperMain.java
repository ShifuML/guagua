/*
 * Copyright [2013-2016] PayPal Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ml.shifu.guagua.coordinator.zk;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

/**
 * Add log4j appender to {@link QuorumPeerMain} in programming way.
 * 
 * @author Zhang David (pengzhang@paypal.com)
 */
public class ZooKeeperMain {

    public static void main(String[] args) {
        org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
        rootLogger.setLevel(Level.INFO);
        // Define log pattern layout
        PatternLayout layout = new PatternLayout("%d{yyyy-MM-dd hh:mm:ss}:%p %t %c{n} - %m%n");
        // Add console appender to root logger
        rootLogger.addAppender(new ConsoleAppender(layout));
        QuorumPeerMain.main(args);
    }
}
