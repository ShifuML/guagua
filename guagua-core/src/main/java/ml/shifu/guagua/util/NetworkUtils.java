/*
 * Copyright [2013-2015] eBay Software Foundation
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
package ml.shifu.guagua.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Random;

import ml.shifu.guagua.GuaguaRuntimeException;

/**
 * {@link NetworkUtils} is used as TCP server or port helper functions.
 */
public final class NetworkUtils {

    private NetworkUtils() {
    }

    /**
     * Client connection retry count
     */
    public static final int RETRY_COUNT = 3;

    /**
     * How many ports will be used to launch embed zookeeper server.
     */
    public static final int TRY_PORT_COUNT = 20;

    private static final Random RANDOM = new Random();

    /**
     * Check whether a server is alive.
     * 
     * @param host
     *            the server host
     * @param port
     *            the server port
     * @return true if a server is alive, false if a server is not alive.
     */
    public static boolean isServerAlive(InetAddress host, int port) {
        Socket socket = null;
        int i = 0;
        while(i < RETRY_COUNT) {
            try {
                socket = new Socket(host, port);
                break;
            } catch (IOException e) {
                i++;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                }
            } finally {
                try {
                    if(socket != null) {
                        socket.close();
                    }
                } catch (IOException ignore) {
                }
            }
        }
        return i != RETRY_COUNT;
    }

    /**
     * Get valid TCP server port.
     * 
     * @return valid TCP server port
     */
    public static int getValidServerPort(int iniatialPort) {
        assert iniatialPort > 100;

        // add random port to avoid port in the same small range.
        if(System.currentTimeMillis() % 2 == 0) {
            iniatialPort += RANDOM.nextInt(100);
        } else {
            iniatialPort -= RANDOM.nextInt(100);
        }

        int zkValidPort = -1;
        for(int i = iniatialPort; i < (iniatialPort + TRY_PORT_COUNT); i++) {
            try {
                if(!isServerAlive(InetAddress.getLocalHost(), i)) {
                    zkValidPort = i;
                    break;
                }
            } catch (UnknownHostException e) {
                throw new GuaguaRuntimeException(e);
            }
        }
        if(zkValidPort == -1) {
            throw new GuaguaRuntimeException("Too many ports are used, please submit guagua app later.");
        }
        return zkValidPort;
    }

}
