/**
 * Copyright [2013-2014] eBay Software Foundation
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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import ml.shifu.guagua.GuaguaConstants;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooKeeper provides only atomic operations. GuaguaZooKeeper provides additional non-atomic operations that are useful.
 * It also provides wrappers to deal with ConnectionLossException. All methods of this class should be thread-safe.
 * 
 */
public class GuaguaZooKeeper {

    /** Internal logger */
    private static final Logger LOG = LoggerFactory.getLogger(GuaguaZooKeeper.class);

    /** Length of the ZK sequence number */
    private static final int SEQUENCE_NUMBER_LENGTH = 10;
    /** Internal ZooKeeper */
    private final ZooKeeper zooKeeper;
    /** Number of max attempts to retry when failing due to connection loss */
    private final int maxRetryAttempts;
    /** Milliseconds to wait before trying again due to connection loss */
    private final long retryWaitMsecs;

    /**
     * Constructor to connect to ZooKeeper, make progress
     * 
     * @param connectString
     *            Comma separated host:port pairs, each corresponding to a zk server. e.g.
     *            "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" If the optional chroot suffix is used the example would
     *            look like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a" where the client would be rooted at
     *            "/app/a" and all paths would be relative to this root - ie getting/setting/etc... "/foo/bar" would
     *            result in operations being run on "/app/a/foo/bar" (from the
     *            server perspective).
     * @param sessionTimeout
     *            Session timeout in milliseconds
     * @param maxRetryAttempts
     *            Max retry attempts during connection loss
     * @param retryWaitMsecs
     *            Msecs to wait when retrying due to connection loss
     * @param watcher
     *            A watcher object which will be notified of state changes, may also be notified for node events
     * @throws IOException
     *             In case of any io exception to connect to zookeeper server.
     */
    public GuaguaZooKeeper(String connectString, int sessionTimeout, int maxRetryAttempts, int retryWaitMsecs,
            Watcher watcher) throws IOException {
        if(maxRetryAttempts <= 0) {
            throw new IllegalArgumentException("'maxRetryAttempts' should be larger than 0.");
        }
        if(retryWaitMsecs <= 0) {
            throw new IllegalArgumentException("'retryWaitMsecs' should be larger than 0.");
        }
        this.zooKeeper = new ZooKeeper(connectString, sessionTimeout, watcher);
        this.maxRetryAttempts = maxRetryAttempts;
        this.retryWaitMsecs = retryWaitMsecs;
    }

    /**
     * Provides a possibility of a creating a path consisting of more than one znode (not atomic). If recursive is
     * false, operates exactly the same as create().
     * 
     * @param path
     *            path to create
     * @param data
     *            data to set on the final znode
     * @param acl
     *            acls on each znode created
     * @param createMode
     *            only affects the final znode
     * @param recursive
     *            if true, creates all ancestors
     * @return Actual created path
     * @throws KeeperException
     * @throws InterruptedException
     *             Both KeeperException InterruptedException are thrown from {@link ZooKeeper} methods.
     * @throws NullPointerException
     *             If {@code path} is null.
     */
    public String createExt(final String path, final byte[] data, final List<ACL> acl, final CreateMode createMode,
            final boolean recursive) throws KeeperException, InterruptedException {
        // LOG.info("createExt: Creating path {}", path);
        String result = retryOperation(new GuaguaZooKeeperOperation<String>() {
            @Override
            public String execute() throws KeeperException, InterruptedException {
                if(!recursive) {
                    return getZooKeeper().create(path, data, acl, createMode);
                }

                try {
                    return getZooKeeper().create(path, data, acl, createMode);
                } catch (KeeperException.NoNodeException e) {
                    LOG.warn("createExt: Cannot directly create node {} because of NoNodeException.", path);
                }

                int pos = path.indexOf(GuaguaConstants.ZOOKEEPER_SEPARATOR, 1);
                for(; pos != -1; pos = path.indexOf(GuaguaConstants.ZOOKEEPER_SEPARATOR, pos + 1)) {
                    String subPath = path.substring(0, pos);
                    try {
                        // set intermediate path to PERSISTENT, because other EPHEMERAL or SEQUENTIAL znode should be
                        // attached to PERSISTENT znode.
                        getZooKeeper().create(subPath, null, acl, CreateMode.PERSISTENT);
                    } catch (KeeperException.NodeExistsException e) {
                        LOG.warn("createExt: Znode {} already exists", subPath);
                    }
                }
                return getZooKeeper().create(path, data, acl, createMode);
            }
        });
        // LOG.info("createExt done: Creating path {}", path);
        return result;
    }

    /**
     * Data structure for handling the output of createOrSet()
     */
    public static class PathStat {
        /** Path to created znode (if any) */
        private String path;
        /** Stat from set znode (if any) */
        private Stat stat;

        /**
         * Put in results from createOrSet()
         * 
         * @param path
         *            Path to created znode (or null)
         * @param stat
         *            Stat from set znode (if set)
         */
        public PathStat(String path, Stat stat) {
            this.path = path;
            this.stat = stat;
        }

        /**
         * Get the path of the created znode if it was created.
         * 
         * @return Path of created znode or null if not created
         */
        public String getPath() {
            return path;
        }

        /**
         * Get the stat of the set znode if set
         * 
         * @return Stat of set znode or null if not set
         */
        public Stat getStat() {
            return stat;
        }
    }

    /**
     * Create a znode. Set the znode if the created znode already exists.
     * 
     * @param path
     *            path to create
     * @param data
     *            data to set on the final znode
     * @param acl
     *            acls on each znode created
     * @param createMode
     *            only affects the final znode
     * @param recursive
     *            if true, creates all ancestors
     * @param version
     *            Version to set if setting
     * @return Path of created znode or Stat of set znode
     * @throws InterruptedException
     * @throws KeeperException
     *             Both KeeperException InterruptedException are thrown from {@link ZooKeeper} methods.
     */
    public PathStat createOrSetExt(final String path, byte[] data, List<ACL> acl, CreateMode createMode,
            boolean recursive, int version) throws KeeperException, InterruptedException {
        String createdPath = null;
        Stat setStat = null;
        try {
            createdPath = createExt(path, data, acl, createMode, recursive);
        } catch (KeeperException.NodeExistsException e) {
            LOG.warn("createOrSet: Node exists on path {}", path);
            setStat = getZooKeeper().setData(path, data, version);
        }
        return new PathStat(createdPath, setStat);
    }

    /**
     * Create a znode if there is no other znode there
     * 
     * @param path
     *            path to create
     * @param data
     *            data to set on the final znode
     * @param acl
     *            acls on each znode created
     * @param createMode
     *            only affects the final znode
     * @param recursive
     *            if true, creates all ancestors
     * @return Path of created znode or Stat of set znode
     * @throws InterruptedException
     * @throws KeeperException
     *             Both KeeperException InterruptedException are thrown from {@link ZooKeeper} methods.
     */
    public PathStat createOnceExt(final String path, byte[] data, List<ACL> acl, CreateMode createMode,
            boolean recursive) throws KeeperException, InterruptedException {
        try {
            return new PathStat(createExt(path, data, acl, createMode, recursive), null);
        } catch (KeeperException.NodeExistsException e) {
            LOG.warn("createOnceExt: Node already exists on path {}", path);
        }
        return null;
    }

    /**
     * Delete a path recursively. When the deletion is recursive, it is a non-atomic operation, hence, not part of
     * ZooKeeper.
     * 
     * @param path
     *            path to remove (i.e. /tmp will remove /tmp/1 and /tmp/2)
     * @param version
     *            expected version (-1 for all)
     * @param recursive
     *            if true, remove all children, otherwise behave like remove()
     * @throws InterruptedException
     * @throws KeeperException
     *             Both KeeperException InterruptedException are thrown from {@link ZooKeeper} methods.
     */
    public void deleteExt(final String path, final int version, final boolean recursive) throws InterruptedException,
            KeeperException {
        retryOperation(new GuaguaZooKeeperOperation<Void>() {
            @Override
            public Void execute() throws KeeperException, InterruptedException {
                if(!recursive) {
                    getZooKeeper().delete(path, version);
                    return null;
                }
                try {
                    getZooKeeper().delete(path, version);
                    return null;
                } catch (KeeperException.NotEmptyException e) {
                    LOG.warn("deleteExt: Cannot directly remove node {}", path);
                }

                List<String> childList = getZooKeeper().getChildren(path, false);
                for(String child: childList) {
                    deleteExt(path + GuaguaConstants.ZOOKEEPER_SEPARATOR + child, -1, true);
                }

                getZooKeeper().delete(path, version);
                return null;
            }
        });
    }

    /**
     * Return the stat of the node of the given path. Return null if no such a node exists.
     * <p>
     * If the watch is true and the call is successful (no exception is thrown), a watch will be left on the node with
     * the given path. The watch will be triggered by a successful operation that creates/delete the node or sets the
     * data on the node.
     * 
     * @param path
     *            the node path
     * @param watch
     *            whether need to watch this node
     * @return the stat of the node of the given path; return null if no such a node exists.
     * @throws KeeperException
     *             If the server signals an error
     * @throws InterruptedException
     *             If the server transaction is interrupted.
     */
    public Stat exists(final String path, final boolean watch) throws KeeperException, InterruptedException {
        return retryOperation(new GuaguaZooKeeperOperation<Stat>() {
            @Override
            public Stat execute() throws KeeperException, InterruptedException {
                return getZooKeeper().exists(path, watch);
            }
        });
    }

    /**
     * Return the stat of the node of the given path. Return null if no such a node exists.
     * <p>
     * If the watch is non-null and the call is successful (no exception is thrown), a watch will be left on the node
     * with the given path. The watch will be triggered by a successful operation that creates/delete the node or sets
     * the data on the node.
     * 
     * @param path
     *            the node path
     * @param watcher
     *            explicit watcher
     * @return the stat of the node of the given path; return null if no such a
     *         node exists.
     * @throws KeeperException
     *             If the server signals an error
     * @throws InterruptedException
     *             If the server transaction is interrupted.
     * @throws IllegalArgumentException
     *             if an invalid path is specified
     */
    public Stat exists(final String path, final Watcher watcher) throws KeeperException, InterruptedException {
        return retryOperation(new GuaguaZooKeeperOperation<Stat>() {
            @Override
            public Stat execute() throws KeeperException, InterruptedException {
                return getZooKeeper().exists(path, watcher);
            }
        });
    }

    /**
     * Return the data and the stat of the node of the given path.
     * <p>
     * If the watch is non-null and the call is successful (no exception is thrown), a watch will be left on the node
     * with the given path. The watch will be triggered by a successful operation that sets data on the node, or deletes
     * the node.
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown if no node with the given path exists.
     * 
     * @param path
     *            the given path
     * @param watcher
     *            explicit watcher
     * @param stat
     *            the stat of the node
     * @return the data of the node
     * @throws KeeperException
     *             If the server signals an error with a non-zero
     *             error code
     * @throws InterruptedException
     *             If the server transaction is interrupted.
     * @throws IllegalArgumentException
     *             if an invalid path is specified
     */
    public byte[] getData(final String path, final Watcher watcher, final Stat stat) throws KeeperException,
            InterruptedException {
        return retryOperation(new GuaguaZooKeeperOperation<byte[]>() {
            @Override
            public byte[] execute() throws KeeperException, InterruptedException {
                return getZooKeeper().getData(path, watcher, stat);
            }
        });
    }

    /**
     * Return the data and the stat of the node of the given path.
     * <p>
     * If the watch is true and the call is successful (no exception is thrown), a watch will be left on the node with
     * the given path. The watch will be triggered by a successful operation that sets data on the node, or deletes the
     * node.
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown if no node with the given path exists.
     * 
     * @param path
     *            the given path
     * @param watch
     *            whether need to watch this node
     * @param stat
     *            the stat of the node
     * @return the data of the node
     * @throws KeeperException
     *             If the server signals an error with a non-zero error code
     * @throws InterruptedException
     *             If the server transaction is interrupted.
     */
    public byte[] getData(final String path, final boolean watch, final Stat stat) throws KeeperException,
            InterruptedException {
        return retryOperation(new GuaguaZooKeeperOperation<byte[]>() {
            @Override
            public byte[] execute() throws KeeperException, InterruptedException {
                return getZooKeeper().getData(path, watch, stat);
            }
        });
    }

    /**
     * Get the children of the path with extensions.
     * Extension 1: Sort the children based on {@link Code childComparator} number
     * Extension 2: Get the full path instead of relative path
     * 
     * @param path
     *            path to znode
     * @param watch
     *            set the watch?
     * @param fullPath
     *            if true, get the fully znode path back
     * @param childComparator
     *            comparator to sort children.
     * @return list of children
     * @throws InterruptedException
     * @throws KeeperException
     *             Both KeeperException InterruptedException are thrown from {@link ZooKeeper} methods.
     */
    public List<String> getChildrenExt(final String path, final boolean watch, final boolean fullPath,
            final Comparator<String> childComparator) throws KeeperException, InterruptedException {
        return retryOperation(new GuaguaZooKeeperOperation<List<String>>() {
            @Override
            public List<String> execute() throws KeeperException, InterruptedException {
                List<String> childList = getZooKeeper().getChildren(path, watch);
                /* Sort children according to the sequence number, if desired */
                if(childComparator != null) {
                    Collections.sort(childList, childComparator);
                }
                // remove guava dependency to avoid making core depending on two many libs.
                if(fullPath) {
                    List<String> fullChildList = new ArrayList<String>();
                    for(String child: childList) {
                        fullChildList.add(path + GuaguaConstants.ZOOKEEPER_SEPARATOR + child);
                    }
                    return fullChildList;
                }
                return childList;
            }
        });
    }

    /**
     * Get the children of the path with extensions.
     * Extension 1: Sort the children based on sequence number
     * Extension 2: Get the full path instead of relative path
     * 
     * @param path
     *            path to znode
     * @param watch
     *            set the watch?
     * @param sequenceSorted
     *            sort by the sequence number
     * @param fullPath
     *            if true, get the fully znode path back
     * @return list of children
     * @throws InterruptedException
     * @throws KeeperException
     *             Both KeeperException InterruptedException are thrown from {@link ZooKeeper} methods.
     */
    public List<String> getChildrenExt(final String path, final boolean watch, final boolean sequenceSorted,
            final boolean fullPath) throws KeeperException, InterruptedException {
        return retryOperation(new GuaguaZooKeeperOperation<List<String>>() {
            @Override
            public List<String> execute() throws KeeperException, InterruptedException {
                List<String> childList = getZooKeeper().getChildren(path, watch);
                /* Sort children according to the sequence number, if desired */
                if(sequenceSorted) {
                    Collections.sort(childList, new SequenceComparator());
                }
                // remove guava dependency to avoid making core depending on two many libs.
                if(fullPath) {
                    List<String> fullChildList = new ArrayList<String>();
                    for(String child: childList) {
                        fullChildList.add(path + GuaguaConstants.ZOOKEEPER_SEPARATOR + child);
                    }
                    return fullChildList;
                }
                return childList;
            }
        });
    }

    private static class SequenceComparator implements Comparator<String> , Serializable{

        private static final long serialVersionUID = 4555088814306270860L;

        @Override
        public int compare(String s1, String s2) {
            if((s1.length() <= SEQUENCE_NUMBER_LENGTH) || (s2.length() <= SEQUENCE_NUMBER_LENGTH)) {
                throw new RuntimeException(String.format(
                        "getChildrenExt: Invalid length for sequence  sorting > %s for s1 (%s) or s2 (%s)",
                        SEQUENCE_NUMBER_LENGTH, s1.length(), s2.length()));
            }
            Integer s1SequenceNumber = Integer.parseInt(s1.substring(s1.length() - SEQUENCE_NUMBER_LENGTH));
            Integer s2SequenceNumber = Integer.parseInt(s2.substring(s2.length() - SEQUENCE_NUMBER_LENGTH));
            return s1SequenceNumber.compareTo(s2SequenceNumber);
        }
    }

    /**
     * Close this client object. Once the client is closed, its session becomes invalid. All the ephemeral nodes in the
     * ZooKeeper server associated with the session will be removed. The watches left on those nodes (and on their
     * parents) will be triggered.
     * 
     * @throws InterruptedException
     *             in case of InterruptedException from {@code ZooKeeper#close()};
     */
    public void close() throws InterruptedException {
        getZooKeeper().close();
    }

    /**
     * Perform the given operation, retrying if the connection fails
     * 
     * @return object. it needs to be cast to the callee's expected return type.
     */
    protected <T> T retryOperation(GuaguaZooKeeperOperation<T> operation) throws KeeperException, InterruptedException {
        KeeperException exception = null;
        for(int i = 0; i < this.getMaxRetryAttempts(); i++) {
            try {
                return operation.execute();
            } catch (KeeperException.SessionExpiredException e) {
                LOG.warn("Session expired so reconnecting due to:", e);
                throw e;
            } catch (KeeperException.ConnectionLossException e) {
                if(exception == null) {
                    exception = e;
                }
                LOG.debug("Attempt {} failed with connection loss so attempting to reconnect. Exception is: {} ", i, e);
                retryDelay(i);
            }
        }
        throw exception;
    }

    /**
     * Performs a retry delay if this is not the first attempt
     * 
     * @param attemptCount
     *            the number of the attempts performed so far
     */
    protected void retryDelay(int attemptCount) {
        if(attemptCount > 0) {
            try {
                Thread.sleep(attemptCount * getRetryWaitMsecs());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public int getMaxRetryAttempts() {
        return maxRetryAttempts;
    }

    public long getRetryWaitMsecs() {
        return retryWaitMsecs;
    }
}
