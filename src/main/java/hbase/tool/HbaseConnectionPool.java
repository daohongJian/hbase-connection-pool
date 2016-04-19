package hbase.tool;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by jiandaohong on 2015/8/10.
 */

/*
 * hbase connection pool for a hbase cluster
 */

public class HbaseConnectionPool {
    private static final Logger logger = LogManager.getLogger(HbaseConnectionPool.class);

    private List<HbaseConnection> busyConnection;
    private List<HbaseConnection> idleConnection;
    // 集群配置
    private HbaseConfig hbaseClusterConfig = null;
    private int unAvailableTimes = 0;
    private final Lock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();

    private HbaseConnection healthCheckConnection = null;
    private ScheduledExecutorService healthCheckExecutor = null;

    public int init(HbaseConfig hbaseConfig) {
        if (hbaseConfig == null) {
            return -1;
        }
        idleConnection = new LinkedList<HbaseConnection>();
        busyConnection = new LinkedList<HbaseConnection>();
        this.hbaseClusterConfig = hbaseConfig;

        int ret;
        HbaseConnection connection;
        for (int i = 0; i < hbaseConfig.getPoolSize(); ++i) {
            connection = new HbaseConnection();
            ret = connection.initConnection(hbaseConfig);
            if (0 != ret) {
                logger.warn("init connection failed.");
                return -1;
            }
            idleConnection.add(connection);
            logger.debug("add connection success");
        }

        // health check
        healthCheckConnection = new HbaseConnection();
        healthCheckConnection.initConnection(hbaseConfig);
        healthCheckExecutor = new ScheduledThreadPoolExecutor(1);
        healthCheckExecutor.scheduleWithFixedDelay(new HbaseHealthCheckThread(),
                hbaseConfig.getHealthCheckIntervalSecond() * 1000,
                hbaseConfig.getHealthCheckIntervalSecond() * 1000,
                TimeUnit.MILLISECONDS);
        return 0;
    }

    // TODO
    public void clearPool() {
        lock.lock();
        try {
            int size = idleConnection.size();
            for (HbaseConnection connection : idleConnection) {
                if (connection != null) {
                    connection.getConnection().close();
                }
            }
            for (HbaseConnection connection : busyConnection) {
                if (connection != null) {
                    connection.getConnection().close();
                }
            }
            busyConnection.clear();
            idleConnection.clear();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public void resetConnectionPool() {
        lock.lock();
        idleConnection = new LinkedList<HbaseConnection>();
        busyConnection = new LinkedList<HbaseConnection>();
        HbaseConnection connection;
        int ret = 0;
        for (int i = 0; i < hbaseClusterConfig.getPoolSize(); ++i) {
            connection = new HbaseConnection();
            ret = connection.initConnection(hbaseClusterConfig);
            if (0 != ret) {
                logger.warn("init connection failed.");
            }
            idleConnection.add(connection);
            logger.debug("add connection success");
        }
        lock.unlock();
    }

    public HbaseConnection getConnection() {
        lock.lock();
        try {
            while (idleConnection.isEmpty()) {
                notEmpty.await(hbaseClusterConfig.getWaitTimeMillis(), TimeUnit.MILLISECONDS);
            }
            if (idleConnection.isEmpty()) {
                logger.warn("no idle connection");
                return null;
            }

            logger.debug("get connection. before idle pool :" + idleConnection.size());
            HbaseConnection connection;
            connection = idleConnection.get(0);
            idleConnection.remove(connection);
            busyConnection.add(connection);
            logger.debug("get connection. after idle pool :" + idleConnection.size());
            logger.debug("get connection from pool success");
            return connection;
        } catch (InterruptedException e) {
            // do nothing
        } finally {
            lock.unlock();
        }
        return null;
    }

    public synchronized void releaseConnection(HbaseConnection connection) {
        lock.lock();
        logger.debug("before busy size :" + busyConnection.size() + "idle size :" + idleConnection.size());
        idleConnection.add(connection);
        busyConnection.remove(connection);
        logger.debug("after busy size :" + busyConnection.size() + "idle size :" + idleConnection.size());
        logger.debug("release connection success");
        notEmpty.signal();
        lock.unlock();
    }

    public void healthCheck() {
        logger.debug("hbase cluster health check.");
        try {
            Admin admin = healthCheckConnection.getConnection().getAdmin();
            TableName tabName = TableName.valueOf(hbaseClusterConfig.getHealthCheckTestTableName());
            admin.tableExists(tabName);
            admin.close();
            unAvailableTimes = 0;
            if (unAvailableTimes >= 10) {
                // todo
                logger.info("hbase cluster unavailable, will reset hbase connection pool.");
                clearPool();
                resetConnectionPool();
                unAvailableTimes = 0;
            }
        } catch (IOException e) {
            logger.warn("hbase health check failed. check times: " + unAvailableTimes + "exception:" + e.getMessage());
            e.printStackTrace();
            unAvailableTimes++;
        } catch (Exception e) {
            logger.warn("hbase health check exception:" + e.getMessage());
            e.printStackTrace();
        }
    }

    class HbaseHealthCheckThread implements Runnable {
        public void run() {
            healthCheck();
        }
    }
}
