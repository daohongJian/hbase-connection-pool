package hbase.tool;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by jiandaohong on 2015/8/10.
 */

public class HbaseConnection {
    private static final Logger logger = LogManager.getLogger(HbaseConnection.class);

    private Connection connection = null;
    private HbaseConfig config = null;

    public HbaseConnection() { }

    public synchronized int initConnection(HbaseConfig config) {
        if (null == config) {
            logger.error("config is null. cannot connect to hbase");
            return -1;
        }
        this.config = config;
        try {
            connection = ConnectionFactory.createConnection(config.getConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("create connection failed. config file :" + config.getHbaseSiteFileName());
            return -1;
        }
        return 0;
    }

    public synchronized void releaseConnection() throws IOException {
        if (null != connection) {
            connection.close();
        }
        connection = null;
    }

    public synchronized Connection getConnection() {
        if (connection == null) {
            reconnect();
        }
        return connection;
    }

    public synchronized void reconnect() {
        try {
            if (null != connection) {
                connection.close();
            }
            connection = ConnectionFactory.createConnection(this.config.getConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
