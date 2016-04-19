package hbase.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Created by jiandaohong on 2015/8/10.
 */

/*
 * hbase configuration wrapper
 */

public class HbaseConfig {
    private static final Logger logger = LogManager.getLogger(HbaseConfig.class);

    protected Configuration configuration = null;
    private String hbaseSiteFileName = null;
    private int poolSize;
    private int waitTimeMillis;
    private int healthCheckIntervalSecond;
    private String healthCheckTestTableName;

    public HbaseConfig(String hbaseSiteFileName, int poolSize, int waitTimeMillis, int healthCheckIntervalSecond,
                       String healthCheckTestTableName, Configuration configuration) {
        this.hbaseSiteFileName = hbaseSiteFileName;
        this.poolSize = poolSize;
        this.waitTimeMillis = waitTimeMillis;
        this.healthCheckIntervalSecond = healthCheckIntervalSecond;
        this.healthCheckTestTableName = healthCheckTestTableName;
        this.configuration = configuration;
    }

    public Configuration getConfiguration() { return configuration; }
    public int getPoolSize() { return poolSize; }
    public String getHbaseSiteFileName() { return this.hbaseSiteFileName; }
    public int getWaitTimeMillis() { return waitTimeMillis; }
    public int getHealthCheckIntervalSecond() { return healthCheckIntervalSecond; }
    public String getHealthCheckTestTableName() { return healthCheckTestTableName; }
}
