package example;

import hbase.tool.HbaseAdapter;

/**
 * Created by jiandaohong on 2015/11/27.
 */

public class HbaseExample {
    public static void main(String[] args) {
        // init hbase connection pool
        HbaseAdapter hbaseAdapter = HbaseAdapter.getInstance();
        hbaseAdapter.init("./conf/hbase-conf.properties");

        // use hbase Adapter function
        hbaseAdapter.isExist("table");

        // or you can wrap a HbaseProxy class to use HbaseAdapter
    }
}
