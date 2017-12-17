package s.j.liu.test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Pair;

public class HTablePoolTest {

  public static void main(String[] args) {
    long s = System.currentTimeMillis();
    for (long i = 0; i < 999999999L; i++) {
      long j = System.currentTimeMillis();
    }
    System.out.println(System.currentTimeMillis() - s);
  }

  public static void main1(String[] args) throws IOException {
    String zk = "standby:2181";
    Configuration conf = null;
    Connection connection = null;
    conf = new Configuration();
    conf.set("hbase.zookeeper.quorum", zk);
    connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();

    // admin.createTable(desc, startKey, endKey, numRegions);

    Table table = connection.getTable(TableName.valueOf("table"));

    table.setWriteBufferSize(10000);
    
    Put puts = null;
    table.put(puts );

    System.out.println(System.currentTimeMillis());

    Put put = null;
    put.setDurability(Durability.SKIP_WAL);

    admin.flush(TableName.valueOf("table"));

    HTable htable = null;
    htable.flushCommits();

    HTableDescriptor td = admin.getTableDescriptor(TableName.valueOf("table"));

    List<HRegionInfo> tr = admin.getTableRegions(TableName.valueOf("table"));
    for (HRegionInfo r : tr) {

    }

    RegionLocator rl = connection.getRegionLocator(TableName.valueOf("table"));
    Pair<byte[][], byte[][]> pair = rl.getStartEndKeys();

    connection.getAdmin().getClusterStatus();
    connection.getAdmin().getClusterStatus().getServers();
    ServerName sn = null;
    connection.getAdmin().getClusterStatus().getLoad(sn);

    HTable ht = null;
    ht.getStartEndKeys();

  }
}
