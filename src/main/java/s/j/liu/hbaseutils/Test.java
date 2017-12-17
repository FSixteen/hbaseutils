package s.j.liu.hbaseutils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

public class Test {

  public static void main(String[] args) throws IOException {
    HbaseUtils hbase = new HbaseUtils("standby:2181");
    hbase.initConnection().initAdmin().initTable("table");
    Table table = hbase.getTable();
    
    
    hbase.releaseResource();
  }

  public static void main2(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "host:2181");
    Connection con = ConnectionFactory.createConnection(conf);
    Table table = con.getTable(TableName.valueOf("tablename".getBytes()));
    // ----------------
    Get get = new Get("id".getBytes());
    Result rs = table.get(get);
    
    
    Cell[] rawCells = rs.rawCells();
    for (Cell cell : rawCells) {
    }

    Cell cell = rs.getColumnLatestCell("info".getBytes(), "A034".getBytes());
    System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
    cell = rs.getColumnLatestCell("info".getBytes(), "A036".getBytes());
    System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
    cell = rs.getColumnLatestCell("info".getBytes(), "A039".getBytes());
    System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
    // ----------------

    List<Get> list = new ArrayList<Get>();
    list.add(new Get("id".getBytes()));
    list.add(new Get("id".getBytes()));
    Result[] rss = table.get(list);
    for (int i = 0; i < rss.length; i++) {
      Result r = rss[i];
      Bytes.toShort(r.getRow());
    }
  }

  public static void main1(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "host:2181");
    Connection con = ConnectionFactory.createConnection(conf);
    Table table = con.getTable(TableName.valueOf("tablename".getBytes()));
    // ----------------
    Get get = new Get("id".getBytes());
    Result rs = table.get(get);

    Cell cell = rs.getColumnLatestCell("info".getBytes(), "A034".getBytes());
    System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
    cell = rs.getColumnLatestCell("info".getBytes(), "A036".getBytes());
    System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
    cell = rs.getColumnLatestCell("info".getBytes(), "A039".getBytes());
    System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
    // ----------------

    List<Get> list = new ArrayList<Get>();
    list.add(new Get("id".getBytes()));
    list.add(new Get("id".getBytes()));
    Result[] rss = table.get(list);
    for (int i = 0; i < rss.length; i++) {
      Result r = rss[i];
      Bytes.toShort(r.getRow());
    }
  }
}
