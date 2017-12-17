package s.j.liu.hbaseutils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

/**
 * @version v0.0.1
 * @since 2017-07-10 09:45:01
 * @author Shengjun Liu
 *
 */
public class HbaseUtils {
  private String zk = null;
  private Configuration conf = null;
  private Connection connection = null;
  private Admin admin = null;
  private Table table = null;

  /**
   * Construction Method.
   * 
   * @param zk
   *          zookeeper host. eg. host1:port1,host2:port2
   */
  public HbaseUtils(String zk) {
    this.zk = zk;
    this.conf = new Configuration();
    this.conf.set("hbase.zookeeper.quorum", this.zk);
  }

  /**
   * Initialization Connection.
   * 
   * @return HbaseUtils
   */
  public HbaseUtils initConnection() {
    try {
      this.connection = ConnectionFactory.createConnection(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return this;
  }

  /**
   * Initialization Admin.
   * 
   * @return HbaseUtils
   */
  public HbaseUtils initAdmin() {
    try {
      this.admin = this.connection.getAdmin();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return this;
  }

  /**
   * Initialization Table.
   * 
   * @param tableName
   *          Table name
   * @return HbaseUtils
   */
  public HbaseUtils initTable(String tableName) {
    try {
      this.table = this.connection.getTable(TableName.valueOf(tableName));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return this;
  }

  /**
   * Get Configuration.
   * 
   * @return Configuration
   */
  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * Get Connection.
   * 
   * @return Connection
   */
  public Connection getConnection() {
    return this.connection;
  }

  /**
   * Get Admin.
   * 
   * @return Admin
   */
  public Admin getAdmin() {
    return this.admin;
  }

  /**
   * Get Table.
   * 
   * @return Table
   */
  public Table getTable() {
    return this.table;
  }

  /**
   * Creates a new table. Synchronous operation.
   * 
   * @param tableName
   *          Table name
   * @param familyName
   *          Family name
   *
   * @throws IllegalArgumentException
   *           if the table name is reserved
   * @throws MasterNotRunningException
   *           if master is not running
   * @throws TableExistsException
   *           if table already exists (If concurrent threads, the table may have been created
   *           between test-for-existence and attempt-at-creation).
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public void createTable(String tableName, String familyName) throws IOException {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    desc.addFamily(new HColumnDescriptor(familyName));
    createTable(desc);
  }

  /**
   * Creates a new table. Synchronous operation.
   *
   * @param desc
   *          table descriptor for table
   *
   * @throws IllegalArgumentException
   *           if the table name is reserved
   * @throws MasterNotRunningException
   *           if master is not running
   * @throws TableExistsException
   *           if table already exists (If concurrent threads, the table may have been created
   *           between test-for-existence and attempt-at-creation).
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public void createTable(HTableDescriptor desc) throws IOException {
    admin.createTable(desc);
  }

  /**
   * Creates a new table with an initial set of empty regions defined by the specified split keys.
   * The total number of regions created will be the number of split keys plus one. Synchronous
   * operation. Note : Avoid passing empty split key.
   *
   * @param desc
   *          table descriptor for table
   * @param splitKeys
   *          array of split keys for the initial regions of the table
   *
   * @throws IllegalArgumentException
   *           if the table name is reserved, if the split keys are repeated and if the split key
   *           has empty byte array.
   * @throws MasterNotRunningException
   *           if master is not running
   * @throws org.apache.hadoop.hbase.TableExistsException
   *           if table already exists (If concurrent threads, the table may have been created
   *           between test-for-existence and attempt-at-creation).
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public void createTable(HTableDescriptor desc, byte[][] splitKeys) throws IOException {
    admin.createTable(desc, splitKeys);
  }

  /**
   * Creates a new table with the specified number of regions. The start key specified will become
   * the end key of the first region of the table, and the end key specified will become the start
   * key of the last region of the table (the first region has a null start key and the last region
   * has a null end key). BigInteger math will be used to divide the key range specified into enough
   * segments to make the required number of total regions. Synchronous operation.
   *
   * @param desc
   *          table descriptor for table
   * @param startKey
   *          beginning of key range
   * @param endKey
   *          end of key range
   * @param numRegions
   *          the total number of regions to create
   *
   * @throws IllegalArgumentException
   *           if the table name is reserved
   * @throws MasterNotRunningException
   *           if master is not running
   * @throws org.apache.hadoop.hbase.TableExistsException
   *           if table already exists (If concurrent threads, the table may have been created
   *           between test-for-existence and attempt-at-creation).
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public void createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions)
      throws IOException {
    admin.createTable(desc, startKey, endKey, numRegions);
  }

  /**
   * Creates a new table but does not block and wait for it to come online. Asynchronous operation.
   * 
   * @param desc
   *          table descriptor for table
   * @param splitKeys
   *          array of split keys for the initial regions of the table
   * @throws IllegalArgumentException
   *           Bad table name, if the split keys are repeated and if the split key has empty byte
   *           array.
   * @throws MasterNotRunningException
   *           if master is not running
   * @throws org.apache.hadoop.hbase.TableExistsException
   *           if table already exists (If concurrent threads, the table may have been created
   *           between test-for-existence and attempt-at-creation).
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public void createTableAsync(HTableDescriptor desc, byte[][] splitKeys) throws IOException {
    admin.createTableAsync(desc, splitKeys);
  }

  /**
   * 
   * @param rowkey
   *          row key
   * @param family
   *          family name
   * @param qualifier
   *          column qualifier
   * @param value
   *          column value
   * @throws IOException
   *           if a remote or network exception occurs.
   */
  public void put(String rowkey, String family, String qualifier, String value) throws IOException {
    Put put = new Put(rowkey.getBytes());
    put.addColumn(family.getBytes(), qualifier.getBytes(), value.getBytes());
    table.put(put);
  }

  /**
   * Puts some data in the table.
   * 
   * @param put
   *          The data to put.
   * @throws IOException
   *           if a remote or network exception occurs.
   */
  public void put(Put put) throws IOException {
    table.put(put);
  }

  /**
   * Puts some data in the table, in batch. This can be used for group commit, or for submitting
   * user defined batches. The writeBuffer will be periodically inspected while the List is
   * processed, so depending on the List size the writeBuffer may flush not at all, or more than
   * once.
   * 
   * @param puts
   *          The list of mutations to apply. The batch put is done by aggregating the iteration of
   *          the Puts over the write buffer at the client-side for a single RPC call.
   * @throws IOException
   *           if a remote or network exception occurs.
   */
  public void put(List<Put> puts) throws IOException {
    table.put(puts);
  }

  /**
   * Extracts certain cells from a given row.
   * 
   * @param row
   *          row key
   * @param family1
   *          family name
   * @param qualifiers1
   *          column qualifier
   * @param family2
   *          family name
   * @param qualifiers2
   *          column qualifier
   * @return Map
   * @throws IOException
   *           if a remote or network exception occurs.
   */
  public Map<String, Object> get(String row, String family1, List<String> qualifiers1,
      String family2, List<String> qualifiers2) throws IOException {
    Map<String, Object> map = new HashMap<String, Object>();
    Get get = new Get(row.getBytes());
    qualifiers1.forEach(qualifier -> {
      get.addColumn(family1.getBytes(), qualifier.getBytes());
    });
    qualifiers2.forEach(qualifier -> {
      get.addColumn(family2.getBytes(), qualifier.getBytes());
    });
    Result rs = table.get(get);
    qualifiers1.forEach(qualifier -> {
      Cell cell = rs.getColumnLatestCell(family1.getBytes(), qualifier.getBytes());
      map.put(qualifier, CellUtil.cloneValue(cell));
    });
    qualifiers2.forEach(qualifier -> {
      Cell cell = rs.getColumnLatestCell(family2.getBytes(), qualifier.getBytes());
      map.put(qualifier, CellUtil.cloneValue(cell));
    });
    return map;
  }

  /**
   * Extracts certain cells from a given row.
   * 
   * @param row
   *          row key
   * @param family
   *          family name
   * @param qualifiers
   *          column qualifier
   * @return Map
   * @throws IOException
   *           if a remote or network exception occurs.
   */
  public Map<String, Object> get(String row, String family, List<String> qualifiers)
      throws IOException {
    Map<String, Object> map = new HashMap<String, Object>();
    Get get = new Get(row.getBytes());
    qualifiers.forEach(qualifier -> {
      get.addColumn(family.getBytes(), qualifier.getBytes());
    });
    Result rs = table.get(get);
    qualifiers.forEach(qualifier -> {
      Cell cell = rs.getColumnLatestCell(family.getBytes(), qualifier.getBytes());
      map.put(qualifier, CellUtil.cloneValue(cell));
    });
    return map;
  }

  /**
   * Release Resource.
   */
  public void releaseResource() {
    if (null != table) {
      try {
        table.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    if (null != admin) {
      try {
        admin.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    if (null != connection) {
      try {
        connection.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
