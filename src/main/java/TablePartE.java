import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;

public class TablePartE{

   // Table metadata, including column family and columns from potential flat file
   private enum TableMeta {
      POWERS("powers", new TreeMap<Integer, String>() {{
         put(0, "personal.hero");
         put(1, "personal.power");
         put(2, "professional.name");
         put(3, "professional.xp");
         put(4, "custom.color");
      }});

      private String tableName;
      private TreeMap<Integer, String> metadata;

      TableMeta(String tableName, TreeMap<Integer, String> metadata) {
         this.tableName = tableName;
         this.metadata = metadata;
      }

      public static TableMeta getTableMeta(String tableMetaName) {
         if(tableMetaName == null)
            return null;

         TableMeta tableMeta = null;
         for(TableMeta tMeta : TableMeta.values()) {
            if(tMeta.toString().equalsIgnoreCase(tableMetaName))
               tableMeta = tMeta;
         }

         return tableMeta;
      }

      public String getTableName() {
         return tableName;
      }

      public TreeMap<Integer, String> getMetadata() {
         return metadata;
      }
   }

   private final Configuration conf = HBaseConfiguration.create();
   private HBaseAdmin admin;

   public TablePartE(String ip, String port) throws IOException {
      setHbaseConf(ip, port);
      admin = new HBaseAdmin(conf);
   }

   private void setHbaseConf(String ip, String port) {
      conf.set("hbase.zookeeper.quorum", ip);
      conf.set("hbase.zookeeper.property.clientport", port);
      conf.set("zookeeper.znode.parent", "/hbase-unsecure");
   }

   /**
    * Uses given Table Metadata to build a Scan object for a table and returns a ResultScanner
    * @param tableMeta
    * @return ResultScanner
    * @throws IOException
    */
   public ResultScanner getTableResultScanner(TableMeta tableMeta) throws IOException {
      Scan scan = new Scan();
      HTable table = new HTable(conf, tableMeta.getTableName());

      // Add column family and column pairs to Scan object
      for(Map.Entry<Integer, String> entry : tableMeta.getMetadata().entrySet()) {
         String columnFamily = getColumnFamily(entry.getValue());
         String column = getColumnName(entry.getValue());

         scan.addColumn(columnFamily.getBytes(), column.getBytes());
      }

      return table.getScanner(scan);
   }

   private String getColumnFamily(String columnMeta) {
      return columnMeta.split("\\.")[0];
   }

   private String getColumnName(String columnMeta) {
      return columnMeta.split("\\.")[1];
   }

   // Print ResultScanner content for powers table
   public static void main(String[] args) throws IOException {

      TablePartE tablePartE = new TablePartE("192.168.1.2", "2181");

      try(ResultScanner rScanner = tablePartE.getTableResultScanner(TableMeta.POWERS)) {
         for(Result result : rScanner) {
            System.out.println("Found row: " + result);
         }
      }
   }
}

