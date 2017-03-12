import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.*;

import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.util.Bytes;

public class TablePartD{

    private final Configuration conf = HBaseConfiguration.create();
    private HBaseAdmin admin;

    public TablePartD(String ip, String port) throws IOException {
        setHbaseConf(ip, port);
        admin = new HBaseAdmin(conf);
    }

    private void setHbaseConf(String ip, String port) {
        conf.set("hbase.zookeeper.quorum", ip);
        conf.set("hbase.zookeeper.property.clientport", port);
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
    }

    // Return HBase Client Result
    public Result getHTableRow(String tableName, Map.Entry<String, String[]> entry) throws IOException {

        HTable hTable = new HTable(conf, tableName);

       // Initialize Get object with row name
       Get get = new Get(entry.getKey().split("-")[0].getBytes());

       // Add column family and columns to Get object
       for(String columnMeta : entry.getValue()) {
          get.addColumn(getColumnFamily(columnMeta).getBytes(),
                  getColumnName(columnMeta).getBytes());
       }
        return hTable.get(get);
    }

   public String getColumnFamily(String columnMeta) {
      return columnMeta.split("\\.")[0];
   }

   public String getColumnName(String columnMeta) {
      return columnMeta.split("\\.")[1];
   }

    // Fetch particular rows and columns
    public static void main(String[] args) throws IOException {

       final String POWERS_TABLE_NAME = "powers";

       TablePartD tablePartD = new TablePartD("192.168.1.2", "2181");

       // Define data struct to facilitate fetching rows
       Map<String, String[]> rowFetchMeta = new HashMap<>();

       // row identifier is key and value is set of column definitions - $(column_family.column)
       rowFetchMeta.put("row1-req1", new String[] {"personal.hero",
                       "personal.power",
                       "professional.name",
                       "professional.xp",
                       "custom.color"
               });

       rowFetchMeta.put("row4-req2", new String[] {"personal.hero",
                       "custom.color"
               });

       rowFetchMeta.put("row12-req3", new String[] {"personal.power",
                       "professional.name",
                       "custom.color"
               });

       rowFetchMeta.put("row1-req4", new String[] {"personal.hero",
                       "professional.name",
                       "custom.color"
               });

       rowFetchMeta.put("row8-req5", new String[] {"personal.hero",
                       "personal.power"
               });

       rowFetchMeta.put("row19-req6", new String[] {"professional.name",
                       "custom.color"
               });


       for(Map.Entry<String, String[]> entry : rowFetchMeta.entrySet()){
          Result result = tablePartD.getHTableRow(POWERS_TABLE_NAME, entry);

          String[] colFamilyMeta = entry.getValue();
          System.out.println(entry.getKey());
          StringBuffer sb = new StringBuffer();

          // Get Values
          for(String columnMeta : colFamilyMeta) {
             String columnFamily = tablePartD.getColumnFamily(columnMeta);
             String qualifier = tablePartD.getColumnName(columnMeta);
             String value = new String( result.getValue(columnFamily.getBytes(), qualifier.getBytes()));

             sb.append(qualifier)
                     .append(':')
                     .append(value)
                     .append(',');
          }

          // Remove last comma since we are not able to use Java 8 String Join
          sb.deleteCharAt(sb.length() - 1);
          // Output result in qualifier:value form
          System.out.println(sb);
       }
    }
}

