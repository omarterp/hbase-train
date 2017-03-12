import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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

public class TablePartC{

    private final String DELIMITER = ",";
    private final String ROW_ID = "id";

    // Table metadata, including column family and columns from potential flat file
    private enum TableMeta {
        POWERS("powers", new TreeMap<Integer, String>() {{
            put(0, "rowkey.id");
            put(1, "personal.hero");
            put(2, "personal.power");
            put(3, "professional.name");
            put(4, "professional.xp");
            put(5, "custom.color");
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

    public TablePartC(String ip, String port) throws IOException {
        setHbaseConf(ip, port);
        admin = new HBaseAdmin(conf);
    }

    private void setHbaseConf(String ip, String port) {
        conf.set("hbase.zookeeper.quorum", ip);
        conf.set("hbase.zookeeper.property.clientport", port);
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
    }

    /**
     *
     */
    private boolean loadFile(String filePath, TableMeta tableMeta) throws IOException {
        boolean fileLoaded;
        int rowCount = 0;

        try(RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
            String line;

            //  Initialize HTable and Rows
            HTable hTable = new HTable(conf, tableMeta.getTableName());

            /*
                Parse file - comma delimited

                    First row contains column family
                    Second row contains column header
            */
            while( (line = raf.readLine()) != null ) {
                String[] tokens = line.split(this.DELIMITER);
                // Skip first two rows as they are column family and header metadata
                if(rowCount++ < 2) {
                    continue;
                }

                // HTable Row
                Put row = null;

                /*
                    Process line
                        Get column family and column and associated column value
                            Add row to hTable
                  */
                for(int i = 0; i < tokens.length; i++) {
                    String columnFamily = this.getColumnFamily(i);
                    String column = this.getColumnName(i);

                    if(column.equals(ROW_ID)) {
                        row = new Put(tokens[i].getBytes());
                        continue;
                    }

                    row.add(columnFamily.getBytes(), column.getBytes(), tokens[i].getBytes());
                }
                hTable.put(row);
            }
            fileLoaded = true;
        }

        return fileLoaded;
    }

    private String getColumnFamily(int index) {
        return TableMeta.POWERS.getMetadata().get(index).split("\\.")[0];
    }

    private String getColumnName(int index) {
        return TableMeta.POWERS.getMetadata().get(index).split("\\.")[1];
    }


   public static void main(String[] args) throws IOException {
	TablePartC tablePartC = new TablePartC("192.168.1.2", "2181");
	tablePartC.loadFile("input.csv", TableMeta.POWERS);

	System.out.println("Inserted data");   
   }
}

