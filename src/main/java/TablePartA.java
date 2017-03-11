import java.io.IOException;
import java.util.*;

import com.sun.istack.NotNull;
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

public class TablePartA{

    // Table metadata, including column family and columns from potential flat file
    private enum TableMeta {
        POWERS("powers", new TreeMap<Integer, String>() {{
                            put(0, "rowkey.id");
                            put(1, "personal.hero");
                            put(2, "personal.power");
                            put(3, "professional.name");
                            put(4, "professional.xp");
                            put(5, "custom.name");
                            }}),
        FOOD("food", new TreeMap<Integer, String>() {{
                            put(0, "rowkey.id");
                            put(1, "nutrition.calories");
                            put(2, "nutrition.fat");
                            put(3, "nutrition.protein");
                            put(4, "nutrition.sugar");
                            put(5, "nutrition.taste");
                            }});

        private String tableName;
        private TreeMap metadata;

        TableMeta(String tableName, TreeMap metadata) {
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

    public TablePartA(String ip, String port) throws IOException {
        setHbaseConf(ip, port);
        admin = new HBaseAdmin(conf);
    }

    private void setHbaseConf(String ip, String port) {
        conf.set("hbase.zookeeper.quorum", ip);
        conf.set("hbase.zookeeper.property.clientport", port);
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
    }

    /**
     * Create tables defined by TableMeta enum; return boolean based on successful Tables creation
     * @return boolean
     */
    public boolean createTables() throws IOException {
        for(TableMeta tableMeta : TableMeta.values()) {
            String tableName = tableMeta.getTableName();
            Set<String> columnFamilySet = new HashSet<>();

            // Check if table exists, if so continue
            if(tableExists(tableName)) {
                continue;
            }

            // Add column family values to be added to Hbase Table
            for(Map.Entry<Integer, String> entry : tableMeta.getMetadata().entrySet()) {
                columnFamilySet.add(entry.getValue().split("\\.")[0]);
            }

            // Build out HTableDescriptor
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for(String columnFamily : columnFamilySet) {
                tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
            }
            admin.createTable(tableDescriptor);
            System.out.println("Created table " + tableName);
        }

        return true;
    }

    private boolean tableExists(String tableName) throws IOException {
        if(admin == null)
            throw new IllegalStateException("Please initialize.");

        return admin.tableExists(tableName.getBytes());
    }

    public static void main(String[] args) throws IOException {
        TablePartA tablePartA = new TablePartA("192.168.1.2", "2181");
        tablePartA.createTables();
    }
}

