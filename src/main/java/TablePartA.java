import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;


public class TablePartA{

    // Table metadata, including column family and columns from potential flat file
    private enum TableMeta {
        POWERS("powers", new TreeMap<Integer, String>() {{
                            put(0, "personal.hero");
                            put(1, "personal.power");
                            put(2, "professional.name");
                            put(3, "professional.xp");
                            put(4, "custom.color");
                            }}),
        FOOD("food", new TreeMap<Integer, String>() {{
                            put(0, "nutrition.calories");
                            put(1, "nutrition.fat");
                            put(2, "nutrition.protein");
                            put(3, "nutrition.sugar");
                            put(4, "nutrition.taste");
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
    private boolean createTables() throws IOException {
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

            // Build out HTableDescriptor with defined column families
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
//        TablePartA tablePartA = new TablePartA("127.0.0.1", "2181");
        tablePartA.createTables();
    }
}
