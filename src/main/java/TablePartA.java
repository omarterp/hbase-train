import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

public class TablePartA{

    //
    private enum TableMeta {
        POWERS("powers", new TreeMap<Integer, String>() {{
                            put(1, "test.test");
                            }}),
        FOOD("food", new TreeMap<Integer, String>() {{
                            put(1, "test.test");
                            }});

        private String tableName;
        private TreeMap metadata;

        TableMeta(String tableName, TreeMap metadata) {
            this.tableName = tableName;
            this.metadata = metadata;
        }

        public String getTableName() {
            return tableName;
        }

        public TreeMap getMetadata() {
            return metadata;
        }
    }

    private final Configuration conf = HBaseConfiguration.create();
    private HBaseAdmin admin;

    private final List<Map<String, Object>> tableMeta = new ArrayList<>();

    private void setHbaseConf(String ip, String port) {
        conf.set("hbase.zookeeper.quorum", ip);
        conf.set("hbase.zookeeper.property.clientport", port);
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
    }

    public TablePartA(String ip, String port) throws IOException {
        setHbaseConf(ip, port);
        admin = new HBaseAdmin(conf);
    }

    public Configuration getConf() {
        return conf;
    }

    public HBaseAdmin getAdmin() {
        return admin;
    }

    public List<Map<String, Object>> getTableMeta() {
        return tableMeta;
    }



    public static void main(String[] args) throws IOException {

        System.out.println(TableMeta.POWERS.getTableName());
        System.out.println(TableMeta.POWERS.getMetadata());

        System.out.println(TableMeta.FOOD.getTableName());
        System.out.println(TableMeta.FOOD.getMetadata());

    //TODO
    System.out.println("Created table powers");

    //TODO
    System.out.println("Created table food");
    }
}

