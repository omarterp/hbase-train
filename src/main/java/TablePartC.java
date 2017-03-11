import java.io.IOException;
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

    // Table metadata, including column family and columns from potential flat file
    private enum TableMeta {
        POWERS("powers", new TreeMap<Integer, String>() {{
            put(0, "rowkey.id");
            put(1, "personal.hero");
            put(2, "personal.power");
            put(3, "professional.name");
            put(4, "professional.xp");
            put(5, "custom.name");
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


   public static void main(String[] args) throws IOException {

	//TODO      
	System.out.println("Inserted data");   
   }
}

