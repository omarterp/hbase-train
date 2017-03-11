import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;


public class TablePartB{

    private final Configuration conf = HBaseConfiguration.create();
    private HBaseAdmin admin;

    public TablePartB(String ip, String port) throws IOException {
        setHbaseConf(ip, port);
        admin = new HBaseAdmin(conf);
    }

    private void setHbaseConf(String ip, String port) {
        conf.set("hbase.zookeeper.quorum", ip);
        conf.set("hbase.zookeeper.property.clientport", port);
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
    }

    private void listTables() throws IOException {
        HTableDescriptor[] tables = admin.listTables();

        for(HTableDescriptor tableDescriptor : tables) {
            System.out.println(tableDescriptor.getNameAsString());
        }
    }

   public static void main(String[] args) throws IOException {
    TablePartB tablePartB = new TablePartB("localhost", "2181");
	
	System.out.println("Found tables:");
	tablePartB.listTables();
   }
}