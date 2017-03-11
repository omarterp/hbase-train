import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

/**
 * Created by omart on 3/5/2017.
 */
public class HbaseTest {

    private final Configuration conf = HBaseConfiguration.create();

    private final String DELIMITER = ",";

    private final String TABLE_NAME = "powers";
    private final Map<Integer, String> columnFamilyMeta;

    private final List<Map<Integer, String>> tableMeta = new ArrayList<>();

    private HbaseTest () throws IOException {
        // Table Metadata
        columnFamilyMeta = new TreeMap<>();

        columnFamilyMeta.put(0, "rowkey.id");
        columnFamilyMeta.put(1, "personal.hero");
        columnFamilyMeta.put(2, "personal.power");
        columnFamilyMeta.put(3, "professional.name");
        columnFamilyMeta.put(4, "professional.xp");
        columnFamilyMeta.put(5, "custom.name");

        setHbaseConf("192.168.1.2", "2181");

        HBaseAdmin admin = new HBaseAdmin(conf);

        HTableDescriptor[] tables = admin.listTables();
        for(HTableDescriptor table : tables) {
            System.out.println(table.getNameAsString());
        }
//        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("emp"));
//        tableDescriptor.addFamily(new HColumnDescriptor("personal"));
//        admin.createTable(tableDescriptor);
    }

    private void setHbaseConf(String ip, String port) {
        conf.set("hbase.zookeeper.quorum", ip);
        conf.set("hbase.zookeeper.property.clientport", port);
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
    }

    private void setTableMeta() {
        // powers table

        //
    }

    public static void main(String[] args) throws IOException {
        int rowCount = 0;

        HbaseTest hbt = new HbaseTest();

        try(RandomAccessFile raf = new RandomAccessFile("input.csv", "r")) {
            String line;

            /*
                Parse file - comma delimited

                    First row contains column family
                    Second row contains column header
            */
            while( (line = raf.readLine()) != null ) {
                String[] tokens = line.split(hbt.DELIMITER);
                // Skip first two rows as they are column family and header metadata
                if(rowCount++ < 2) {
                    continue;
                }

                for(int i = 0; i < tokens.length; i++) {
                    String columnFamily = hbt.getColumnFamily(i);
                    String column = hbt.getColumnName(i);

//                    System.out.println("columnFamily->" + columnFamily);
//                    System.out.println("column->" + column);
                }

//                System.out.println(line);
            }
        }
    }

    private String getColumnFamily(int index) {
        return columnFamilyMeta.get(index).split("\\.")[0];
    }

    private String getColumnName(int index) {
        return columnFamilyMeta.get(index).split("\\.")[1];
    }
}