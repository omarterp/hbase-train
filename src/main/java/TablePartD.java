import java.io.IOException;
import java.util.*;

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

    public List<byte[]> getHTableRow(String tableName, Map.Entry<String, Set<String>> entry) throws IOException {
        List<byte[]> hTableRow = new ArrayList<>();

        HTable hTable = new HTable(conf, tableName);



        return hTableRow;
    }

    public static void main(String[] args) throws IOException {



    }
}

