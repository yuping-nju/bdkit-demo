import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class HBaseTest2 {

    private static Configuration configuration;
    private static Connection connection;
    private static Admin admin;

    public static void main(String[] args) throws IOException {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();

        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder
                .newBuilder(TableName.valueOf("Students"));
        Collection<ColumnFamilyDescriptor> columnFamilies = new ArrayList<>();
        columnFamilies.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("ID")).build());
        columnFamilies.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("Description")).build());
        columnFamilies.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("Courses")).build());
        columnFamilies.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("Home")).build());
        tableDescriptorBuilder.setColumnFamilies(columnFamilies);
        admin.createTable(tableDescriptorBuilder.build());

        Table table = connection.getTable(TableName.valueOf("Students"));

        Put put = new Put("001".getBytes());
        put.addColumn("Description".getBytes(), "Name".getBytes(), "Li Lei".getBytes());
        put.addColumn("Description".getBytes(), "Height".getBytes(), "176".getBytes());
        put.addColumn("Courses".getBytes(), "Chinese".getBytes(), "80".getBytes());
        put.addColumn("Courses".getBytes(), "Math".getBytes(), "90".getBytes());
        put.addColumn("Courses".getBytes(), "Physics".getBytes(), "95".getBytes());
        put.addColumn("Home".getBytes(), "Province".getBytes(), "Zhejiang".getBytes());
        table.put(put);

        put = new Put("002".getBytes());
        put.addColumn("Description".getBytes(), "Name".getBytes(), "Han Meimei".getBytes());
        put.addColumn("Description".getBytes(), "Height".getBytes(), "183".getBytes());
        put.addColumn("Courses".getBytes(), "Chinese".getBytes(), "88".getBytes());
        put.addColumn("Courses".getBytes(), "Math".getBytes(), "77".getBytes());
        put.addColumn("Courses".getBytes(), "Physics".getBytes(), "66".getBytes());
        put.addColumn("Home".getBytes(), "Province".getBytes(), "Beijing".getBytes());
        table.put(put);

        put = new Put("003".getBytes());
        put.addColumn("Description".getBytes(), "Name".getBytes(), "Xiao Ming".getBytes());
        put.addColumn("Description".getBytes(), "Height".getBytes(), "162".getBytes());
        put.addColumn("Courses".getBytes(), "Chinese".getBytes(), "90".getBytes());
        put.addColumn("Courses".getBytes(), "Math".getBytes(), "90".getBytes());
        put.addColumn("Courses".getBytes(), "Physics".getBytes(), "90".getBytes());
        put.addColumn("Home".getBytes(), "Province".getBytes(), "Shanghai".getBytes());
        table.put(put);

        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);

        for (Result row : results) {
            for (Cell cell : row.listCells()){
                System.out.println("RowKey:" + Bytes.toString(row.getRow())
                        + " Family:" + Bytes.toString(CellUtil.cloneFamily(cell))
                        + " Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + " Value:" + Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        }

        Get get = new Get("001".getBytes());
        get.addColumn("Home".getBytes(),"Province".getBytes());
        Result result = table.get(get);
        System.out.println("001's home is in "+new String(result.getValue("Home".getBytes(),"Province".getBytes()))+" Province.");

        put = new Put("001".getBytes());
        put.addColumn("Courses".getBytes(), "English".getBytes(), "99".getBytes());
        table.put(put);

        admin.disableTable(TableName.valueOf("Students"));
        admin.addColumnFamilyAsync(TableName.valueOf("Students"),ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("Contact")).build());
        admin.enableTableAsync(TableName.valueOf("Students"));

        put = new Put("001".getBytes());
        put.addColumn("Contact".getBytes(), "Email".getBytes(), "001@hbase.com".getBytes());
        table.put(put);

        scan = new Scan();
        results = table.getScanner(scan);

        for (Result row : results) {
            for (Cell cell : row.listCells()){
                System.out.println("RowKey:" + Bytes.toString(row.getRow())
                        + " Family:" + Bytes.toString(CellUtil.cloneFamily(cell))
                        + " Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + " Value:" + Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        }

        table.close();

        admin.disableTable(TableName.valueOf("Students"));
        admin.deleteTable(TableName.valueOf("Students"));

        admin.close();
        connection.close();
    }
}