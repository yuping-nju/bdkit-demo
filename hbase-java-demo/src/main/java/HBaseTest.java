import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseTest {
    private static Configuration configuration;
    private static Connection connection;
    private static Admin admin;
    static {
        //1.获得Configuration实例并进行相关设置
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","localhost");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        //configuration.addResource(HBaseTest.class.getResource("hbase-site.xml"));
        //2.获得Connection实例
        try {
            connection = ConnectionFactory.createConnection(configuration);
            //3.1获得Admin接口
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        //创建表
        String  familyNames[]={"Description","Courses","Home"};
        createTable("students",familyNames);
        //向表中插入001数据
        insert("students","001","Description","Name","Li Lei");
        insert("students","001","Description","Height","176");
        insert("students","001","Courses","Chinese","80");
        insert("students","001","Courses","Math","90");
        insert("students","001","Courses","Physics","95");
        insert("students","001","Home","Province","Zhejiang");
        //向表中插入002数据
        insert("students","002","Description","Name","Han Meimei");
        insert("students","002","Description","Height","183");
        insert("students","002","Courses","Chinese","88");
        insert("students","002","Courses","Math","77");
        insert("students","002","Courses","Physics","66");
        insert("students","002","Home","Province","Beijing");
        //向表中插入003数据
        insert("students","003","Description","Name","Xiao Ming");
        insert("students","003","Description","Height","162");
        insert("students","003","Courses","Chinese","90");
        insert("students","003","Courses","Math","90");
        insert("students","003","Courses","Physics","90");
        insert("students","003","Home","Province","Shanghai");

        //删除表
//        dropTable("students");
    }
    /**
     * 创建表
     * @param tableName 表名
     * @param familyNames 列族名
     * */
    public static void createTable(String tableName, String familyNames[]) throws IOException {
        //如果表存在退出
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("Table exists!");
            return;
        }
        //通过HTableDescriptor类来描述一个表，HColumnDescriptor描述一个列族
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String familyName : familyNames) {
            tableDescriptor.addFamily(new HColumnDescriptor(familyName));
        }
        //tableDescriptor.addFamily(new HColumnDescriptor(familyName));
        admin.createTable(tableDescriptor);
        System.out.println("createtable success!");
    }

    /**
     * 删除表
     * @param tableName 表名
     * */
    public static void dropTable(String tableName) throws IOException {
        //如果表不存在报异常
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName+"不存在");
            return;
        }

        //删除之前要将表disable
        if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
        }
        admin.deleteTable(TableName.valueOf(tableName));
        System.out.println("deletetable " + tableName + "ok.");
    }

    /**
     * 指定行/列中插入数据
     * @param tableName 表名
     * @param rowKey 主键rowkey
     * @param family 列族
     * @param column 列
     * @param value 值
     * TODO: 批量PUT
     */
    public static void insert(String tableName, String rowKey, String family, String column, String value) throws IOException {
        //3.2获得Table接口,需要传入表名
        Table table =connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        System.out.println("insertrecored " + rowKey + " to table " + tableName + "ok.");
    }

    /**
     * 删除表中的指定行
     * @param tableName 表名
     * @param rowKey rowkey
     * TODO: 批量删除
     */
    public static void delete(String tableName, String rowKey) throws IOException {
        //3.2获得Table接口,需要传入表名
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
    }
}
