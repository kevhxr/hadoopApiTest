package com.hxr.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class HbaseDemo {

    Admin admin;
    Table hTable;
    String TN = "phone";

    @Before
    public void init() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
        hTable = connection.getTable(TableName.valueOf(TN));
    }

    /**
     * HTableDescriptor As of release 2.0.0, this will be removed in HBase 3.0.0.
     * * Use {@link TableDescriptorBuilder} to build {@link HTableDescriptor}.
     * <p>
     * TableName tname = TableName.valueOf(fullTableName);
     * TableDescriptorBuilder tableDescBuilder = TableDescriptorBuilder.newBuilder(tname);
     * ColumnFamilyDescriptorBuilder columnDescBuilder = ColumnFamilyDescriptorBuilder
     * .newBuilder(Bytes.toBytes(family)).setBlocksize(32 * 1024)
     * .setCompressionType(Compression.Algorithm.SNAPPY).setDataBlockEncoding(DataBlockEncoding.NONE);
     * tableDescBuilder.setColumnFamily(columnDescBuilder.build());
     * tableDescBuilder.build();
     **/
    @Test
    public void createTable() throws IOException {
        TableName tableName = TableName.valueOf(TN);
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            System.out.println(tableName.getName() + "disabled");
            admin.deleteTable(tableName);
        }

        TableDescriptorBuilder desc = TableDescriptorBuilder.newBuilder(tableName);
        ColumnFamilyDescriptorBuilder cf = ColumnFamilyDescriptorBuilder.newBuilder("cf".getBytes());
        desc.setColumnFamily(cf.build());
        admin.createTable(desc.build());
    }

    @Test
    public void insertDB() throws IOException, ParseException {
        List<Put> puts = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            String phonwNum = getPhoneNum("186");
            for (int j = 0; j < 10; j++) {
                String dNum = getPhoneNum("158");
                String dLength = r.nextInt(99) + "";
                String dType = r.nextInt(2) + "";
                String dateStr = getDate("2018");
                String rowKey = phonwNum + "_" + (Long.MAX_VALUE - simpleDateFormat.parse(dateStr).getTime());
                Put put = new Put(rowKey.getBytes());
                put.addColumn("cf".getBytes(), "dnum".getBytes(), dNum.getBytes());
                put.addColumn("cf".getBytes(), "length".getBytes(), dLength.getBytes());
                put.addColumn("cf".getBytes(), "type".getBytes(), dType.getBytes());
                put.addColumn("cf".getBytes(), "date".getBytes(), dateStr.getBytes());
                puts.add(put);
                System.out.println(phonwNum + "---" + dNum + "---" + dLength + "---" + dType + "---" + dateStr);
            }
        }
        hTable.put(puts);
    }

    @Test
    public void insertDBProto() throws IOException, ParseException {
        List<Put> puts = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            String phonwNum = getPhoneNum("186");
            for (int j = 0; j < 10; j++) {
                String dNum = getPhoneNum("158");
                String dLength = r.nextInt(99) + "";
                String dType = r.nextInt(2) + "";
                String dateStr = getDate("2018");
                String rowKey = phonwNum + "_" + (Long.MAX_VALUE - simpleDateFormat.parse(dateStr).getTime());

                Phone.PhoneDetail.Builder phoneDetil = Phone.PhoneDetail.newBuilder();
                phoneDetil.setDate(dateStr);
                phoneDetil.setDnum(dNum);
                phoneDetil.setLength(dLength);
                phoneDetil.setType(dType);
                Put put = new Put(rowKey.getBytes());
                put.addColumn("cf".getBytes(),
                        "phoneDetail".getBytes(),
                        phoneDetil.build().toByteArray());
                puts.add(put);

            }
        }
        hTable.put(puts);
    }

    @Test
    public void insertDBProto2() throws IOException, ParseException {
        List<Put> puts = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            String phonwNum = getPhoneNum("186");
            Phone2.dayPhoneDetail.Builder dayPhone = Phone2.dayPhoneDetail.newBuilder();
            String rowKey = phonwNum + "_" + (Long.MAX_VALUE - simpleDateFormat.parse(getDate2("20190621")).getTime());
            for (int j = 0; j < 10; j++) {
                String dNum = getPhoneNum("158");
                String dLength = r.nextInt(99) + "";
                String dType = r.nextInt(2) + "";
                String dateStr = getDate("2018");
                Phone2.PhoneDetail.Builder phoneDetil = Phone2.PhoneDetail.newBuilder();
                phoneDetil.setDate(dateStr);
                phoneDetil.setDnum(dNum);
                phoneDetil.setLength(dLength);
                phoneDetil.setType(dType);
                dayPhone.addDayPhoneDetail(phoneDetil);
            }
            Put put = new Put(rowKey.getBytes());
            put.addColumn("cf".getBytes(),
                    "day".getBytes(),
                    dayPhone.build().toByteArray());
            puts.add(put);

        }
        hTable.put(puts);
    }

    @Test
    public void scanTest() throws IOException, ParseException {
        String phoneNum = "186992049563";
        String startRow = phoneNum + "_"
                + (Long.MAX_VALUE - simpleDateFormat.parse("20180601000000").getTime());
        String endRow = phoneNum + "_"
                + (Long.MAX_VALUE - simpleDateFormat.parse("20180101000000").getTime());
        Scan scan = new Scan();
        scan.withStartRow(startRow.getBytes());
        scan.withStopRow(endRow.getBytes());
        ResultScanner resultScanner = hTable.getScanner(scan);
        for (Result rs : resultScanner) {
            System.out.print(
                    new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "dnum".getBytes())
                    )));
            System.out.print("-" +
                    new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "length".getBytes())
                    )));
            System.out.print("-" +
                    new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "type".getBytes())
                    )));
            System.out.println("-" +
                    new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "date".getBytes())
                    )));
        }
    }

    /**
     * @throws IOException    Client Result Filter
     * @throws ParseException
     */
    @Test
    public void scanTest2() throws IOException {
        String phoneNum = "186992049563";
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        PrefixFilter prefixFilter = new PrefixFilter(phoneNum.getBytes());
        SingleColumnValueFilter singleFilter = new SingleColumnValueFilter(
                "cf".getBytes(),
                "dType".getBytes(),
                CompareOperator.EQUAL, "1".getBytes());
        list.addFilter(prefixFilter);
        list.addFilter(singleFilter);
        Scan scan = new Scan();
        scan.setFilter(list);
        ResultScanner resultScanner = hTable.getScanner(scan);
        for (Result rs : resultScanner) {
            System.out.print(
                    new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "dnum".getBytes())
                    )));
            System.out.print("-" +
                    new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "dLength".getBytes())
                    )));
            System.out.print("-" +
                    new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "dType".getBytes())
                    )));
            System.out.println("-" +
                    new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "date".getBytes())
                    )));
        }
    }


    Random r = new Random();
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    private String getPhoneNum(String s) {

        return s + String.format("%08d", r.nextInt(999999999));
    }

    private String getDate(String year) {
        return year + String.format("%02d%02d%02d%02d%02d", new Object[]{r.nextInt(12) + 1,
                r.nextInt(31) + 1,
                r.nextInt(24) + 1,
                r.nextInt(60) + 1,
                r.nextInt(60) + 1
        });
    }

    private String getDate2(String year) {
        return year + String.format("%02d%02d%02d", new Object[]{
                r.nextInt(24) + 1,
                r.nextInt(60) + 1,
                r.nextInt(60) + 1
        });
    }

    @Test
    public void insertDB2() throws IOException {
        String rowKey = "q12131";
        Put put = new Put(rowKey.getBytes());
        put.addColumn("cf".getBytes(), "name".getBytes(), "kevin1".getBytes());
        put.addColumn("cf".getBytes(), "age".getBytes(), "24".getBytes());
        put.addColumn("cf".getBytes(), "sex".getBytes(), "male".getBytes());
        hTable.put(put);
    }


    @Test
    public void getTest() throws IOException {
        String rowKey = "q12131";
        Get get = new Get(rowKey.getBytes());
        //get.addColumn("cf".getBytes(),"name".getBytes());
        Result result = hTable.get(get);

/*        while(result.advance()){
            byte[] row = result.getRow();
            System.out.println(String.valueOf(row));

        }*/
        List<Cell> cell1 = result.getColumnCells("cf".getBytes(), "name".getBytes());
        List<Cell> cell2 = result.getColumnCells("cf".getBytes(), "age".getBytes());
        List<Cell> cell3 = result.getColumnCells("cf".getBytes(), "sex".getBytes());
        doPrint(cell1);
        doPrint(cell2);
        doPrint(cell3);
    }


    @Test
    public void getTestProto() throws IOException {
        String rowKey = "186761035503_9223370475737822807";
        Get get = new Get(rowKey.getBytes());
        Result result = hTable.get(get);
        Cell cell = result.getColumnLatestCell("cf".getBytes(), "day".getBytes());
        Phone2.dayPhoneDetail dayPhoneDetail = Phone2.dayPhoneDetail.parseFrom(CellUtil.cloneValue(cell));
        for (Phone2.PhoneDetail phoneDetail : dayPhoneDetail.getDayPhoneDetailList()) {
            System.out.println(phoneDetail);
        }
    }

    public void doPrint(List<Cell> cell) {
        cell.stream().forEach(
                cc -> System.out.println(new String(CellUtil.cloneValue(cc)))
        );
    }

    @After
    public void destory() throws IOException {
        if (admin != null) {
            admin.close();
        }
    }
}
