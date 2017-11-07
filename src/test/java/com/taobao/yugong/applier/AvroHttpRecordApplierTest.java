package com.taobao.yugong.applier;

import com.google.common.collect.Lists;
import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.db.meta.Table;
import com.taobao.yugong.common.model.record.Record;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.sql.Types;
import java.util.ArrayList;

/**
 * Created by weicm on 2017/11/6.
 */
public class AvroHttpRecordApplierTest {
    static Record getRecord(Table table, int id) {
        ArrayList<ColumnValue> columns = new ArrayList<ColumnValue>();
        columns.add(new ColumnValue(new ColumnMeta("id", Types.INTEGER), id));
        columns.add(new ColumnValue(new ColumnMeta("name", Types.LONGNVARCHAR), "jack" + id));
        columns.add(new ColumnValue(new ColumnMeta("city", Types.LONGNVARCHAR), "北京" + id));
        Record record = new Record(table.getSchema(), table.getName(), Lists.<ColumnValue>newArrayList(), columns);
        return record;
    }
    public static void main(String[] args) {
        Configuration config = new BaseConfiguration();
        config.addProperty("yugong.target.host", "localhost");
        config.addProperty("yugong.target.port", "8888");
        Table table = new Table("MYSQL", "test", "mytable");
        AvroHttpRecordApplier client = new AvroHttpRecordApplier(config, table);
        client.start();
        client.apply(Lists.newArrayList(getRecord(client.getTable(), 1)));
        client.apply(Lists.newArrayList(getRecord(client.getTable(), 2)));
        client.apply(Lists.newArrayList(getRecord(client.getTable(), 3)));
    }
}
