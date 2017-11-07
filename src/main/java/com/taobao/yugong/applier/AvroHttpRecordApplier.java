package com.taobao.yugong.applier;

import com.google.common.collect.Lists;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.db.meta.Table;
import com.taobao.yugong.common.lifecycle.AbstractYuGongLifeCycle;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.exception.YuGongException;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.ipc.Callback;
import org.apache.avro.ipc.SaslSocketTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.generic.GenericRequestor;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.avro.util.Utf8;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ByteBuffered;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by weicm on 2017/11/6.
 */
public class AvroHttpRecordApplier extends AbstractYuGongLifeCycle implements RecordApplier {
    protected static final Logger logger   = LoggerFactory.getLogger(AvroHttpRecordApplier.class);
    private Configuration config;
    private Table table;
    private final String MESSAGE_TYP = "Message";
    private final String SEND_METHOD = "send";
    private final String MSG_TOPIC = "topic";
    private final String MSG_CONTENT = "content";
    private Protocol protocol;
    private Transceiver transceiver;
    private GenericRequestor requestor;
    private Schema requestSchema;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");

    public AvroHttpRecordApplier(Configuration config, Table table) {
        this.config = config;
        this.table = table;
    }

    /**
     * 将 Record 转换成 GenericRecord 以用于通过 avro rpc 方式发送
     * @param r Record
     * @return GenericRecord
     */
    private GenericRecord record2GR(Record r) {
        GenericRecord msg = new GenericData.Record(requestSchema);
        try {
            List<ColumnValue> primaryKeys = r.getPrimaryKeys();
            List<ColumnValue> columns = r.getColumns();
            primaryKeys.addAll(columns);

            ArrayList<Schema.Field> fields = Lists.newArrayList();
            for (ColumnValue cv: primaryKeys) {
                String name = cv.getColumn().getName();
                Object value = cv.getValue();
                int type = cv.getColumn().getType();

                Schema.Field field = sqlType2AvroType(name, type, value);
                fields.add(field);
            }

            Schema schema = Schema.createRecord("Message", "", "com.taobao.yugong", false, fields);
            GenericRecord data = new GenericData.Record(schema);

            //填充数据
            for (ColumnValue cv: primaryKeys) {
                data.put(cv.getColumn().getName(), cv.getValue());
            }

            //编码
            ByteArrayOutputStream os = new ByteArrayOutputStream();

            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>());
            dataFileWriter.create(schema, os);
            dataFileWriter.append(data);
            dataFileWriter.close();
            //构造请求参数
            msg.put(MSG_TOPIC, r.getTableName());
            msg.put(MSG_CONTENT, ByteBuffer.wrap(os.toByteArray()));
        } catch (IOException e) {
            throw new YuGongException("Msg serialize exception!", e);
        }

        return msg;
    }

    private Schema.Field sqlType2AvroType(String name, int type, Object value) {
        //Avro 基本类型：INT, LONG, FLOAT, DOUBLE, STRING, BYTES, BOOLEAN, NULL;
        Schema.Type t;
        Object val = value;
        switch (type) {
            case Types.TINYINT: ;
            case Types.SMALLINT: ;
            case Types.INTEGER:
                t = Schema.Type.INT;
                break;
            case Types.TIMESTAMP:
                val = ((Timestamp)value).getTime();
                t = Schema.Type.LONG;
                break;
            case Types.BIGINT:
                t = Schema.Type.LONG;
                break;
            case Types.FLOAT:
                t = Schema.Type.FLOAT;
                break;
            case Types.DECIMAL: ;
            case Types.DOUBLE:
                t = Schema.Type.DOUBLE;
                break;
            case Types.CHAR: ;
            case Types.NCHAR: ;
            case Types.VARCHAR: ;
            case Types.NVARCHAR: ;
            case Types.LONGVARCHAR: ;
            case Types.LONGNVARCHAR:
                t = Schema.Type.STRING;
                break;
            case Types.DATE:
                val = dateFormat.format(new java.util.Date(((Date)value).getTime()));
                t = Schema.Type.STRING;
                break;
            case Types.TIME:
                val = timeFormat.format(new java.util.Date(((Time)value).getTime()));;
                t = Schema.Type.STRING;
                break;
            case Types.BINARY: ;
            case Types.VARBINARY: ;
            case Types.LONGVARBINARY:
                t = Schema.Type.BYTES;
                break;
            case Types.BOOLEAN:
                t = Schema.Type.BOOLEAN;
                break;
            default:
                val = null;
                t = Schema.Type.NULL;
        }
        return new Schema.Field(name, Schema.create(t), "", val);
    }
    @Override
    public void apply(List<Record> records) throws YuGongException {
        for (final Record record: records) {
            try {
                requestor.request(SEND_METHOD, record2GR(record), new Callback<Utf8>() {
                    @Override
                    public void handleResult(Utf8 s) {
                        System.out.println("消息发送成功：" + record);
                        System.out.println(s);
                    }

                    @Override
                    public void handleError(Throwable throwable) {
                        throw new YuGongException("Hadle message exception!", throwable);
                    }
                });
            } catch (IOException e) {
                logger.error("Send message exception!" + record, e);
            }

        }
    }

    @Override
    public void start() {
        try {
            //加载链接信息
            String host = config.getString("yugong.target.host");
            Integer port = config.getInt("yugong.target.port");
            transceiver = new SaslSocketTransceiver(new InetSocketAddress(host, port));

            //加载歇息
            protocol = Protocol.parse(this.getClass().getResourceAsStream("/protocal.json"));

            //获取请求执行器
            requestor = new GenericRequestor(protocol, transceiver);

            //获取请求约束
            requestSchema = protocol.getMessages().get(SEND_METHOD).getRequest();

        } catch (IOException e) {
            throw new YuGongException("Load protocol exception!", e);
        }
        super.start();
    }

    @Override
    public void stop() {
        try {
            //释放链接
            transceiver.close();

            logger.info(table.getType()+"."+table.getSchema()+"."+table.getName() + " collect is over!");
        } catch (IOException e) {
            e.printStackTrace();
        }
        super.stop();
    }

    public Table getTable() {
        return table;
    }
}
