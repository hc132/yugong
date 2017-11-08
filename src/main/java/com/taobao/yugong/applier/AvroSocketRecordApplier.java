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
import org.apache.avro.ipc.SaslSocketTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.generic.GenericRequestor;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
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
public class AvroSocketRecordApplier extends AbstractYuGongLifeCycle implements RecordApplier {
    protected static final Logger logger = LoggerFactory.getLogger(AvroSocketRecordApplier.class);
    private Configuration config;
    private Table table;
    private final String AVRO_MESSAGE_TRANS_PROTOCAL_FILE = "/protocal.json";
    private final String MESSAGE_TYP = "Message";
    private final String MESSAGE_NAME_SPACE = "com.taobao.yugong";
    private final String SEND_METHOD = "send";
    private final String MSG_TOPIC = "topic";
    private final String MSG_CONTENT = "content";
    private final String AVRO_HOST;
    private final int AVRO_PORT;
    private Protocol protocol;
    private Transceiver transceiver;
    private GenericRequestor requestor;
    private Schema requestSchema;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
    private Throwable exception = null;

    public AvroSocketRecordApplier(Configuration config, Table table) {
        this.config = config;
        this.table = table;
        AVRO_HOST = config.getString("yugong.target.host");
        AVRO_PORT = config.getInt("yugong.target.port");
    }

    /**
     * 将 Record 转换成 GenericRecord 以用于通过 avro rpc 方式发送
     *
     * @param r Record
     * @return GenericRecord
     */
    private GenericRecord record2GR(Record r) {
        GenericRecord msg = new GenericData.Record(requestSchema);
        try {
            ArrayList<ColumnValue> cols = Lists.newArrayList();
            cols.addAll(r.getPrimaryKeys());
            cols.addAll(r.getColumns());

            ArrayList<Schema.Field> fields = Lists.newArrayList();
            for (ColumnValue cv : cols) {
                String name = cv.getColumn().getName();
                Object value = cv.getValue();
                int type = cv.getColumn().getType();

                Schema.Field field = sqlType2AvroType(name, type, value);
                fields.add(field);
            }

            Schema schema = Schema.createRecord(MESSAGE_TYP, "", MESSAGE_NAME_SPACE, false, fields);
            GenericRecord data = new GenericData.Record(schema);

            //填充数据
            for (Schema.Field f : fields) {
                data.put(f.name(), f.defaultVal());
            }

            //编码
            ByteArrayOutputStream os = new ByteArrayOutputStream();

            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>());
            dataFileWriter.create(schema, os);
            dataFileWriter.append(data);
            dataFileWriter.close();
            //构造请求参数
            msg.put(MSG_TOPIC, r.getSchemaName() + "." + r.getTableName());
            msg.put(MSG_CONTENT, ByteBuffer.wrap(os.toByteArray()));
        } catch (IOException e) {
            throw new YuGongException("Msg convert GenericRecord exception!", e);
        }

        return msg;
    }

    private Schema.Field sqlType2AvroType(String name, int type, Object value) {
        //Avro 基本类型：INT, LONG, FLOAT, DOUBLE, STRING, BYTES, BOOLEAN, NULL;
        Schema.Type t;
        Object val = value;
        switch (type) {
            case Types.TINYINT:;
            case Types.SMALLINT:;
            case Types.INTEGER:
                t = Schema.Type.INT;
                break;
            case Types.TIMESTAMP:
                val = ((Timestamp) value).getTime();
                t = Schema.Type.LONG;
                break;
            case Types.BIGINT:
                t = Schema.Type.LONG;
                break;
            case Types.FLOAT:
                t = Schema.Type.FLOAT;
                break;
            case Types.DECIMAL:
                val = ((BigDecimal) value).doubleValue();
                t = Schema.Type.DOUBLE;
                break;
            case Types.DOUBLE:
                t = Schema.Type.DOUBLE;
                break;
            case Types.CHAR:;
            case Types.NCHAR:;
            case Types.VARCHAR:;
            case Types.NVARCHAR:;
            case Types.LONGVARCHAR:;
            case Types.LONGNVARCHAR:
                t = Schema.Type.STRING;
                break;
            case Types.DATE:
                val = dateFormat.format(new java.util.Date(((Date) value).getTime()));
                t = Schema.Type.STRING;
                break;
            case Types.TIME:
                val = timeFormat.format(new java.util.Date(((Time) value).getTime()));
                t = Schema.Type.STRING;
                break;
            case Types.BINARY:;
            case Types.VARBINARY:;
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
        try {
            //如果连接断开，则尝试重连
            if (null != exception && exception instanceof IOException) {
                logger.info("Reconnect avro server ...");
                //transceiver.close();
                transceiver = new SaslSocketTransceiver(new InetSocketAddress(AVRO_HOST, AVRO_PORT));
                requestor = new GenericRequestor(protocol, transceiver);
                //异常处理完后释放
                exception = null;
            }
        } catch (Exception e) {
            exception = e;
            throw new YuGongException("Reconnect avro server exception", e);
        }
        for (final Record record : records) {
            try {
                requestor.request(SEND_METHOD, record2GR(record));
            } catch (Exception e) {
                exception = e;
                throw new YuGongException("Send message exception!", e);
            }

        }
    }

    @Override
    public void start() {
        try {
            //加载链接信息
            transceiver = new SaslSocketTransceiver(new InetSocketAddress(AVRO_HOST, AVRO_PORT));

            //加载歇息
            protocol = Protocol.parse(this.getClass().getResourceAsStream(AVRO_MESSAGE_TRANS_PROTOCAL_FILE));

            //获取请求执行器
            requestor = new GenericRequestor(protocol, transceiver);

            //获取远程方法约束
            requestSchema = protocol.getMessages().get(SEND_METHOD).getRequest();

        } catch (IOException e) {
            throw new YuGongException("Init connect avro server exception!", e);
        }
        super.start();
    }

    @Override
    public void stop() {
        try {
            //释放链接
            transceiver.close();
            logger.info(table.getType() + "." + table.getSchema() + "." + table.getName() + " collect is over!");
        } catch (Exception e) {
            logger.error("Stop exception!", e);
        } finally {
            super.stop();
        }
    }
}
