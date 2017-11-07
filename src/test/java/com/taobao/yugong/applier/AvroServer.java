package com.taobao.yugong.applier;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.SaslSocketServer;
import org.apache.avro.ipc.generic.GenericResponder;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by weicm on 2017/11/6.
 */
public class AvroServer extends GenericResponder {
    public AvroServer(Protocol local) {
        super(local);
    }

    @Override
    public Object respond(Protocol.Message message, Object request) throws Exception {
        GenericRecord req = (GenericRecord) request;
        Utf8 topic = (Utf8) req.get("topic");
        ByteBuffer content = (ByteBuffer) req.get("content");
        DataFileStream<GenericRecord> dataFileReader = new DataFileStream<GenericRecord>(new ByteArrayInputStream(content.array()), new GenericDatumReader<GenericRecord>());

        GenericRecord record = null;
        while (dataFileReader.hasNext()) {
            record = dataFileReader.next(record);
            System.out.println(">>>>>>>>>>>>>>>>>>>>>> "+record);
        }
        /*List<Schema.Field> fields = req.getSchema().getFields().get(0).schema().getFields();
        for (Schema.Field field: fields) {
            System.out.println(field.name() + " : " + field.schema().getType());
        }*/
        return "OK";
    }


    public void run() {
        try {
            SaslSocketServer server = new SaslSocketServer(this, new InetSocketAddress("localhost", 8888));
            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws Exception {
        new AvroServer(Protocol.parse(AvroServer.class.getResourceAsStream("/protocal.json"))).run();
    }
}
