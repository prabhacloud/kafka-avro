package eventlog;

import com.opencsv.CSVReader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Created by kkoppula on 6/27/2017.
 */
public class EventLogProducer {
    private static String inputDirectoryPath = null;
    static final String EVENT_LOG_TOPIC = "event_log";

    public static void main(String[] args) throws Exception {
//        produceInputs();
//        consumeOutput();
        //Cluster
        // inputDirectoryPath = "/home/kkoppula/kafka-streams/src/main/resources/avro/io/confluent/examples/streams/input";
//       Local
         inputDirectoryPath = "C:\\dev\\examples-3.1.x\\kafka-streams\\src\\main\\resources\\input";
        produceInputs();
    }

    private static void produceInputs() throws Exception {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "dbsrd3293:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://dbsrd3293:8081");
        final KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);

        final GenericRecordBuilder eventLogBuilder =
                new GenericRecordBuilder(loadSchema("eventlog.avsc"));

        File directory = new File(inputDirectoryPath);
        //get all the files from a directory
        File[] fList = directory.listFiles();
        for (File file : fList) {
            if (file.isFile()) {
                System.out.println(file.getName());
                //poll(file);
                try {
                    CSVReader reader = new CSVReader(new FileReader(file), '|', '\'', 0);
                    List<String[]> records = reader.readAll();
                    Iterator<String[]> iterator = records.iterator();

                    while (iterator.hasNext()) {
                        String[] record = iterator.next();
                        System.out.println(record[0].toString());
                        eventLogBuilder.set("id", record[0]);
                        eventLogBuilder.set("source_name", record[1]);
                        eventLogBuilder.set("edgenode_name", record[0]);
                        eventLogBuilder.set("pid", record[3]);
                        eventLogBuilder.set("execution_id", record[4]);
                        eventLogBuilder.set("application_name", record[5]);
                        eventLogBuilder.set("message", record[6]);
                        eventLogBuilder.set("date_created", record[7]);
                        eventLogBuilder.set("user_created", record[8]);
                        System.out.println(eventLogBuilder.build());
                        producer.send(new ProducerRecord<String, GenericRecord>(EVENT_LOG_TOPIC, null, eventLogBuilder.build()));
                    }
                    producer.flush();
                    reader.close();
                } catch (Exception ex) {
                    throw new Exception(ex);
                }
            }
        }
    }

    private static void consumeOutput() {

    }

    static Schema loadSchema(String name) throws IOException {
        try (InputStream input = EventLogProducer.class.getClassLoader()
                .getResourceAsStream("avro/" + name)) {
            return new Schema.Parser().parse(input);
        }
    }
}
