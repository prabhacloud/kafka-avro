package eventlogconsumer;

import org.apache.avro.generic.IndexedRecord;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.kafkastream;
import kafka.javaapi.consumer.ConsumerConnector;
import io.confluent.kafka.serializers.kafkaAvroDecoder;
import kafka.message.MessageAndMetadata;
import kafka.utils.VerifiableProperties;

import org.apache.kafka.common.errors.SerializationException;

import java.util.*;

public class Eventlogconslist {

public static void main(String args[]){
	
Properties props = new Properties();
//props.put("zookeeper.connect","localhoast:");
props.put("group.id","group1");
props.put("schema.registry.url","");

String topic = "topic1";
Map<String,Integer> topicCountMap = new HashMap<>();
topicCountMap.put(topic,new Integer(1));

verifiableProperties vprops = new VerifiableProperties(props);
//kafkaAvroDecoder keyDecoder = new kafkaAvroDecoder(vProps);
kafkaAvroDecoder valueDecoder = new kafkaAvroDecoder(vProps);

consumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

Map<String,List<KafkaStream<Object,Object>>> consumerMap = consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);

kafkastream stream = consumerMap.get(topic).get(0);
consumerIterator it = stream.iterator();
while(it.hasNext()){
	MessageAndMetadata messageAndMetadata = it.next();
	try{
		String key = (String) messageAndMetadata.key();
		IndexedRecord value = (IndexedRecord) messageAndMetadata.message();
		
	}catch(SerializationException e){
		//
	}
}
	}
}
