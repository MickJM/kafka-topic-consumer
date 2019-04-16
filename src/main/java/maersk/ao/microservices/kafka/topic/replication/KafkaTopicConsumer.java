package maersk.ao.microservices.kafka.topic.replication;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import maersk.ao.microservices.kafka.topic.configurations.KafkaConsumerConfigurations;

@Component
@EnableKafka
public class KafkaTopicConsumer {
	//implements ConsumerSeekAware {

	//ConsumerAwareRebalanceListener
	//ConsumerSeekAware {

    static Logger log = Logger.getLogger(KafkaTopicConsumer.class);

    @Value("${kafka.debug:false}")
    private boolean _debug;

    private KafkaTemplate<String, String> kafkaProducerTemplate;
    
	private KafkaConsumerConfigurations kafkaConsumerConfig;
	//private KafkaProducerConfigurations kafkaProducerConfig;

	
    //containerFactory="kafkaListenerContainerFactory"
	// 			groupId = "${kafka.src.consumer.group}"
	@KafkaListener( 
			topics = "${kafka.src.topic}")
	public void listen(ConsumerRecord<?,?> consumerRecord, KafkaConsumer consumer, Acknowledgment ack) throws InterruptedException, ExecutionException {

		if (this._debug) { log.info("----- ACK MODE ----- "); }

		if (this._debug) {
			log.info("AckMode: received message on " 
					+ consumerRecord.topic() 
					+ "- key   :" + consumerRecord.key()
					+ "- offset: " + consumerRecord.offset()
					+ "- partition: " + consumerRecord.partition()
					+ "- value : " + consumerRecord.value());
		}
		
		if (ack != null) {
			ack.acknowledge();			
		} else {
			log.warn("----- ACK is null -----");
		}
		//consumer.commitAsync();
	}
	
	
	/*
	@KafkaListener(topics = "${kafka.src.topic}", containerFactory="kafkaListenerContainerFactory")
	public void listen(ConsumerRecord<?,?> consumerRecord, KafkaConsumer consumer) throws InterruptedException, ExecutionException {
        
		System.out.println("**** NO ACK MODE **** "); 

		System.out.println("**** received message on " 
        					+ consumerRecord.topic() 
        					+ "- key   :" + consumerRecord.key()
        					+ "- offset: " + consumerRecord.offset()
        					+ "- partition: " + consumerRecord.partition()
        					+ "- value : " + consumerRecord.value());

        consumer.commitAsync();
       
    }
	*/
	
	/*
	@Override
	public void registerSeekCallback(ConsumerSeekCallback callback) {
		// TODO Auto-generated method stub
		System.out.println("KafkaTopicConsumer:  registerSeekCallback"); 
		
		
	}
	*/
	
	/*
	@Override
	public void onPartitionsAssigned(Map<org.apache.kafka.common.TopicPartition, Long> assignments,
			ConsumerSeekCallback callback) {
	//public void onPartitionsAssigned(Consumer<?,?> consumer, Collection<org.apache.kafka.common.TopicPartition> assignments) 
	//{
	
		
		// TODO Auto-generated method stub
		////System.out.println("KafkaTopicConsumer:  onPartitionsAssigned"); 
		
		//long rewindTo = System.currentTimeMillis() - 60000;
        //Map<org.apache.kafka.common.TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(assignments.stream()
         //       .collect(Collectors.toMap(tp -> tp, tp -> rewindTo)));
        
       // offsetsForTimes.forEach((k, v) -> consumer.seek(k, v.offset()));

		assignments.forEach((t, o) 
				-> System.out.println("Topic: " + t.topic() + " partition: " + t.partition() + " offset: " + o.longValue()));
				
		////assignments.forEach((t, o) 
		////		-> callback.seekToEnd(t.topic(), t.partition()));

		////System.out.println("KafkaTopicConsumer:  seeToEnd complete"); 

	}
	*/
	
	
	/*
	@Override
	public void onIdleContainer(Map<org.apache.kafka.common.TopicPartition, Long> assignments,
			ConsumerSeekCallback callback) {
		// TODO Auto-generated method stub
		System.out.println("KafkaTopicConsumer:  onIdleContainer");
	}
	*/
	
	/*
    @KafkaListener(topics = {"${kafka.src.topic}"}, containerFactory = "kafkaListenerContainerFactory")
    public void replicateMessage(@Payload String message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY)
                                 Acknowledgment acknowledgment) throws ExecutionException, InterruptedException  {


            ProducerRecord<String, String> record =  new ProducerRecord<>(kafkaConfig.getDestTopic(), message);

            RecordMetadata recordMetadata = kafkaTemplate.send(record).get().getRecordMetadata();

            log.debug("Message key : {} - Published to Kafka topic {} on partition {} at offset {}. result message: {}",
                     kafkaConfig.getDestTopic(), recordMetadata.partition(), recordMetadata.offset(), message);

            acknowledgment.acknowledge();
       }
		*/
	
}
