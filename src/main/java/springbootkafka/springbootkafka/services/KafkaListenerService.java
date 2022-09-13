package springbootkafka.springbootkafka.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import springbootkafka.springbootkafka.models.User;

@Service
public class KafkaListenerService {


    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    @KafkaListener(topics = "topic-strings", groupId = "consumidor-springboot")
    public void listenStrings(String message) {
        logger.info("Mensaje recibido: {}", message);

    }

    // Sin especfiicar el offset, siempre se posicionara sobre el offset actual
    @KafkaListener(topics = {"topic-pruebas-consumer-1-partition"}, groupId = "consumidor-inicial")
    public void listenUsers(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) int offset
    ) {
        logger.info("Mensaje recibido [consumidor-inicial]: [{}] | particion: {} | offset: {}", message, partition, offset);
    }

    @KafkaListener(topics = {"topic-pruebas-consumer-1-partition"}, groupId = "consumidor-secundario")
    public void listenUsers2(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) int offset
    ) {
        logger.info("Mensaje recibido [consumidor-secundario]: [{}] | particion: {} | offset: {}", message, partition, offset);
    }

    // Con esta configuracion cada ves que se reinicie ira a tomar desde el principio hasta el offset actual
    @KafkaListener(
            groupId = "consumidor-tercero",
            topicPartitions = @TopicPartition(
                    topic = "topic-pruebas-consumer-1-partition",
                    partitionOffsets = {
                            @PartitionOffset(partition = "0", initialOffset = "0", relativeToCurrent = "false")
                    }
            )
    )
    public void listenUsers3(
            @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) int offset
    ) {
        logger.info("Mensaje recibido [consumidor-tercero]: [{}] | key: {} |particion: {} | offset: {}", message, key, partition, offset);
    }

    // Con esta configuracion setea una sola particion, y tomara el offset actual
    @KafkaListener(
            groupId = "consumidor-cuarto",
            topicPartitions = @TopicPartition(
                    topic = "topic-pruebas-consumer-1-partition",
                    partitionOffsets = {
                            @PartitionOffset(partition = "0", initialOffset = "0", relativeToCurrent = "true")
                    }
            )
    )
    public void listenUsers4(
            ConsumerRecord<String, String> consumerRecord
    ) {
        logger.info("Mensaje recibido [consumidor-cuarto]: [{}] | particion: {} | offset: {} | key {} | topic {}", consumerRecord.value(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), consumerRecord.topic());
    }


    // Especificamos la particion que vamos a consumir
    // ${name.topic} => topic-users-partitions
    // initialOffset = "0" => desde el inicio, se puede especificar un numero de offset
    // relativeToCurrent = "true" // si esta en true, tomara el offset actual, si esta en false, volvara desde el comienzo del offset
    @KafkaListener(
            groupId = "${spring.application.name}",
            topicPartitions = @TopicPartition(
                    topic = "${name.topic}",
                    partitionOffsets = {
                            @PartitionOffset(partition = "1", initialOffset = "0", relativeToCurrent = "true")
                    }
            )
    )
    //   Acknowledgment acknowledgment => para confirmar que se recibio el mensaje, si se debe dejar manualmente
    /*
    *
    public void listenUsersPartitions(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) int offset,
            Acknowledgment acknowledgment
    )
    * */

    public void listenUsersPartitions(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) int offset
    ) {

        logger.info("Mensaje recibido: [{}] | particion: {} | offset: {}", message, partition, offset);

        /*try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }*/

        // Configuracion para marcar el Ack manualmente
        // Marca el mensaje como leido
        // acknowledgment.acknowledge();

        ObjectMapper mapper = new ObjectMapper();
        User user = null;
        try {
            user = mapper.readValue(message, User.class);
        } catch (JsonProcessingException e) {
            logger.error("No se pudo hacer la conversion");
        }

        logger.info("Usuario recibido: {}", message);
        logger.info("Usuario mapeado: " + user);

    }
}
