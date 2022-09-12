package springbootkafka.springbootkafka.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
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

    @KafkaListener(topics = {"topic-users"}, groupId = "consumidor-springboot")
    public void listenUsers(String message) {

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        ObjectMapper mapper = new ObjectMapper();
        // mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        User user = null;
        try {
            user = mapper.readValue(message, User.class);
        } catch (JsonProcessingException e) {
            logger.error("No se pudo hacer la conversion");
        }

        logger.info("Usuario recibido: {}", message);
        logger.info("Usuario mapeado: " + user);

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
