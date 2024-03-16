package sk.projects.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Created by sathishkanagaraj on 16/03/24.
 */
public class KafkaSampleProducerApplication {

    private final Producer<String, String> producer;
    private final String outTopic;

    public KafkaSampleProducerApplication(Producer<String, String> producer, String outTopic) {
        this.producer = producer;
        this.outTopic = outTopic;
    }

    public Future<RecordMetadata> produce(final String message) {
        final String[] parts = message.split("-");
        final String key, value;
        if (parts.length > 1) {
            key = parts[0];
            value = parts[1];
        } else {
            key = "NO-KEY";
            value = parts[0];
        }
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(outTopic, key, value);
        return producer.send(producerRecord);
    }

    public void shutdown(){
        producer.close();
    }

    public static Properties loadProperties(String fileName) throws FileNotFoundException {
        final Properties properties = new Properties();
        try (FileInputStream fileInputStream = new FileInputStream(fileName)) {
            properties.load(fileInputStream);
            fileInputStream.close();
            return properties;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void printMeteData(final Collection<Future<RecordMetadata>> metedata, final String fileName){
        System.out.println("Offsets and timestamps committed in batch from " + fileName);
        metedata.forEach(m -> {
            try{
                final RecordMetadata recordMetadata = m.get();
                System.out.println("recordMetadata = " + recordMetadata.offset() + " timestamp : "+recordMetadata.timestamp());
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
                }
        );
    }

    public static void main(String[] args) throws FileNotFoundException {
        if(args.length < 2){
            throw new IllegalArgumentException("1. properties file and 2.inputFile");
        }
        final Properties properties = KafkaSampleProducerApplication.loadProperties(args[0]);
        final String topic = properties.getProperty("output.topic.name");
        Producer<String, String> producer = new KafkaProducer<>(properties);
        KafkaSampleProducerApplication producerApplication = new KafkaSampleProducerApplication(producer, topic);
        String filePath = args[1];
        try {
            List<String> linesToProduce = Files.readAllLines(Paths.get(filePath));
            List<Future<RecordMetadata>> metadata = linesToProduce.stream()
                    .filter(line -> !line.trim().isEmpty())
                    .filter(line -> !line.startsWith("1"))
                    .map(producerApplication::produce)
                    .collect(Collectors.toList());
            producerApplication.printMeteData(metadata, filePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            producerApplication.shutdown();
        }
    }
}


