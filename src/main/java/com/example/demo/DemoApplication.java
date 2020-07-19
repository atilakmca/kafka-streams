package com.example.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.KTableImpl;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import sun.security.krb5.internal.tools.Ktab;

import java.util.Arrays;
import java.util.Properties;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);

		Properties properties = new Properties();

		properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"kafka-streams");
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder streamsBuilder = new StreamsBuilder();

		//KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String,String>().poll(10);

		KStream<String, String> kStream = streamsBuilder.stream("streams5-15");

        KTable<String, Long> kTable1 =  kStream.flatMapValues(x-> Arrays.asList(x.toLowerCase().split("\\W+")))
                .groupBy((key,value)->value)
                .count();

		KTable<String, Long> kTable = kStream.flatMapValues(x-> Arrays.asList(x.toLowerCase().split("\\W+")))
                             .groupBy((key,value)->value)
                             .count();

		System.out.println("%%%%%%%%%%%%%%%%%%%5" +kTable.toString().getBytes());


		kTable.toStream().to("streamsout5-15");

		KafkaStreams streams= new KafkaStreams(streamsBuilder.build(), properties);

		streams.start();
		System.out.println(streams.toString());


	}

}
