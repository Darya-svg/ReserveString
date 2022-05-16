package com.example.reverse.topology;

import com.example.Record;
import com.example.reverse.config.KafkaStreamConfiguration;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

@Component
@PropertySource("classpath:application.yaml")
@Slf4j
public class ReverseString {

  @Value("${topic.name.input}")
  private String inputTopic;
  @Value("${topic.name.output}")
  private String outputTopic;

  SpecificAvroSerde<Record> valueAvroSerde = KafkaStreamConfiguration.getAvroSerde();
  final Serde<String> stringSerde = Serdes.String();

/*  @Autowired
  void defineTopology(StreamsBuilder streamsBuilder) {
    streamsBuilder.stream(inputTopic,
        Consumed.with(stringSerde, valueAvroSerde))
            .peek((s, record) -> record.setText(String.valueOf(new StringBuilder(record.getText()).reverse())))
        .to(outputTopic, Produced.with(stringSerde, valueAvroSerde));
  }*/

  @Autowired
  void defineTopology(StreamsBuilder streamsBuilder) {
    streamsBuilder.stream(inputTopic,
            Consumed.with(stringSerde, valueAvroSerde))
        .peek((s, record) -> {
          record.setText(String.valueOf(new StringBuilder(record.getText()).reverse()));
          log.info(
              "Reversed message with key=[{}] is going to be written", record.getNumber());
        }
        )
        .to(outputTopic, Produced.with(stringSerde, valueAvroSerde));
  }
}
