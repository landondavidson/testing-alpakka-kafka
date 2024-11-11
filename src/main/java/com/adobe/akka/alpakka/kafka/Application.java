package com.adobe.akka.alpakka.kafka;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.kafka.ConsumerMessage;
import akka.kafka.ProducerMessage;
import akka.kafka.javadsl.Consumer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.CompletionStage;

public class Application {
    public static void main(String[] args) {
        System.out.println("Hello World!");
    }

    public static CompletionStage<Done> run(
            ActorSystem system,
            Source<ConsumerMessage.CommittableMessage<String, String>, Consumer.Control> source,
            Flow<
                    ProducerMessage.Envelope<String, String, ConsumerMessage.CommittableOffset>,
                    ProducerMessage.Results<String, String, ConsumerMessage.CommittableOffset>,
                    NotUsed> flow,
            Sink<ConsumerMessage.CommittableOffset, CompletionStage<Done>> sink) {
        Pair<Consumer.Control, CompletionStage<Done>> stream =
                source
                        .map(
                                msg ->
                                        (ProducerMessage.Envelope<String, String, ConsumerMessage.CommittableOffset>) new ProducerMessage.Message<>(
                                                new ProducerRecord<>(
                                                        "targetTopic", msg.record().key(), msg.record().value()),
                                                msg.committableOffset()))
                        .via(flow)
                        .map(ProducerMessage.Results::passThrough)
                        .toMat(sink, Keep.both())
                        .run(system);
        return stream.second();
    }
}
