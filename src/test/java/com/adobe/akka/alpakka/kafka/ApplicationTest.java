package com.adobe.akka.alpakka.kafka;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.function.Function;
import akka.kafka.ConsumerMessage;
import akka.kafka.ProducerMessage;
import akka.kafka.javadsl.Consumer;
import akka.kafka.testkit.ConsumerResultFactory;
import akka.kafka.testkit.ProducerResultFactory;
import akka.kafka.testkit.javadsl.ConsumerControlFactory;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.TestPublisher;
import akka.stream.testkit.TestSubscriber;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ApplicationTest {

    @Test
    void givenTwoCommittableMessages_whenConsumed_thenProduceThoseMessagesOntoATargetTopic() throws Exception {
        // Arrange
        var sys = ActorSystem.create("QuickStart");
        var topic = "topic";
        var partition = 0;
        var startOffset = 0L;
        var groupId = "groupId";

        // create elements emitted by the mocked Consumer
        List<ConsumerMessage.CommittableMessage<String, String>> elements =
                Arrays.asList(
                        ConsumerResultFactory.committableMessage(
                                new ConsumerRecord<>(topic, partition, startOffset, "key", "value 1"),
                                ConsumerResultFactory.committableOffset(
                                        groupId, topic, partition, startOffset, "metadata")),
                        ConsumerResultFactory.committableMessage(
                                new ConsumerRecord<>(topic, partition, startOffset + 1, "key", "value 2"),
                                ConsumerResultFactory.committableOffset(
                                        groupId, topic, partition, startOffset + 1, "metadata 2")));

        // remove the previous defined mockedKafkaConsumerSource and us a TestPublisher to allow for more granular control
        var source = new TestPublisher.Probe<ConsumerMessage.CommittableMessage<String, String>>(0, sys);

        // create a source imitating the Consumer.committableSource
        Source<ConsumerMessage.CommittableMessage<String, String>, Consumer.Control> mockedKafkaConsumerSource =
                Source.fromPublisher(source).viaMat(ConsumerControlFactory.controlFlow(), Keep.right());

        ProducerMessage.Message<String, String, ConsumerMessage.CommittableOffset> message1 = new ProducerMessage.Message<>(
                new ProducerRecord<>(
                        "targetTopic", elements.get(0).record().key(), elements.get(0).record().value()),
                elements.get(0).committableOffset());
        ProducerMessage.Message<String, String, ConsumerMessage.CommittableOffset> message2 = new ProducerMessage.Message<>(
                new ProducerRecord<>(
                        "targetTopic", elements.get(1).record().key(), elements.get(1).record().value()),
                elements.get(1).committableOffset());

        // create a source imitating the Producer.flexiFlow
        Function<ProducerMessage.Envelope<String, String, ConsumerMessage.CommittableOffset>,
                ProducerMessage.Results<String, String, ConsumerMessage.CommittableOffset>> mockTransformer = mock(Function.class);
        when(mockTransformer.apply(any()))
                .thenReturn(ProducerResultFactory.result(message1))
                .thenReturn(ProducerResultFactory.result(message2));
        Flow<
                ProducerMessage.Envelope<String, String, ConsumerMessage.CommittableOffset>,
                ProducerMessage.Results<String, String, ConsumerMessage.CommittableOffset>,
                NotUsed>
                mockedKafkaProducerFlow =
                Flow.fromFunction(mockTransformer);

        // create a committable sink setup to commit the offsets
        TestSubscriber.Probe<ConsumerMessage.CommittableOffset> requestReceivedProbe =
                new TestSubscriber.Probe<>(sys);
        Sink<ConsumerMessage.CommittableOffset, CompletionStage<Done>> mockCommitterSink =
                Sink.fromSubscriber(requestReceivedProbe)
                        .mapMaterializedValue(done -> CompletableFuture.completedFuture(null));

        CompletionStage<Done> stream =
                Application.run(
                        sys,
                        mockedKafkaConsumerSource,
                        mockedKafkaProducerFlow,
                        mockCommitterSink);

        // Act
        requestReceivedProbe.request(2);
        source
                .sendNext(elements.get(0))
                .sendNext(elements.get(1))
                .sendComplete();

        // Assert
        requestReceivedProbe
                .expectNext(elements.get(0).committableOffset())
                .expectNext(elements.get(1).committableOffset())
                .expectComplete();

        verify(mockTransformer, times(2)).apply(any());

        // Cleanup
        stream.thenAccept(done -> sys.terminate());
    }
}