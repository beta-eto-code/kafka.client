<?php

namespace KafkaClient;

use EmptyIterator;
use Iterator;
use MessageBroker\ClientInterface;
use MessageBroker\MessageInterface;
use RdKafka\Consumer;
use RdKafka\ConsumerTopic;
use RdKafka\Exception;
use RdKafka\Producer;
use RdKafka\ProducerTopic;
use RdKafka\TopicConf;

class Client implements ClientInterface
{
    private ?Consumer $consumer = null;
    private ?Producer $producer = null;
    private ?TopicConf $topicConf = null;

    public static function initAsConsumer(Consumer $consumer, ?TopicConf $topicConf = null): Client
    {
        return new static($consumer, null, $topicConf);
    }

    public static function initAsProducer(Producer $producer, ?TopicConf $topicConf = null): Client
    {
        return new static( null, $producer, $topicConf);
    }

    public function __construct(?Consumer $consumer = null, ?Producer $producer = null, ?TopicConf $topicConf = null)
    {
        $this->consumer = $consumer;
        $this->producer = $producer;
        $this->topicConf = $topicConf;
    }

    public function __destruct()
    {
        if ($this->consumer instanceof Consumer) {
            $this->consumer->flush(1000);
        }

        if ($this->producer instanceof Producer) {
            $this->producer->flush(1000);
        }
    }

    /**
     * @inheritdoc
     * @throws Exception
     */
    public function getMessageIterator(string $exchangeKey, array $options = []): Iterator
    {
        $this->checkConsumer();
        $topic = $this->createConsumerTopicWithOptions($exchangeKey, $options);
        $partition = (int) ($options['partition'] ?: 0);
        $offset = (int) ($options['offset'] ?: RD_KAFKA_OFFSET_STORED);
        $topic->consumeStart($partition, $offset);

        $timeout = (int) ($options['timeout'] ?: 1000);
        while ($message = $this->consumeMessage($topic, $partition, $timeout)) {
            yield $message;
        }
        return new EmptyIterator();
    }

    /**
     * @throws Exception
     */
    public function getMessage(string $exchangeKey, array $options = []): ?MessageInterface
    {
        $this->checkConsumer();
        $topic = $this->createConsumerTopicWithOptions($exchangeKey, $options);
        $partition = (int) ($options['partition'] ?: 0);
        $offset = (int) ($options['offset'] ?: RD_KAFKA_OFFSET_STORED);
        $topic->consumeStart($partition, $offset);

        $timeout = (int) ($options['timeout'] ?: 1000);
        $message = $this->consumeMessage($topic, $partition, $timeout);

        $topic->consumeStop($partition);
        return $message;
    }

    /**
     * @throws Exception
     */
    private function checkConsumer(): void
    {
        if (empty($this->consumer instanceof Consumer)) {
            throw new Exception('Consumer is not init');
        }
    }

    private function createConsumerTopicWithOptions(string $topicName, array $options = []): ConsumerTopic
    {
        $topicOptions = (array) ($options['topicOptions'] ?: []);
        if (empty($topicOptions)) {
            return $this->consumer->newTopic($topicName, $this->topicConf);
        }

        $topicConf = empty($this->topicConf) ? new TopicConf() : clone $this->topicConf;
        foreach ($topicOptions as $name => $value) {
            if (is_string($name) && !empty($name)) {
                $topicConf->set($name, $value);
            }
        }
        return $this->consumer->newTopic($topicName, $topicConf);
    }

    /**
     * @throws Exception
     */
    private function consumeMessage(ConsumerTopic $topic, int $partition, int $timeout): ?MessageInterface
    {
        $message = $topic->consume($partition, $timeout);
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                return new Message($message);
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                $topic->consumeStop($partition);
                return null;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                $topic->consumeStop($partition);
                throw new Exception('Time out');
            default:
                $topic->consumeStop($partition);
                throw new Exception($message->errstr(), $message->err);
        }
    }

    /**
     * @throws Exception
     */
    public function sendMessage(string $message, string $exchangeKey, array $options = [])
    {
        $this->checkProducer();
        $topic = $this->createProducerTopicWithOptions($exchangeKey, $options);
        $partition = (int) ($options['partition'] ?: 0);
        $messageFlag = (int) ($options['messageFlag'] ?: 0);
        $key = $options['key'] ?: null;
        $headers = $options['headers'] ?: null;
        if ($headers !== null && !is_array($headers)) {
            $headers = (array) $headers;
        }

        $timestamp = (int) ($options['timestamp'] ?: 0);
        $opaque = $options['opaque'] ?: null;
        $topic->producev($partition, $messageFlag, $message, $key, $headers, $timestamp, $opaque);
    }

    /**
     * @throws Exception
     */
    private function checkProducer(): void
    {
        if (empty($this->producer)) {
            throw new Exception('Producer is not init');
        }
    }

    private function createProducerTopicWithOptions(string $topicName, array $options = []): ProducerTopic
    {
        $topicOptions = (array) ($options['topicOptions'] ?: []);
        if (empty($topicOptions)) {
            return $this->producer->newTopic($topicName, $this->topicConf);
        }

        $topicConf = empty($this->topicConf) ? new TopicConf() : clone $this->topicConf;
        foreach ($topicOptions as $name => $value) {
            if (is_string($name) && !empty($name)) {
                $topicConf->set($name, $value);
            }
        }
        return $this->producer->newTopic($topicName, $topicConf);
    }
}
