# Клиент для работы с брокером сообщений Kafka

## Установка

```shell
composer require beta/kafka.client
```

## Consumer пример работы

```php
use RdKafka\Conf;
use RdKafka\Consumer;
use RdKafka\TopicConf;
use KafkaClient\Client;

$consumerConfig = new Conf();
$consumerConfig->set('group.id', 'myConsumerGroup'); // устанавливаем идентификатор группы потребителей сообщений
$consumerConfig->set('enable.partition.eof', 'true');

$consumer = new Consumer($consumerConfig);
$consumer->addBrokers("172.0.0.1,172.0.0.2"); // указываем адреса для подключения к брокерам сообщений

$topicConfig = new TopicConf();
$topicConfig->set('auto.commit.interval.ms', 100);
$topicConfig->set('offset.store.method', 'broker'); // механизм для хранения курсора
$topicConfig->set('auto.offset.reset', 'earliest'); // курсор по-умолчанию

$client = Client::initAsConsumer($consumer, $topicConfig);
$message = $client->getMessage(
    'my_topic', 
    [
        'partition' => 0,
        'offset' => 2,
        'timeout' => 2000
    ]
); // запрашиваем 1 сообщение из брокера
$message->getData(); // payload сообщения
$message->getOriginal(); // оригинальное сообщение RdKafka

/**
* Перебираем новые сообщения из брокера
**/
foreach ($client->getMessageIterator('my_topic') as $message) {
    echo $message->getData();
}
```

## Producer пример работы

```php
use RdKafka\Conf;
use RdKafka\Producer;
use RdKafka\TopicConf;
use KafkaClient\Client;

$producerConfig = new Conf();
$producerConfig->set('log_level', (string) LOG_DEBUG); // режим ведения логов
$producerConfig->set('debug', 'all');

$producer = new Producer($producerConfig);
$producer->addBrokers("172.0.0.1,172.0.0.2"); // указываем адреса для подключения к брокерам сообщений

$client = Client::initAsProducer($producer);
$client->sendMessage(
    'Test message', 
    'my_topic', 
    [
        'partition' => 0, 
        'key' => 'example', 
        'headers' => ['One' => 'Two']
    ]
);
```