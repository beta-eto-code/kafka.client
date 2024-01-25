<?php

namespace KafkaClient;

use MessageBroker\MessageInterface;
use RdKafka\Message as RdMessage;

class Message implements MessageInterface
{
    private RdMessage $originalMessage;

    public function __construct(RdMessage $message)
    {
        $this->originalMessage = $message;
    }

    public function confirm($data = null): void
    {
    }

    public function getOriginal(): RdMessage
    {
        return $this->originalMessage;
    }

    public function getData(): string
    {
        return $this->originalMessage->payload;
    }
}
