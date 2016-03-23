# swoole-amqp

An asynchronous PHP AMQP client base on swoole.

## Classes

### swoole_amqp

Class `swoole_amqp`(`Swoole\\Amqp` if `swoole.use_namespace=On` was set in `php.ini`) represents client of amqp.

Methods list of `swoole_amqp`:

- `__construct`
- `connect`
- `createChannel`
- `declareExchange`
- `deleteExchange`
- `bindExchange`
- `unbindExchange`
- `declareQueue`
- `purgeQueue`
- `deleteQueue`
- `bindQueue`
- `unbindQueue`
- `qos`
- `consume`
- `on`
- `ack`
- `cancel`
- `close`

#### `__construct`

`__construct($host, $port)`

#### `connect`

`connect($vhost, $username, $passwd[, $timeout])`.

It create a connection to AMQP server, and it should be called before doing any operations.

It is a blocking method actually.

#### `createChannel`

`createChannel($channel)`

`$channel` is an integer that identify a channel, it should less than 65535.

Channel represents a session with server base on a connection. It can be considered as logical connection. You can create many channel on a single connection.

Most operation are base on channel.

#### `declareExchange`

`declareExchange(int $channel, string $exchange, string $type[, bool $passive, bool $durable, bool $autoDelete, bool $internal])`

#### `deleteExchange`

`deleteExchange(int $channel, string $exchange[, bool ifNotUsed])`

#### `bindExchange`

`bindExchange(int $channel, string $destination, string $source, string $routing)`

#### `unbindExchange`

`unbindExchange(int $channel, string $destination, string $source, string $routine)`

#### `declareQueue`

`declareQueue(int $channel, string $queueName[, bool passive = false, bool durable = false, bool autoDelete = false, bool exclusive = false])`

#### `purgeQueue`

`purgeQueue(int $channel, string $queueName)`

#### `deleteQueue`

`deleteQueue(int $channel, string $queueName[, bool $ifUnused = false, bool $ifEmpty = false])`

#### `bindQueue`

`bindQueue(int $channel, string $queueName, string $exchangeName, string $routing)`

#### `unbindQueue`

`unbindQueue(int $channel, string $queueName, string $exchangeName, string $routing)`

#### `qos`

`qos(int $channel, int $size, $count[, bool $global = false])`

#### `consume`

`consume(int $channel, string $queueName, string $tag, bool $noLocal, bool $ack, bool exclusive)`

#### `on`

`on(string $event, callable $callback)`

`$event` could be `consume` / `channel_close` / `close`.

`$callback` must be **String, Array or Closure** that specifies a function.

For event `consume`, callback will be given an array value likes:

``` php
Array
(
    [channel] => 1
    [delivery_tag] => 1
    [redelivered] => 0
    [consumer_tag] => amq.ctag-gGpIz8Cc-L5tv9GRmjehfA
    [exchange] => exchange_name
    [message] => body
    [routing_key] => the-routing-key
)
```

For event `channel_close`, callback will be given a integer specifies the channel.

#### `ack`

`ack(int $channel, int $deliveryTag[, bool $multiple = false])`

#### `cancel`

`cancel(int $channel, string $consumerTag)`

#### `close`

`close()`

Closing the connection. This operation will destroy all the channel on this connection then close connection.

# Example

``` php
$rabbitmq = new swoole_amqp('127.0.0.1', 5672);
$rabbitmq->connect('vhost', 'guest', 'guest');
$rabbitmq->createChannel(1);
$rabbitmq->qos(1, 0, 10, 1);
$rabbitmq->on('consume', function($msg) {
    print_r($msg);
});
$rabbitmq->on('close', function() {
    echo "on close\n";
});
$rabbitmq->on('channel_close', function($channel) {
    echo "channel: $channel is closed\n";
});

$res = $rabbitmq->declareExchange(1, 'exchange.direct', 'direct');
$res = $rabbitmq->declareQueue(1, 'a.queue.name', 0, 1);

$rabbitmq->bindQueue(1, 'a.queue.name', 'exchange.direct', 'a.queue.name');

$tag = $rabbitmq->consume(1, 'a.queue.name', '', 0, 1, 0);
```


