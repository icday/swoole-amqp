# swoole-amqp

An async AMQP client base on swoole

# Classes

## swoole_amqp

Class `swoole_amqp`(`Swoole\\Amqp` if `swoole.use_namespace=On` was set in `php.ini`) represents client of amqp.

### Methods

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

`declareExchange(int $channel, string $exchange, string $type[, $passive, $durable, $autoDelete, $internal])`

#### `deleteExchange`

`deleteExchange(int $channel, string $exchange[, bool ifNotUsed)`

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

`qos(int $channel, int $size, $count[, $global = false])`

#### `consume`

`consume(int $channel, string $queueName, string $tag, bool $noLocal, bool $ack, bool exclusive)`

#### `on`

`on(string $event, callable $callback)`

#### `ack`

`ack(int $channel, int $deliveryTag[, bool $multiple = false])`

#### `cancel`

`cancel(int $channel, string $consumerTag)`

#### `close`

`close()`
