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

#### `bindExchange`

#### `unbindExchange`

#### `declareQueue`

#### `purgeQueue`

#### `deleteQueue`

#### `bindQueue`

#### `unbindQueue`

#### `qos`

#### `consume`

#### `on`

#### `ack`

#### `cancel`

#### `close`
