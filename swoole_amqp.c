#include "php_swoole.h"

#include <string.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>

#include <amqp_tcp_socket.h>
#include <amqp.h>
#include <amqp_framing.h>

#define SW_AMQP_INIT_CHANNEL_TABLE_SIZE 10

#define SW_AMQP_DEFAULT_MAX_FRAME_SIZE 131072
#define SW_AMQP_DEFAULT_PORT 5672

#ifndef FALSE
#define FALSE 0
#endif

#ifndef TRUE
#define TRUE 1
#endif

extern swServerG SwooleG;

struct timeval timeout_zero = {0, 0};

typedef struct _swoole_amqp_client_t swoole_amqp_client_t;

struct _swoole_amqp_client_t {
    amqp_connection_state_t connection;

    HashTable *opened_channels;

    /* callbacks */
    zval *on_consume;
    zval *on_close;
    zval *on_channel_close;

    zval _object;
    zval *object;

    int connected:1;
};

static int add_connection_reactor(swoole_amqp_client_t *);
static int remove_connection_reactor(swoole_amqp_client_t *);

static int swoole_amqp_on_read(swReactor *, swEvent *);
static int swoole_amqp_on_channel_close(swoole_amqp_client_t *, int);
static void swoole_amqp_on_data(swoole_amqp_client_t *);

static int swoole_amqp_close_connection(swoole_amqp_client_t *, int);

static int swoole_amqp_handle_frame(swoole_amqp_client_t *, amqp_frame_t *, int);

static int swoole_amqp_handle_reply(swoole_amqp_client_t *, amqp_rpc_reply_t *, int);

static int add_connection_reactor(swoole_amqp_client_t *client) {

    php_swoole_check_reactor();

    swReactor *reactor = SwooleG.main_reactor;

    swConnection *conn = swReactor_get(reactor, amqp_get_sockfd(client->connection));
    conn->object = client;

    reactor->setHandle(reactor, PHP_SWOOLE_FD_AMQP | SW_EVENT_READ,
                       swoole_amqp_on_read);
    if (reactor->add(reactor, amqp_get_sockfd(client->connection), PHP_SWOOLE_FD_AMQP) < 0) {
        swoole_php_fatal_error(E_WARNING, "swoole_event_add amqp connection failed.");
        return -1;
    }

    return 0;
}

static int remove_connection_reactor(swoole_amqp_client_t *client) {
    swReactor *reactor = SwooleG.main_reactor;
    if (reactor->del(reactor, amqp_get_sockfd(client->connection)) == SW_ERR) {
        swTrace("error when del\n");
    }

    return 0;
}

static int swoole_amqp_on_read(swReactor *reactor, swEvent *event) {
    swoole_amqp_on_data(event->socket->object);
    return SW_OK;
}

static int swoole_amqp_on_channel_close(swoole_amqp_client_t *client, int channel) {
    amqp_rpc_reply_t reply = amqp_channel_close(client->connection, channel, AMQP_REPLY_SUCCESS);
    swoole_amqp_handle_reply(client, &reply, channel);

    if (!client->on_channel_close) {
        goto unset_opened_channel;
    }

    zval *zchan;
    MAKE_STD_ZVAL(zchan);
    ZVAL_LONG(zchan, channel);

    zval **args[] = {&zchan};
    zval *retval;

    if (sw_call_user_function_ex(EG(function_table), NULL, client->on_channel_close, &retval, 1, args, 0, NULL) != SUCCESS) {
        swoole_php_error(E_WARNING, "execute on channel close callback failure.");
    }
    sw_zval_ptr_dtor(&zchan);

unset_opened_channel:

    return zend_hash_index_del(client->opened_channels, channel);
}

static void swoole_amqp_on_data(swoole_amqp_client_t *client) {
    amqp_connection_state_t conn = client->connection;

    amqp_envelope_t envelope;
    amqp_frame_t frame;

    while (1) {
        amqp_maybe_release_buffers(conn);
        amqp_rpc_reply_t reply = amqp_consume_message(conn, &envelope, &timeout_zero, 0);
        /* TIMEOUT */
        if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION && reply.library_error == AMQP_STATUS_TIMEOUT) {
            /* do nothing than quit */
            return ;
        }

        int frameStatus = 0, again = 1;
        if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
            switch(reply.library_error) {
                case AMQP_STATUS_TIMEOUT:
                    return ;
                case AMQP_STATUS_SOCKET_ERROR:
                case AMQP_STATUS_SOCKET_CLOSED:
                case AMQP_STATUS_CONNECTION_CLOSED:
                    /* get socket error */
                    swoole_amqp_close_connection(client, 1);
                    return;
                case AMQP_STATUS_UNEXPECTED_STATE:
                default:
                    frameStatus = amqp_simple_wait_frame_noblock(conn, &frame, &timeout_zero);
                    if (!swoole_amqp_handle_frame(client, &frame, frameStatus)) {
                        return;
                    }
                    break;
            }
        } else {
            zval *zmessage;
            MAKE_STD_ZVAL(zmessage);
            array_init_size(zmessage, 7);

            add_assoc_long(zmessage, "channel", envelope.channel);
            add_assoc_long(zmessage, "delivery_tag", envelope.delivery_tag);
            add_assoc_bool(zmessage, "redelivered", envelope.redelivered);

            add_assoc_stringl(zmessage, "consumer_tag", envelope.consumer_tag.bytes, envelope.consumer_tag.len, 1);
            add_assoc_stringl(zmessage, "exchange", envelope.exchange.bytes, envelope.exchange.len, 1);
            add_assoc_stringl(zmessage, "message", envelope.message.body.bytes, envelope.message.body.len, 1);
            add_assoc_stringl(zmessage, "routing_key", envelope.routing_key.bytes, envelope.routing_key.len, 1);

            amqp_destroy_envelope(&envelope);

            zval **args[] = {&zmessage};
            zval *retval;
            if (sw_call_user_function_ex(EG(function_table), NULL, client->on_consume, &retval, 1, args, 0, NULL) != SUCCESS) {
                swoole_php_fatal_error(E_WARNING, "execute on consume callback failure.");
            }

            if (retval != NULL) {
                sw_zval_ptr_dtor(&retval);
            }
            sw_zval_ptr_dtor(&zmessage);
        }
    }
}

/**
 * @desc Close channel and connection, and callback
 *
 */
static int swoole_amqp_close_connection(swoole_amqp_client_t *client, int passive) {
    int error = 0;
    if (!client->connected || !client->connection) {
        error = -1;
        goto out1;
    }

    /* close channels and callback */
    HashTable *opened_channels = client->opened_channels;
    if (passive && !client->on_channel_close) {
        long *channel;
        HashPosition pos;
        for (zend_hash_internal_pointer_reset_ex(opened_channels, &pos);
             zend_hash_has_more_elements_ex(opened_channels, &pos) == SUCCESS;
             zend_hash_move_forward_ex(opened_channels, &pos)) {
            if (zend_hash_get_current_data_ex(opened_channels, (void *) &channel, &pos) == FAILURE ||
                *channel <= 0) {
                continue;
            }

            if (!passive) {
                amqp_rpc_reply_t reply = amqp_channel_close(client->connection, *channel, AMQP_REPLY_SUCCESS);
            }

            swoole_amqp_on_channel_close(client, *channel);
        }
    }
    zend_hash_clean(opened_channels);

    if (!passive) {
        amqp_rpc_reply_t reply = amqp_connection_close(client->connection, AMQP_REPLY_SUCCESS);
    }

    if (client->on_close) {
        zval *retval;
        if (sw_call_user_function_ex(EG(function_table), NULL, client->on_close, &retval, 0, NULL, 0, NULL) != SUCCESS) {
            swoole_php_fatal_error(E_WARNING, "execute on connection close callback failure.");
        }
    }

out1:
    if (client->connection) {
        remove_connection_reactor(client);

        int status = amqp_destroy_connection(client->connection);
        if (status != AMQP_STATUS_OK) {
            swoole_php_fatal_error(E_WARNING, "Destroy connection failure.");
        }
    }
    client->connection = NULL;
    client->connected = 0;
    sw_zval_ptr_dtor(&client->object);

    return error;
}

/**
 *
 * @return {int} keep on recv or not
 */
static int swoole_amqp_handle_frame(swoole_amqp_client_t *client, amqp_frame_t *frame, int status) {
    /* @return AMQP_STATUS_OK / AMQP_STATUS_INVALID_PARAMETER / AMQP_STATUS_TIMER_FAILURE / AMQP_STATUS_NO_MEMORY / AMQP_STATUS_BAD_AMQP_DATA*/

    if (status == AMQP_STATUS_OK) {
        if (frame->frame_type == AMQP_FRAME_METHOD) {
            switch(frame->payload.method.id) {
                case AMQP_CONNECTION_CLOSE_METHOD:
                    swoole_amqp_close_connection(client, 1);
                    return FALSE;
                case AMQP_CHANNEL_CLOSE_METHOD:
                    swoole_amqp_on_channel_close(client, frame->channel);
                    return TRUE;
                    /* TODO more */
                default:
                    break;
            }
        } /* else TODO */

    } else {
        switch(status) {
            case AMQP_STATUS_TIMEOUT:
                return FALSE;
                /* close connection for other error */
            default:
                swTrace("recv frame error, will close connection\n");
                swoole_amqp_close_connection(client, 1);
                return FALSE;
        }
    }

    return TRUE;
}

static int swoole_amqp_handle_reply(swoole_amqp_client_t *client, amqp_rpc_reply_t *reply, int channel) {
    if (reply->reply_type == AMQP_RESPONSE_NORMAL) {
        return TRUE;
    }

    if (reply->reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
        switch(reply->library_error) {
            case AMQP_STATUS_TIMEOUT:
                return FALSE;
                /* close connection for other error */
            default:
                swoole_amqp_close_connection(client, 1);
                return FALSE;
        }
        return FALSE;
    }


    /* AMQP_RESPONSE_SERVER_EXCEPTION */
    switch(reply->reply.id) {
        case AMQP_CONNECTION_CLOSE_METHOD:
            swoole_amqp_close_connection(client, 1);
            return FALSE;
        case AMQP_CHANNEL_CLOSE_METHOD:
            if (channel <= 0) {
                return FALSE;
            }
            swoole_amqp_on_channel_close(client, channel);
            /* TODO more */
            break;
        default:
            break;
    }

    return FALSE;
}

static PHP_METHOD(swoole_amqp, __construct);
static PHP_METHOD(swoole_amqp, __destruct);
static PHP_METHOD(swoole_amqp, connect);
static PHP_METHOD(swoole_amqp, createChannel);
static PHP_METHOD(swoole_amqp, consume);
static PHP_METHOD(swoole_amqp, cancel);
static PHP_METHOD(swoole_amqp, on);
static PHP_METHOD(swoole_amqp, close);
static PHP_METHOD(swoole_amqp, qos);

static PHP_METHOD(swoole_amqp, declareExchange);
static PHP_METHOD(swoole_amqp, deleteExchange);
static PHP_METHOD(swoole_amqp, bindExchange);
static PHP_METHOD(swoole_amqp, unbindExchange);

static PHP_METHOD(swoole_amqp, declareQueue);
static PHP_METHOD(swoole_amqp, deleteQueue);
static PHP_METHOD(swoole_amqp, bindQueue);
static PHP_METHOD(swoole_amqp, unbindQueue);
static PHP_METHOD(swoole_amqp, purgeQueue);

static PHP_METHOD(swoole_amqp, ack);

static zend_class_entry swoole_amqp_ce;
zend_class_entry* swoole_amqp_class_entry_ptr;

static const zend_function_entry swoole_amqp_methods[] = {
    PHP_ME(swoole_amqp, __construct, NULL, ZEND_ACC_PUBLIC | ZEND_ACC_CTOR)
    PHP_ME(swoole_amqp, __destruct, NULL, ZEND_ACC_PUBLIC | ZEND_ACC_DTOR)
    PHP_ME(swoole_amqp, connect, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(swoole_amqp, createChannel, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(swoole_amqp, qos, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(swoole_amqp, ack, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(swoole_amqp, consume, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(swoole_amqp, cancel, NULL, ZEND_ACC_PUBLIC)
    ZEND_MALIAS(swoole_amqp, unconsume, cancel, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(swoole_amqp, on, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(swoole_amqp, close, NULL, ZEND_ACC_PUBLIC)

    PHP_ME(swoole_amqp, declareExchange, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(swoole_amqp, deleteExchange, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(swoole_amqp, bindExchange, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(swoole_amqp, unbindExchange, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(swoole_amqp, declareQueue, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(swoole_amqp, deleteQueue, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(swoole_amqp, bindQueue, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(swoole_amqp, unbindQueue, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(swoole_amqp, purgeQueue, NULL, ZEND_ACC_PUBLIC)
    PHP_FE_END
};

void swoole_amqp_init(int module_number TSRMLS_DC) {
    SWOOLE_INIT_CLASS_ENTRY(swoole_amqp_ce, "swoole_amqp", "Swoole\\Amqp", swoole_amqp_methods);
    swoole_amqp_class_entry_ptr = zend_register_internal_class(&swoole_amqp_ce);

    zend_declare_property_string(swoole_amqp_class_entry_ptr, "ip", sizeof("ip") - 1, "127.0.0.1", ZEND_ACC_PROTECTED TSRMLS_CC);
    zend_declare_property_long(swoole_amqp_class_entry_ptr, "port", sizeof("port") - 1, SW_AMQP_DEFAULT_PORT, ZEND_ACC_PROTECTED TSRMLS_CC);
    zend_declare_property_string(swoole_amqp_class_entry_ptr, "vhost", sizeof("vhost") - 1, "/", ZEND_ACC_PROTECTED TSRMLS_CC);
    zend_declare_property_string(swoole_amqp_class_entry_ptr, "username", sizeof("username") - 1, "", ZEND_ACC_PROTECTED TSRMLS_CC);
    zend_declare_property_string(swoole_amqp_class_entry_ptr, "password", sizeof("password") - 1, "", ZEND_ACC_PROTECTED TSRMLS_CC);
}

static PHP_METHOD(swoole_amqp, __construct) {
    char *ip;
    zend_size_t ip_len;
    long port;
    zval *object = getThis();

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "sl", &ip, &ip_len, &port) == FAILURE) {
        RETURN_FALSE;
    }

    if (port <= 0 || ip_len <= 0) {
        RETURN_FALSE;
    }

    zend_update_property_stringl(swoole_amqp_class_entry_ptr, object, "ip", sizeof("ip") - 1, ip, ip_len TSRMLS_CC);

    zend_update_property_long(swoole_amqp_class_entry_ptr, object, "port", sizeof("port") - 1, port TSRMLS_CC);

    swoole_amqp_client_t *client = emalloc(sizeof(*client));
    memset(client, 0, sizeof(*client));
    ALLOC_HASHTABLE(client->opened_channels);
    if (zend_hash_init(client->opened_channels, SW_AMQP_INIT_CHANNEL_TABLE_SIZE, NULL, NULL, 0) == FAILURE) {
        FREE_HASHTABLE(client->opened_channels);
        RETURN_FALSE;
    }

#if PHP_MAJOR_VERSION < 7
    client->object = object;
#else
    memcpy(&client->_object, object, sizeof(zval));
    client->object = &client->_object;
#endif

    swoole_set_object(object, client);
}

static PHP_METHOD(swoole_amqp, __destruct) {
    zval *object = getThis();
    swoole_amqp_client_t *client = swoole_get_object(object);

    if (client->on_consume) {
        sw_zval_ptr_dtor(&client->on_consume);
    }

    if (client->on_channel_close) {
        sw_zval_ptr_dtor(&client->on_channel_close);
    }

    if (client->on_close) {
        sw_zval_ptr_dtor(&client->on_close);
    }

    swoole_set_object(object, NULL);
    efree(client);
}

static PHP_METHOD(swoole_amqp, connect) {
    char *username, *password, *vhost;
    zend_size_t username_len, password_len, vhost_len;
    long itimeout = 0;
    zval *object = getThis();

    /*  vhost, username, password, timeout */
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "sss|l", &vhost, &vhost_len, &username, &username_len,
                              &password, &password_len, &itimeout) == FAILURE) {
        RETURN_FALSE;
    }

    swoole_amqp_client_t *client = swoole_get_object(object);

    if (client->connected) {
        RETURN_TRUE;
    }

    if (itimeout < 0) {
        itimeout = 0;
    }

    zend_update_property_stringl(swoole_amqp_class_entry_ptr, object, "vhost", sizeof("vhost") - 1, vhost, vhost_len TSRMLS_CC);
    zend_update_property_stringl(swoole_amqp_class_entry_ptr, object, "username", sizeof("username") - 1, username, username_len TSRMLS_CC);
    zend_update_property_stringl(swoole_amqp_class_entry_ptr, object, "password", sizeof("password") - 1, password, password_len TSRMLS_CC);

    zend_update_property_long(swoole_amqp_class_entry_ptr, object, "timeout", sizeof("timeout") - 1, itimeout TSRMLS_CC);

    zval *zip = zend_read_property(swoole_amqp_class_entry_ptr, object, "ip", sizeof("ip") - 1, 0 TSRMLS_CC);
    zval *zport = zend_read_property(swoole_amqp_class_entry_ptr, object, "port", sizeof("port") - 1, 0 TSRMLS_CC);

    if (client->connection) {
        amqp_destroy_connection(client->connection);
    }
    amqp_connection_state_t conn = client->connection = amqp_new_connection();
    amqp_socket_t *socket = amqp_tcp_socket_new(conn);

    struct timeval timeout = {itimeout, 0};
    if (amqp_socket_open_noblock(socket, Z_STRVAL_P(zip), Z_LVAL_P(zport), &timeout) != AMQP_STATUS_OK) {
        RETURN_FALSE;
    }

    amqp_rpc_reply_t reply = amqp_login(conn, vhost, AMQP_DEFAULT_MAX_CHANNELS, SW_AMQP_DEFAULT_MAX_FRAME_SIZE, 0,
                                        AMQP_SASL_METHOD_PLAIN, username, password);
    if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
        client->connected = 1;
        sw_zval_add_ref(&client->object);
        RETURN_TRUE;
    }

    RETURN_FALSE;
}

static PHP_METHOD(swoole_amqp, createChannel) {
    long channel;
    zval *object = getThis();
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "l", &channel) == FAILURE) {
        RETURN_FALSE;
    }

    if (channel <= 0) {
        swoole_php_fatal_error(E_WARNING, "channel must greater than 0.");
        RETURN_FALSE;
    }

    swoole_amqp_client_t *client = swoole_get_object(object);
    if (!client->connected) {
        swoole_php_error(E_WARNING, "amqp is not connected to server.");
        RETURN_FALSE;
    }

    if (zend_hash_index_exists(client->opened_channels, channel)) {
        swoole_php_error(E_WARNING, "Channel %d is opened, can not open a channel twice.", channel);
        RETURN_FALSE;
    }

    amqp_channel_open(client->connection, channel);
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(client->connection);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
        swoole_php_fatal_error(E_WARNING, "Failure to open channel %d.\n");
        swoole_amqp_handle_reply(client, &reply, channel);
        RETURN_FALSE;
    }

    zend_hash_index_update(client->opened_channels, channel, &channel, sizeof(channel), NULL);

    RETURN_TRUE;
}

static PHP_METHOD(swoole_amqp, declareExchange) {
    long channel;
    /* TODO CONSTANT to name */
    char *exchange_name, *type;
    zend_size_t name_len, type_len;
    zend_bool passive = 0, durable = 0, auto_delete = 0, internal = 0;
    /* TODO args */

    zval *object = getThis();

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "lss|bbbb", &channel, &exchange_name, &name_len, &type, &type_len,
                &passive, &durable, &auto_delete, &internal) == FAILURE) {
        RETURN_FALSE;
    }

    swoole_amqp_client_t *client = swoole_get_object(object);

    if (channel <= 0 || !zend_hash_index_exists(client->opened_channels, channel)) {
        swoole_php_error(E_WARNING, "Channel %d is not a available channle.\n", channel);
        RETURN_FALSE;
    }

    amqp_table_t empty_args = {0, NULL};
    amqp_exchange_declare_ok_t *ok = amqp_exchange_declare(client->connection, channel, amqp_cstring_bytes(exchange_name), amqp_cstring_bytes(type),
                passive, durable, auto_delete, internal, empty_args);
    if (!ok) {
        swoole_php_error(E_WARNING, "Failure to declare exchange:%s.\n", exchange_name);
        amqp_rpc_reply_t reply = amqp_get_rpc_reply(client->connection);
        swoole_amqp_handle_reply(client, &reply, channel);
        RETURN_FALSE;
    }
    RETURN_TRUE;
}

static PHP_METHOD(swoole_amqp, deleteExchange) {
    long channel;
    char *exchange_name;
    zend_size_t exchange_name_len;
    /* 是否只在没有bindings时删除 */
    zend_bool if_unused = 0;

    zval *object = getThis();

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ls|b", &channel, &exchange_name, &exchange_name_len, &if_unused) == FAILURE) {
        RETURN_FALSE;
    }

    swoole_amqp_client_t *client = swoole_get_object(object);

    if (channel <= 0 || !zend_hash_index_exists(client->opened_channels, channel)) {
        swoole_php_error(E_WARNING, "Channel %d is not a available channle.\n", channel);
        RETURN_FALSE;
    }

    amqp_table_t empty_args = {0, NULL};
    amqp_exchange_delete_ok_t *ok = amqp_exchange_delete(client->connection, channel, amqp_cstring_bytes(exchange_name), if_unused);

    if (!ok) {
        swoole_php_fatal_error(E_WARNING, "Error when deleting exchange %s.\n", exchange_name);
        amqp_rpc_reply_t reply = amqp_get_rpc_reply(client->connection);
        swoole_amqp_handle_reply(client, &reply, channel);
        RETURN_FALSE;
    }

    RETURN_TRUE;
}

static PHP_METHOD(swoole_amqp, bindExchange) {
    long channel;
    char *destination, *source, *routing_key;
    zend_size_t dest_len, source_len, key_len;

    zval *object = getThis();

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "lsss", &channel, &destination, &dest_len, &source, &source_len, &routing_key, &key_len) == FAILURE) {
        RETURN_FALSE;
    }

    swoole_amqp_client_t *client = swoole_get_object(object);

    if (channel <= 0 || !zend_hash_index_exists(client->opened_channels, channel)) {
        swoole_php_error(E_WARNING, "Channel %d is not a available channle.\n", channel);
        RETURN_FALSE;
    }

    amqp_table_t empty_args = {0, NULL};
    amqp_exchange_bind_ok_t *ok = amqp_exchange_bind(client->connection, channel, amqp_cstring_bytes(destination),
            amqp_cstring_bytes(source), amqp_cstring_bytes(routing_key), empty_args);
    if (!ok) {
        swoole_php_fatal_error(E_WARNING, "Failure to bind exchange, from %s to %s by %s.\n", source, destination, routing_key);
        amqp_rpc_reply_t reply = amqp_get_rpc_reply(client->connection);
        swoole_amqp_handle_reply(client, &reply, channel);
        RETURN_FALSE;
    }
    RETURN_TRUE;
}

static PHP_METHOD(swoole_amqp, unbindExchange) {
    long channel;
    char *destination, *source, *routing_key;
    zend_size_t dest_len, source_len, key_len;

    zval *object = getThis();

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "lsss", &channel, &destination, &dest_len, &source, &source_len, &routing_key, &key_len) == FAILURE) {
        RETURN_FALSE;
    }

    swoole_amqp_client_t *client = swoole_get_object(object);

    if (channel <= 0 || !zend_hash_index_exists(client->opened_channels, channel)) {
        swoole_php_error(E_WARNING, "Channel %d is not a available channle.\n", channel);
        RETURN_FALSE;
    }

    amqp_table_t empty_args = {0, NULL};
    amqp_exchange_unbind_ok_t *ok = amqp_exchange_unbind(client->connection, channel, amqp_cstring_bytes(destination),
            amqp_cstring_bytes(source), amqp_cstring_bytes(routing_key), empty_args);
    if (!ok) {
        swoole_php_fatal_error(E_WARNING, "Failure to unbind exchange, from %s to %s by %s.\n", source, destination, routing_key);
        amqp_rpc_reply_t reply = amqp_get_rpc_reply(client->connection);
        swoole_amqp_handle_reply(client, &reply, channel);
        RETURN_FALSE;
    }
    RETURN_TRUE;
}

static PHP_METHOD(swoole_amqp, declareQueue) {
    long channel;
    char *queue_name;
    zend_size_t name_len;
    zend_bool passive = 0, durable = 0, auto_delete = 0, exclusive = 0;

    zval *object = getThis();
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ls|bbbb", &channel, &queue_name, &name_len, &passive, &durable, &auto_delete, &exclusive)
            == FAILURE) {
        RETURN_FALSE;
    }

    swoole_amqp_client_t *client = swoole_get_object(object);
    if (channel <= 0 || !zend_hash_index_exists(client->opened_channels, channel)) {
        swoole_php_error(E_WARNING, "Channel %d is not a available channle.\n", channel);
        RETURN_FALSE;
    }

    amqp_table_t empty_args = {0, NULL};
    amqp_queue_declare_ok_t *ok = amqp_queue_declare(client->connection, channel, amqp_cstring_bytes(queue_name),
            passive, durable, exclusive, auto_delete, empty_args);
    if (!ok) {
        swoole_php_fatal_error(E_WARNING, "Error when declare queue %s.\n", queue_name);
        amqp_rpc_reply_t reply = amqp_get_rpc_reply(client->connection);
        swoole_amqp_handle_reply(client, &reply, channel);
        RETURN_FALSE;
    }

    RETURN_TRUE;
}

static PHP_METHOD(swoole_amqp, purgeQueue) {
    long channel;
    char *queue_name;
    zend_size_t name_len;

    zval *object = getThis();
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ls", &channel, &queue_name, &name_len) == FAILURE) {
        RETURN_FALSE;
    }

    swoole_amqp_client_t *client = swoole_get_object(object);
    if (channel <= 0 || !zend_hash_index_exists(client->opened_channels, channel)) {
        swoole_php_error(E_WARNING, "Channel %d is not a available channle.\n", channel);
        RETURN_FALSE;
    }

    amqp_table_t empty_args = {0, NULL};
    amqp_queue_purge_ok_t *ok = amqp_queue_purge(client->connection, channel, amqp_cstring_bytes(queue_name));
    if (!ok) {
        swoole_php_fatal_error(E_WARNING, "Error when purging queue %s.\n", queue_name);
        amqp_rpc_reply_t reply = amqp_get_rpc_reply(client->connection);
        swoole_amqp_handle_reply(client, &reply, channel);
        RETURN_FALSE;
    }

    RETURN_LONG(ok->message_count);
}

static PHP_METHOD(swoole_amqp, deleteQueue) {
    long channel;
    char *queue_name;
    zend_size_t queue_name_len;
    /* 是否只在没有consumer时 | 队列没有消息时  删除，不满足条件则会报错 */
    zend_bool if_unused = 0, if_empty = 0;

    zval *object = getThis();

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ls|bb", &channel, &queue_name, &queue_name_len, &if_unused, &if_empty) == FAILURE) {
        RETURN_FALSE;
    }

    swoole_amqp_client_t *client = swoole_get_object(object);

    if (channel <= 0 || !zend_hash_index_exists(client->opened_channels, channel)) {
        swoole_php_error(E_WARNING, "Channel %d is not a available channle.\n", channel);
        RETURN_FALSE;
    }

    amqp_table_t empty_args = {0, NULL};
    amqp_queue_delete_ok_t *ok = amqp_queue_delete(client->connection, channel, amqp_cstring_bytes(queue_name), if_unused, if_empty);

    if (!ok) {
        swoole_php_fatal_error(E_WARNING, "Error when deleting queue %s.\n", queue_name);
        amqp_rpc_reply_t reply = amqp_get_rpc_reply(client->connection);
        swoole_amqp_handle_reply(client, &reply, channel);
        RETURN_FALSE;
    }

    RETURN_LONG(ok->message_count);
}

static PHP_METHOD(swoole_amqp, bindQueue) {
    long channel;
    char *queue_name, *exchange_name, *routing_key;
    zend_size_t queue_name_len, exchange_name_len, routing_key_len;

    zval *object = getThis();
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "lsss", &channel, &queue_name, &queue_name_len, &exchange_name, &exchange_name_len,
                &routing_key, &routing_key_len) == FAILURE) {
        RETURN_FALSE;
    }

    swoole_amqp_client_t *client = swoole_get_object(object);
    if (channel <= 0 || !zend_hash_index_exists(client->opened_channels, channel)) {
        swoole_php_error(E_WARNING, "Channel %d is not a available channle.\n", channel);
        RETURN_FALSE;
    }

    amqp_table_t empty_args = {0, NULL};
    amqp_queue_bind_ok_t *ok = amqp_queue_bind(client->connection, channel, amqp_cstring_bytes(queue_name), amqp_cstring_bytes(exchange_name),
                amqp_cstring_bytes(routing_key), empty_args);
    if (!ok) {
        amqp_rpc_reply_t reply = amqp_get_rpc_reply(client->connection);
        swoole_amqp_handle_reply(client, &reply, channel);
        RETURN_FALSE;
    }

    RETURN_TRUE;
}

static PHP_METHOD(swoole_amqp, unbindQueue) {
    long channel;
    char *queue_name, *exchange_name, *routing_key;
    zend_size_t queue_name_len, exchange_name_len, routing_key_len;

    zval *object = getThis();
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "lsss", &channel, &queue_name, &queue_name_len, &exchange_name, &exchange_name_len,
                &routing_key, &routing_key_len) == FAILURE) {
        RETURN_FALSE;
    }

    swoole_amqp_client_t *client = swoole_get_object(object);
    if (channel <= 0 || !zend_hash_index_exists(client->opened_channels, channel)) {
        swoole_php_error(E_WARNING, "Channel %d is not a available channle.\n", channel);
        RETURN_FALSE;
    }

    amqp_table_t empty_args = {0, NULL};
    amqp_queue_unbind_ok_t *ok = amqp_queue_unbind(client->connection, channel, amqp_cstring_bytes(queue_name), amqp_cstring_bytes(exchange_name),
                amqp_cstring_bytes(routing_key), empty_args);
    if (!ok) {
        amqp_rpc_reply_t reply = amqp_get_rpc_reply(client->connection);
        swoole_amqp_handle_reply(client, &reply, channel);
        RETURN_FALSE;
    }

    RETURN_TRUE;
}

/**
 * @param   {long} channel
 * @param   {long} size
 * @param   {long} count
 * @param   {bool} global. defaults to false
 *
 * @return {bool}
 */
static PHP_METHOD(swoole_amqp, qos) {
    long size, count, channel;
    zend_bool global = 0;
    zval *object = getThis();

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "lll|b", &channel, &size, &count, &global) == FAILURE) {
        RETURN_FALSE;
    }

    if (channel <= 0) {
        RETURN_FALSE;
    }

    swoole_amqp_client_t *client = swoole_get_object(object);
    if (!client->connected) {
        swoole_php_error(E_WARNING, "amqp is not connected to server.");
        RETURN_FALSE;
    }

    if (size < 0) {
        size = 0;
    }
    if (count < 0) {
        count = 0;
    }

    amqp_basic_qos_ok_t *ok = amqp_basic_qos(client->connection, channel, size, count, global);

    RETURN_BOOL((ok != NULL));
}

/**
 * @desc    应当在最后调用
 * @param   {int} $channle
 * @param   {string} $queueName
 * @param   {string} $tag
 * @param   {bool} $noLocal
 * @param   {bool} $needAck
 * @param   {bool} $exclusive
 *
 * @return {bool} whether consume success
 */
static PHP_METHOD(swoole_amqp, consume) {
    long channel;
    char *queue, *tag;
    zend_size_t queue_len, tag_len;
    zend_bool no_local, ack, exclusive;
    zval *object = getThis();
    /*  args */

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "lssbbb", &channel, &queue, &queue_len, &tag, &tag_len,
                              &no_local, &ack, &exclusive) == FAILURE) {
        RETURN_FALSE;
    }

    swoole_amqp_client_t *client = swoole_get_object(object);

    if (queue_len <= 0) {
        RETURN_FALSE;
    }

    if (!client->connected) {
        RETURN_FALSE;
    }

    if (!client->on_consume) {
        swoole_php_error(E_WARNING, "Can not starts consuming before set the consuming callback.");
        RETURN_FALSE;
    }

    if (channel <= 0 || !zend_hash_index_exists(client->opened_channels, channel)) {
        swoole_php_error(E_WARNING, "Channel %ld is not opened, can not consume on it.\n", channel);
        RETURN_FALSE;
    }

    amqp_bytes_t tag_bytes = tag_len > 0 ? amqp_cstring_bytes(tag) : amqp_empty_bytes;
    amqp_basic_consume_ok_t *ok = amqp_basic_consume(client->connection, channel, amqp_cstring_bytes(queue), tag_bytes,
                       no_local, !ack, exclusive, amqp_empty_table);
    if (!ok) {
        swoole_php_fatal_error(E_WARNING, "Failure to consume queue:%s.\n", queue);
        amqp_rpc_reply_t reply = amqp_get_rpc_reply(client->connection);
        swoole_amqp_handle_reply(client, &reply, channel);
        RETURN_FALSE;
    }

    add_connection_reactor(client);
    /* MUST check message at once for checking recv buffer. */
    swoole_amqp_on_data(client);

    RETURN_STRINGL(ok->consumer_tag.bytes, ok->consumer_tag.len, 1);
}

static PHP_METHOD(swoole_amqp, on) {
    char *event;
    zend_size_t event_len;
    zval *cb, *object = getThis();

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "sz!", &event, &event_len, &cb) == FAILURE) {
        RETURN_FALSE;
    }

    if (event_len <= 0) {
        RETURN_FALSE;
    }

#if PHP_MAJOR_VERSION >= 7
    zval *_tmp = emalloc(sizeof(zval));
    memcpy(_tmp, cb, sizeof(zval));
    cb = _tmp;
#endif

    swoole_amqp_client_t *client = swoole_get_object(object);

    if (strncasecmp("consume", event, event_len) == 0) {
        client->on_consume = cb;
    } else if (strncasecmp("close", event, event_len) == 0) {
        client->on_close = cb;
    } else if (strncasecmp("channel_close", event, event_len) == 0) {
        client->on_channel_close = cb;
    } else {
        RETURN_FALSE;
    }
    sw_zval_add_ref(&cb);

    RETURN_TRUE;
}

static PHP_METHOD(swoole_amqp, ack) {
    long channel, delivery_tag;
    zend_bool multiple = 0;
    zval *object = getThis();

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ll|b", &channel, &delivery_tag, &multiple) == FAILURE) {
        RETURN_FALSE;
    }

    /* TODO check channel*/
    if (channel <= 0 || delivery_tag <= 0) {
        RETURN_FALSE;
    }

    swoole_amqp_client_t *client = swoole_get_object(object);
    if (!client->connected) {
        swoole_php_error(E_WARNING, "Can not ack message, client is not connected to server.");
        RETURN_FALSE;
    }

    RETURN_LONG(amqp_basic_ack(client->connection, channel, delivery_tag, multiple));
}

static PHP_METHOD(swoole_amqp, cancel) {
    char *consumer_tag;
    zend_size_t tag_len;
    zval *object = getThis();
    long channel;

    swoole_amqp_client_t *client = swoole_get_object(object);

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ls", &channel, &consumer_tag, &tag_len) == FAILURE) {
        RETURN_FALSE;
    }

    if (channel <= 0 || !zend_hash_index_exists(client->opened_channels, channel)) {
        swoole_php_error(E_WARNING, "Channel %ld is not a available channle.\n", channel);
        RETURN_FALSE;
    }

    if (tag_len <= 0) {
        swoole_php_error(E_WARNING, "\"%s\" is not a valid consumer tag.\n", consumer_tag);
        RETURN_FALSE;
    }

    if (!client->connected) {
        swoole_php_error(E_WARNING, "Client is not connected to server.\n");
        RETURN_FALSE;
    }

    amqp_basic_cancel_ok_t *ok = amqp_basic_cancel(client->connection, channel, amqp_cstring_bytes(consumer_tag));
    RETURN_BOOL((ok != NULL));
}

static PHP_METHOD(swoole_amqp, close) {
    zval *object = getThis();

    swoole_amqp_client_t *client = swoole_get_object(object);

    if (!client->connected) {
        swoole_php_error(E_WARNING, "Client is not connected to server, can not close it.");
        RETURN_FALSE;
    }

    swoole_amqp_close_connection(client, 0);
}
