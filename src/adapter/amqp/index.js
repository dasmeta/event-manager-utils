const amqplib = require('amqplib');
const { Buffer } = require('buffer');
const genericPool = require('generic-pool');

class messageAdapter {
    #AMQPMessage;
    #channel;

    constructor(AMQPMessage, channel) {
        this.#channel = channel;
        this.#AMQPMessage = AMQPMessage;

        const content = JSON.parse(AMQPMessage.content.toString());

        this.data = Buffer.from(JSON.stringify(content));
    }

    ack() {
        this.#channel.ack(this.#AMQPMessage);
    }
}

class subscriptionAdapter {
    #channel;

    /**
     * Rabbitmq queue name and subscription tag
     * @type string
     */
    #name;

    /**
     * @type topicAdapter
     */
    #topic;

    onMassage = async () => {};
    onError = async () => {};

    constructor(name, topic) {
        this.#name = name;
        this.#topic = topic;
    }

    async exists(...props) {
        await this.#topic.exists();

        this.#channel = await (this.#topic.getChannel())
        await this.#channel.assertQueue(this.#name, {
            exclusive: true
        })

        await this.#channel.bindQueue(this.#name, this.#topic.name, '');

        return [true];
    }

    async create() {
        return true;
    }

    async on(key, callback) {
        if (key === 'message') {
            await this.exists()

            const channel = this.#channel;

            this.onMassage = async (msg) => {
                if (msg !== null) {
                    await callback(new messageAdapter(msg, channel))
                }
            }

            this.#channel.consume(this.#name, this.onMassage, {
                consumerTag: this.#name,
                noAck: false
            });
        } else if (key === 'error') {
            console.log(`key: '${key}' handler is not implemented`)
        } else {
            console.error(`key: '${key}' handler is not implemented`)
        }
    }

    removeListener(key, callback) {
        if(this.#channel) {
            this.#channel.cancel(this.#name)
        }
    }
}

class topicAdapter {
    /**
     * @type string
     */
    #getChannel;
    #exchangeType;
    #pool;

    constructor(name, getChannel, pool) {
        /* Rabbitmq exchanger name */
        this.name = name;

        /* Function to get a RabbitMQ channel */
        this.#getChannel = getChannel;

        /* Connection pool */
        this.#pool = pool;

        /* Rabbitmq exchanger type */
        this.#exchangeType = process.env.MQ_EXCHANGE_TYPE || 'fanout';
    }

    subscription(name) {
        return new subscriptionAdapter(name, this);
    }

    async exists() {
        await this.create();
        return [true];
    }

    async create(...props) {
        const channel = await this.#getChannel();
        try {
            channel.assertQueue(this.name, {
                durable: true
            });
            channel.assertExchange(this.name, this.#exchangeType, {
                durable: true,
                ...props
            });
            return channel.bindQueue(this.name, this.name);
        } finally {
            this.#pool.release(channel);
        }
       
    }

    async publish(...props) {
        const channel = await this.#getChannel();
        try {
            return channel.publish(
                this.name,
                '',
                ...props
            );
        } finally {
            await this.#pool.release(channel);
        }
    }
}

/**
 * Publisher Subscriber client adapter for AMQP
 *
 * Required env
 * - RABBITMQ_URL || AMQP_URL
 * 
 * Not required env
 * 
 * - MQ_CLIENT_RECONNECT_TIMEOUT
 * - MQ_CLIENT_MAX_POOL_SIZE
 * - MQ_CLIENT_MIN_POOL_SIZE
 */
class clientAdapter {
    /**
     * @type function
     */
    #getChannel;
    #connection;
    #reconnectTimeout;
    #channelPool;

    constructor(...props) {
        this.openConnection();
    }

    async openConnection() {
        try {

            this.#connection = await amqplib.connect(process.env.RABBITMQ_URL || process.env.AMQP_URL);
            this.#connection.on('error', (err) => {
                console.error(`AMQP client connection error`, err);
                this.#connection = null;
                this.reconnect();
            })

            this.#connection.on('close', () => {
                console.log(`AMQP client closed connection`, err);
                this.#connection = null;
                this.reconnect();
            })

            const factory = {
                create: async () => {
                    const channel = await this.#connection.createChannel();
                    channel.on('close', () => console.log('AMQP client channel closed'));
                    channel.on('error', () => console.error('AMQP client channel error', err));

                    return channel;
                },
                destroy: async (channel) => {
                    await channel.close();
                }
            }

            this.#channelPool = genericPool.createPool(factory, {
                max: process.env.MQ_CLIENT_MAX_POOL_SIZE || 10,
                min: process.env.MQ_CLIENT_MIN_POOL_SIZE || 2
            });

            this.#getChannel = async () => {
                return this.#channelPool.acquire();
            }

            clearTimeout(this.#reconnectTimeout);
            console.log('AMQP client connected');

        } catch (err) {
            console.log(`Error connection to AMQP client`, err);
            this.reconnect();
        }
    }

    reconnect() {
        clearTimeout(this.#reconnectTimeout);
        this.#reconnectTimeout = setTimeout(() => this.openConnection(), process.env.MQ_CLIENT_RECONNECT_TIMEOUT || 1000);
    }

    topic(name) {
        return new topicAdapter(name, this.#getChannel, this.#channelPool);
    }

    createSubscription(...props) {
        // todo implement creation and configuration
        throw Error('Method is not implemented');
    }
}

module.exports = {
    clientAdapter
}
