const EventEmitter = require("events");
const logger = require("./logger");
const { clientFactory } = require("./adapter/clientFactory");

const mqClient = clientFactory.createClient();

const topicsMap = new Map();
const topicsWaiting = new Map();

class TopicEmitter extends EventEmitter {}
const topicEmitter = new TopicEmitter();

const subscriptionHandler = (topic, subscription, handler, maxAttempts = 5) => async subscriptionObject => {
    const onMessage = message => {
        const { eventId, traceId, data, subscription: eventSubscription = "", dataSource } = JSON.parse(message.data.toString());
        const context = { topic, subscription, traceId, dataSource };

        if (logger.isDebug()) {
            logger.debug(`RECEIVE eventId: ${eventId}, topic: ${topic}, subscription: ${subscription}`);
        }
        if (eventSubscription && subscription !== eventSubscription) {
            if (logger.isDebug()) {
                logger.debug(`SKIP event subscription: "${eventSubscription}", current subscription: ${subscription}`);
            }
            message.ack();
            return;
        }
        const fulfilled = async () => {
            if (logger.isDebug() && eventId) {
                logger.timeEnd(`EXEC eventId: ${eventId}, topic: ${topic}, subscription: ${subscription}`);
            }
            if (logger.isDebug()) {
                logger.debug("EXEC SUCCESS");
            }
            if (eventId) {
                await store.recordSuccess({ topic, subscription, eventId, traceId });
            }
            if (logger.isDebug()) {
                logger.debug(`ACK Before eventId: ${eventId}, topic: ${topic}, subscription: ${subscription}`);
            }
            message.ack();

            if (logger.isDebug()) {
                logger.debug(
                    `ACK After. Subscription Collection updated eventId: ${eventId}, topic: ${topic}, subscription: ${subscription}`
                );
            }
        };
        const rejected = async err => {
            logger.error("HANDLE ERROR", err, { topic, subscription, data, eventId });
            bugsnag.notify(err, { topic, subscription, data, eventId });
            if (logger.isDebug() && eventId) {
                logger.timeEnd(`EXEC eventId: ${eventId}, topic: ${topic}, subscription: ${subscription}`);
            }
            if (logger.isDebug()) {
                logger.error("EXEC FAIL", { topic, subscription });
            }

            if (eventId) {
                if (err.message.includes("PreconditionFailedError")) {
                    if ((await store.hasReachedMaxAttempts({ topic, subscription, eventId, maxAttempts }))) {
                        await store.recordFailure({ topic, subscription, eventId, traceId, error: err });
                    } else {
                        await store.recordPreconditionFailure({ topic, subscription, eventId, traceId });
                    }
                } else {
                    await store.recordFailure({ topic, subscription, eventId, traceId, error: err });
                }
            }
            if (logger.isDebug()) {
                logger.debug("Subscription Collection updated");
            }
            if (! err.message.includes("PreconditionFailedError")) {
                message.ack();
            }
        };

        if (logger.isDebug() && eventId) {
            logger.timeStart(`EXEC eventId: ${eventId}, topic: ${topic}, subscription: ${subscription}`);
        }
        if (eventId) {
            store
                .recordStart({ topic, subscription, eventId })
                .then(() => {
                    handler(data, context).then(fulfilled, rejected);
                })
                .catch(err => {
                    logger.error("ERROR: Can not store record start, it is probably issue with mongodb", err);
                    bugsnag.notify(err, { topic, subscription, eventId });
                });
        } else {
            handler(data, context).then(fulfilled, rejected);
        }
    };

    const onError = err => {
        logger.error("SUBSCRIPTION ERROR", err, { topic, subscription });
        bugsnag.notify(err, { topic, subscription });
        // process.exit(1);
    };

    subscriptionObject.onMessage = onMessage;
    subscriptionObject.onError = onError;
    subscriptionObject.on("message", onMessage);
    subscriptionObject.on("error", onError);
};

async function getTopic(topicName) {
    if (topicsMap.has(topicName)) {
        return mqClient.topic(topicName);
    }

    if (topicsWaiting.has(topicName)) {
        return new Promise(resolve => {
            topicEmitter.setMaxListeners(topicEmitter.getMaxListeners() + 1);
            topicEmitter.once(topicName, () => {
                resolve(mqClient.topic(topicName));
                topicEmitter.setMaxListeners(Math.max(topicEmitter.getMaxListeners() - 1, 0));
            });
        });
    }
    topicsWaiting.set(topicName, true);
    const topic = mqClient.topic(topicName);
    const [exists] = await topic.exists();
    if (!exists) {
        if (logger.isDebug()) {
            logger.debug(`TOPIC "${topicName}" NOT EXISTS, creating...`);
            logger.timeStart(`TOPIC CREATED "${topicName}"`);
        }
        await topic.create();
        if (logger.isDebug()) {
            logger.timeEnd(`TOPIC CREATED "${topicName}"`);
        }
    }
    topicsMap.set(topicName, true);
    topicsWaiting.delete(topicName);
    topicEmitter.emit(topicName);
    return mqClient.topic(topicName);
}

async function getSubscription(topicName, subscriptionName) {
    const topic = await getTopic(topicName);
    const subscription = topic.subscription(subscriptionName);
    const [exists] = await subscription.exists();
    if (exists) {
        return subscription;
    }

    if (logger.isDebug()) {
        logger.debug(`SUBSCRIPTION "${subscriptionName}" NOT EXISTS, creating...`);
        logger.timeStart(`SUBSCRIPTION CREATED "${subscriptionName}"`);
    }
    await subscription.create();

    if (logger.isDebug()) {
        logger.timeEnd(`SUBSCRIPTION CREATED "${subscriptionName}"`);
    }

    return topic.subscription(subscriptionName);
}

async function registerSubscriber(topic, subscriptionName, handler, maxAttempts) {
    getSubscription(topic, subscriptionName)
        .then(subscriptionHandler(topic, subscriptionName, handler, maxAttempts))
        .catch(err => {
            logger.error(`GET "${subscriptionName}" SUBSCRIPTION ERROR`, err, { topic, subscriptionName });
            process.exit(1);
        });
}

module.exports = {
    getTopic,
    getSubscription,
    registerSubscriber
};