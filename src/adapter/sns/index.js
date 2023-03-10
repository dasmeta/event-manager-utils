const { SNSClient, PublishCommand } = require("@aws-sdk/client-sns");
const { STSClient, GetCallerIdentityCommand } = require("@aws-sdk/client-sts");

class subscriptionAdapter {
    mqClient;

    constructor(subscription) {
        return this.subscription = subscription;
    }

    exists(...props) {
        return this.subscription.exists(...props);
    }

    create() {
        const topic = this.subscription.topic;
        const subscriptionName = this.subscription.name;
        
        return mqClient.createSubscription(topic, subscriptionName, {
            flowControl: {
                maxMessages: 1,
            },
            ackDeadlineSeconds: 60, // max 10 min
            // messageRetentionDuration: 4 * 60 * 60, // max 7 day
            // retainAckedMessages: true,
        });
    }

    getClientAdapter() {
        if(!this.mqClient) {
            this.mqClient = new clientAdapter();
        }
        return this.mqClient;
    }
}

class topicAdapter {
    constructor(topic, client) {
        this.topic = topic;
        this.client = client;
        this.stsClient = new STSClient({ region: process.env.AWS_REGION });
    }

    exists(...props) {
        return [true];
    }

    create(...props) {
        return true;
    }

    // subscription(...props) {
    //     return this.topic.subscription(...props)
    // }

    subscription(...props) {
        return new subscriptionAdapter(this.topic.subscription(...props));
    }

    async publish(message) {

        const data = await this.stsClient.send(new GetCallerIdentityCommand());
        const command = new PublishCommand({
            Message: message,
            TopicArn: `arn:aws:sns:${process.env.AWS_REGION}:${data['Account']}:${this.topic}`
        })
        
        const result = await this.client.send(command);
        return result['MessageId'];
    }
}

/**
 * SNS client adapter for AWS
 *
 * Required env
 * - AWS_REGION
 * - AWS_ACCESS_KEY_ID
 * - AWS_SECRET_ACCESS_KEY
 */
class clientAdapter {
    constructor(...props) {
        this.client = new SNSClient({ region: process.env.AWS_REGION });
    }

    topic(name) {
        return new topicAdapter(name, this.client);
    }

    createSubscription(topic, ...props) {
        return this.client.createSubscription(topic.topic, ...props)
    }
}

module.exports = {
    clientAdapter
}