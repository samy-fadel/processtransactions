const { PubSub } = require('@google-cloud/pubsub');
const Web3 = require('web3');
const { SecretManagerServiceClient } = require('@google-cloud/secret-manager');
const { v1 } = require('@google-cloud/pubsub');

const projectId = process.env.PROJECT_ID;
const subscriptionName = 'transactions-topic-sub';
const smartContractsTopicName = 'smart-contracts-transactions';

const client = new v1.SubscriberClient();
const pubsub = new PubSub({ projectId });

console.log("line 13 ");

// Function to retrieve the API key from Secret Manager
async function getApiKey() {
  const secretName = `projects/${process.env.PROJECT_NUMBER}/secrets/web3-api-key/versions/latest`;
  const client = new SecretManagerServiceClient();
  const [version] = await client.accessSecretVersion({ name: secretName });
  return version.payload.data.toString();
}

async function publishTransaction(transaction) {
  console.log("line 24 publishTransaction ");
  const data = Buffer.from(JSON.stringify(transaction));
  await pubsub.topic(smartContractsTopicName).publish(data);
}

async function handleError(message) {
  try {
    const data = Buffer.from(JSON.stringify(message));
    await pubsub.topic(subscriptionName).publish(data);
    console.log('Message put back in Pub/Sub subscription:', message);
  } catch (error) {
    console.error('Error handling error:', error);
  }
}

async function retrieveTransactions() {
  console.log("line 40 retrieveTransactions ");
  const apiKey = await getApiKey();
  const web3 = new Web3(`https://mainnet.infura.io/v3/${apiKey}`);

  try {
    const request = {
      subscription: client.subscriptionPath(process.env.PROJECT_ID, subscriptionName),
      maxMessages: 1,
    };
    console.log("line 49 try retrieveTransactions ");
    const [response] = await client.pull(request);
    const messages = response.receivedMessages;

    if (messages && messages.length > 0) {
      const transactionMessages = messages.map((msg) => JSON.parse(msg.message.data.toString()));
      const transactions = transactionMessages.map((transactionMsg) => transactionMsg.transaction);

      const sortedTransactions = transactions.filter((transaction) => {
        // Check if the transaction is a smart contract creation transaction
        return transaction && (!transaction.to || transaction.to.trim() === '');
      });

      for (const transaction of sortedTransactions) {
        console.log("line 59 ", sortedTransactions);
        await publishTransaction(transaction);
      }

      const ackRequest = {
        subscription: request.subscription,
        ackIds: messages.map((msg) => msg.ackId),
      };

      await client.acknowledge(ackRequest);
    } else {
      console.log('No messages received from Pub/Sub subscription');
    }
  } catch (error) {
    console.error('line 77 Error retrieving transactions:', error);
    if (messages && messages.length > 0) {
      const ackRequest = {
        subscription: request.subscription,
        ackIds: messages.map((msg) => msg.ackId),
      };
      await client.acknowledge(ackRequest);

      for (const message of messages) {
        await handleError(message);
      }
    }
  }
}

retrieveTransactions();