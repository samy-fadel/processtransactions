const { PubSub } = require('@google-cloud/pubsub');
const Web3 = require('web3');
const { SecretManagerServiceClient } = require('@google-cloud/secret-manager');
const { v1 } = require('@google-cloud/pubsub');

const projectId = process.env.PROJECT_ID;
const subscriptionName = 'transactions-topic-sub';
const smartContractsTopicName = 'smart-contracts-transactions';

const client = new v1.SubscriberClient();
const pubsub = new PubSub({ projectId });

// Function to retrieve the API key from Secret Manager
async function getApiKey() {
  const secretName = `projects/${process.env.PROJECT_NUMBER}/secrets/web3-api-key/versions/latest`;
  const client = new SecretManagerServiceClient();
  const [version] = await client.accessSecretVersion({ name: secretName });
  return version.payload.data.toString();
}

async function publishSmartContractAddress(address) {
  const data = Buffer.from(JSON.stringify({ address }));
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
  const apiKey = await getApiKey();
  const web3 = new Web3(`https://mainnet.infura.io/v3/${apiKey}`);
  let continueProcessing = true;

  while (continueProcessing) {
    try {
      const request = {
        subscription: client.subscriptionPath(process.env.PROJECT_ID, subscriptionName),
        maxMessages: 50, // Process messages in batches of 50
      };

      const [response] = await client.pull(request);
      const messages = response.receivedMessages;

      if (messages && messages.length > 0) {
        for (const message of messages) {
          const transaction = JSON.parse(message.message.data.toString());
          console.log("Transaction:", transaction);

          // Get the transaction receipt to check if it's a contract creation
          const receipt = await web3.eth.getTransactionReceipt(transaction);
          const isSmartContractCreation = receipt.contractAddress !== null;

          if (isSmartContractCreation) {
            // If it's a smart contract creation, publish the contract address as an individual message
            await publishSmartContractAddress(receipt.contractAddress);
          } else {
            console.log("Not a smart contract creation transaction.");
          }
        }

        const ackRequest = {
          subscription: request.subscription,
          ackIds: messages.map((msg) => msg.ackId),
        };

        await client.acknowledge(ackRequest);
      } else {
        console.log('No more messages to process. Exiting loop.');
        continueProcessing = false;
      }
    } catch (error) {
      console.error('Error retrieving transactions:', error);
      continueProcessing = false;
    }
  }
}

retrieveTransactions();