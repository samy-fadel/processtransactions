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
    console.log("line 14 ", messages);

    if (messages && messages.length > 0) {
      console.log("line 17 in if ");
      const message = messages[0].message;
      const transaction = JSON.parse(message.data.toString());
      console.log("line 20 ", transaction);

      // Check if the transaction is a smart contract creation transaction
      const isSmartContractCreation = !transaction.to || transaction.to.trim() === '';
      console.log("line 24 ", isSmartContractCreation);


      if (isSmartContractCreation) {
        console.log("line 28 ");
        // If it's a smart contract creation, publish it to the topic
        await publishTransaction(transaction);
      } else {
        console.log("Not a smart contract creation transaction.");
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