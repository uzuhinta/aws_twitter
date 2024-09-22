import { DynamoDBClient, GetItemCommand } from '@aws-sdk/client-dynamodb';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import { SignatureV4 } from '@aws-sdk/signature-v4';
import { Sha256 } from '@aws-sdk/crypto-sha256-js';
import { ulid } from 'ulid';
import { defaultProvider } from '@aws-sdk/credential-provider-node';

const { GRAPHQL_API_URL, AWS_REGION } = process.env;

const client = new DynamoDBClient();

const signer = new SignatureV4({
  credentials: defaultProvider(),
  region: AWS_REGION,
  service: 'appsync',
  sha256: Sha256,
});

async function makeSignedAppSyncRequest(query, msg) {
  const headers = {
    'Content-Type': 'application/json',
    host: new URL(GRAPHQL_API_URL).hostname,
  };
  const body = JSON.stringify({
    query,
    variables: JSON.stringify(msg),
  });
  console.log('body', body);
  const method = 'POST';
  let res;

  const request = {
    method,
    headers,
    protocol: 'https:',
    hostname: new URL(GRAPHQL_API_URL).hostname,
    path: '/graphql',
    body,
    region: AWS_REGION,
    service: 'appsync',
  };
  const signedRequest = await signer.sign(request, {
    signingDate: new Date(),
  });
  res = await fetch(GRAPHQL_API_URL, {
    method: signedRequest.method,
    headers: signedRequest.headers,
    body: signedRequest.body,
  });
  const data = await res.json();
  console.log('appsync send message successfully:: ', data);

  return data;
}

export const handler = async (event) => {
  console.log('event', event);
  for (const record of event.Records) {
    if (record.eventName === 'INSERT') {
      const dm = unmarshall(record.dynamodb.NewImage);
      console.log('dm', dm);

      await notifyDMed(dm);
    }
  }
};

async function notifyDMed(dm) {
  const userIds = dm.conversationId.split('_')
  const userId = userIds.filter(x => x != dm.from)[0]

  try {
    const response = await makeSignedAppSyncRequest(`mutation notifyDMed(
      $id: ID!
      $userId: ID!
      $otherUserId: ID!
      $message: String!
    ) {
      notifyDMed(
        id: $id
        userId: $userId
        otherUserId: $otherUserId
        message: $message
      ) {
        __typename
        ... on DMed {
          id
          type
          userId
          createdAt
          otherUserId
          message
        }
      }
    }`, {
      id: ulid(),
      userId: userId,
      otherUserId: dm.from,
      message: dm.message
    });

    console.log(response);
  } catch (error) {
    console.log(error);
  }
}
