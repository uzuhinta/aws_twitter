import { DynamoDBClient, GetItemCommand } from '@aws-sdk/client-dynamodb';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import { SignatureV4 } from '@aws-sdk/signature-v4';
import { Sha256 } from '@aws-sdk/crypto-sha256-js';
import { ulid } from 'ulid';
import { defaultProvider } from '@aws-sdk/credential-provider-node';

const { GRAPHQL_API_URL, TWEETS_TABLE_NAME, AWS_REGION } = process.env;

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
      const like = unmarshall(record.dynamodb.NewImage);
      console.log('like', like);

      await notifyLiked(like);
    }
  }
};

async function notifyLiked(like) {
  let tweet = await getTweetById(like.tweetId);
  tweet = unmarshall(tweet);
  console.log('tweet', tweet);
  try {
    const response = await makeSignedAppSyncRequest(
      `mutation notifyLiked(
        $id: ID!
        $userId: ID!
        $tweetId: ID!
        $likedBy: ID!
      ) {
        notifyLiked(
          id: $id
          userId: $userId
          tweetId: $tweetId
          likedBy: $likedBy
        ) {
          __typename
          ... on Liked {
            id
            type
            userId
            tweetId
            likedBy
            createdAt
          }
        }
      }`,
      {
        id: ulid(),
        userId: tweet.creator,
        tweetId: tweet.id,
        likedBy: like.userId,
      }
    );

    console.log(response);
  } catch (error) {
    console.log(error);
  }
}

async function getTweetById(tweetId) {
  console.log('tweetId', tweetId);
  const res = await client.send(
    new GetItemCommand({
      TableName: TWEETS_TABLE_NAME,
      Key: {
        id: { S: tweetId },
      },
    })
  );

  console.log('res', res);

  return res.Item;
}
