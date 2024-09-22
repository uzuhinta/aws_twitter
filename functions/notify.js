import { DynamoDBClient, GetItemCommand } from '@aws-sdk/client-dynamodb';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import { SignatureV4 } from '@aws-sdk/signature-v4';
import { createRequest } from '@aws-sdk/util-create-request';
import { Sha256 } from '@aws-sdk/crypto-sha256-js';
import gql from 'graphql-tag';
import { ulid } from 'ulid';
import { defaultProvider } from '@aws-sdk/credential-provider-node';

const {
  GRAPHQL_API_URL,
  TWEETS_TABLE_NAME,
  USERS_TABLE_NAME,
  AWS_REGION,
  AWS_ACCESS_KEY_ID,
  AWS_SECRET_ACCESS_KEY,
} = process.env;

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
      const tweet = unmarshall(record.dynamodb.NewImage);
      console.log('tweet', tweet);

      switch (tweet.__typename) {
        case 'Retweet':
          await notifyRetweet(tweet);
          break;
        // case TweetTypes.REPLY:
        //   await notifyReply(tweet.inReplyToUserIds, tweet)
        //   break
      }
    }
  }
};

async function notifyRetweet(tweet) {
  let retweetOf = await getTweetById(tweet.retweetOf);
  retweetOf = unmarshall(retweetOf);
  console.log('retweetOf', retweetOf);
  try {
    const response = await makeSignedAppSyncRequest(
      `
        mutation notifyRetweeted(
          $id: ID!
          $userId: ID!
          $tweetId: ID!
          $retweetedBy: ID!
          $retweetId: ID!
        ) {
          notifyRetweeted(
            id: $id
            userId: $userId
            tweetId: $tweetId
            retweetedBy: $retweetedBy
            retweetId: $retweetId
          ) {
            __typename
            ... on Retweeted {
              id
              type
              userId
              tweetId
              retweetedBy
              retweetId
              createdAt
            }
          }
        }
      `,
      {
        id: ulid(),
        userId: retweetOf.creator,
        tweetId: tweet.retweetOf,
        retweetId: tweet.id,
        retweetedBy: tweet.creator,
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
