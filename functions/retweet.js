import {
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
  TransactWriteItemsCommand,
} from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';
import { ulid } from 'ulid';

const client = new DynamoDBClient();
const {
  USERS_TABLE_NAME,
  TWEETS_TABLE_NAME,
  RETWEETS_TABLE_NAME,
  TIMELINES_TABLE_NAME,
} = process.env;
export const handler = async (event) => {
  console.log('EVENT: \n' + JSON.stringify(event, null, 2));

  const { tweetId } = event.arguments;
  const { username } = event.identity;
  const id = ulid();
  const timestamp = new Date().toJSON();

  const tweetRes = await client.send(
    new GetItemCommand({
      TableName: TWEETS_TABLE_NAME,
      Key: {
        id: { S: tweetId },
      },
    })
  );

  const storedTweet = tweetRes.Item;

  if (!storedTweet) throw new Error('Not found');

  const tweet = {
    __typename: 'Retweet',
    id,
    createdAt: timestamp,
    creator: username,
    retweetOf: tweetId,
  };

  console.log('tweet: \n' + JSON.stringify(tweet, null, 2));

  const transactItems = [
    {
      Put: {
        TableName: TWEETS_TABLE_NAME,
        Item: marshall(tweet),
      },
    },
    {
      Put: {
        TableName: RETWEETS_TABLE_NAME,
        Item: marshall({
          userId: username,
          tweetId,
          createdAt: timestamp,
        }),
        ConditionExpression: 'attribute_not_exists(tweetId)',
      },
    },
    {
      Update: {
        TableName: TWEETS_TABLE_NAME,
        Key: marshall({
          id: tweetId,
        }),
        UpdateExpression: 'ADD retweets :one',
        ConditionExpression: 'attribute_exists(id)',
        ExpressionAttributeValues: marshall({
          ':one': 1,
        }),
      },
    },
    {
      Update: {
        TableName: USERS_TABLE_NAME,
        Key: marshall({
          id: username,
        }),
        UpdateExpression: 'ADD tweetsCount :one',
        ConditionExpression: 'attribute_exists(id)',
        ExpressionAttributeValues: marshall({
          ':one': 1,
        }),
      },
    },
  ];

  if (tweet.creator !== username) {
    transactItems.push({
      Put: {
        TableName: TIMELINES_TABLE_NAME,
        Item: marshall({
          userId: username,
          tweetId: id,
          retweetOf: tweetId,
          timestamp,
        }),
      },
    });
  }

  await client.send(
    new TransactWriteItemsCommand({
      TransactItems: transactItems,
    })
  );

  return true;
};
