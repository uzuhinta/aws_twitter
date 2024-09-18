import {
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
  TransactWriteItemsCommand,
} from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';
import { ulid } from 'ulid';

const client = new DynamoDBClient();
const { USERS_TABLE_NAME, TWEETS_TABLE_NAME, TIMELINES_TABLE_NAME } =
  process.env;

const getTweetById = async (tweetId) => {
  const res = await client.send(
    new GetItemCommand({
      TableName: TWEETS_TABLE_NAME,
      Key: {
        id: { S: tweetId },
      },
    })
  );

  return res.Item
};

export { getTweetById };
