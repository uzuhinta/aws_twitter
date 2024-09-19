import {
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
  QueryCommand,
  TransactWriteItemsCommand,
} from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';
import _ from 'lodash';

const client = new DynamoDBClient();
const {
  USERS_TABLE_NAME,
  TWEETS_TABLE_NAME,
  RETWEETS_TABLE_NAME,
  TIMELINES_TABLE_NAME,
} = process.env;
export const handler = async (event) => {
  try {
    console.log('EVENT: \n' + JSON.stringify(event, null, 2));

    const { tweetId } = event.arguments;
    const { username } = event.identity;

    const tweetRes = await client.send(
      new GetItemCommand({
        TableName: TWEETS_TABLE_NAME,
        Key: {
          id: { S: tweetId },
        },
      })
    );

    const storedTweet = tweetRes.Item;

    if (!storedTweet) throw new Error('Tweet Not found');

    const queryRes = await client.send(
      new QueryCommand({
        TableName: TWEETS_TABLE_NAME,
        IndexName: 'retweetsByCreator',
        KeyConditionExpression: 'creator = :creator AND retweetOf = :tweetId',
        ExpressionAttributeValues: {
          ':creator': username,
          ':tweetId': tweetId,
        },
        Limit: 1,
      })
    );

    const retweet = _.get(queryRes, 'Items.0');

    if (!retweet) throw new Error('Retweet is not found!');

    console.log('tweet: \n' + JSON.stringify(retweet, null, 2));

    const transactItems = [
      {
        Delete: {
          TableName: TWEETS_TABLE_NAME,
          Key: marshall({ id: retweet.id }),
          ConditionExpression: 'attribute_exists(id)',
        },
      },
      {
        Delete: {
          TableName: RETWEETS_TABLE_NAME,
          Key: marshall({
            userId: username,
            tweetId,
          }),
          ConditionExpression: 'attribute_exists(tweetId)',
        },
      },
      {
        Update: {
          TableName: TWEETS_TABLE_NAME,
          Key: marshall({
            id: tweetId,
          }),
          UpdateExpression: 'ADD retweets :minusOne',
          ConditionExpression: 'attribute_exists(id)',
          ExpressionAttributeValues: marshall({
            ':minusOne': $util.dynamodb.toDynamoDBJson(-1),
          }),
        },
      },
      {
        Update: {
          TableName: USERS_TABLE_NAME,
          Key: marshall({
            id: username,
          }),
          UpdateExpression: 'ADD tweetsCount :minusOne',
          ConditionExpression: 'attribute_exists(id)',
          ExpressionAttributeValues: marshall({
            ':minusOne': $util.dynamodb.toDynamoDBJson(-1),
          }),
        },
      },
    ];

    if (storedTweet.creator !== username) {
      transactItems.push({
        Delete: {
          TableName: TIMELINES_TABLE_NAME,
          Key: marshall({
            userId: username,
            tweetId: retweet.id,
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
  } catch (error) {
    console.error('Error executing transaction: ', error);
    throw new Error('Transaction failed');
  }
};
