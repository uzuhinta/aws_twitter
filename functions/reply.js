import {
  DynamoDBClient,
  GetItemCommand,
  TransactWriteItemsCommand,
} from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';
import _ from 'lodash';
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

  return res.Item;
};

export const handler = async (event) => {
  console.log('EVENT: \n' + JSON.stringify(event, null, 2));

  const { tweetId, text } = event.arguments;
  const { username } = event.identity;
  const id = ulid();
  const timestamp = new Date().toJSON();

  const tweet = await getTweetById(tweetId);

  if (!tweet) throw new Error('Not found');

  const inReplyToUserIds = await getUserIdsToReplyTo(tweet);

  const newTweet = {
    __typename: 'Reply',
    id,
    creator: username,
    createdAt: timestamp,
    inReplyToTweetId: tweetId,
    inReplyToUserIds,
    text,
    replies: 0,
    likes: 0,
    retweets: 0,
  };

  console.log('tweet: \n' + JSON.stringify(tweet, null, 2));

  const transactItems = [
    {
      Put: {
        TableName: TWEETS_TABLE_NAME,
        Item: marshall(newTweet),
      },
    },
    {
      Update: {
        TableName: TWEETS_TABLE_NAME,
        Key: marshall({
          id: tweetId,
        }),
        UpdateExpression: 'ADD replies :one',
        ConditionExpression: 'attribute_exists(id)',
        ExpressionAttributeValues: marshall({
          ':one': $util.dynamodb.toDynamoDBJson(1),
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
          ':one': $util.dynamodb.toDynamoDBJson(1),
        }),
      },
    },
    {
      Put: {
        TableName: TIMELINES_TABLE_NAME,
        Item: marshall({
          userId: username,
          tweetId: id,
          inReplyToTweetId: tweetId,
          inReplyToUserIds,
          timestamp,
        }),
      },
    },
  ];

  await client.send(
    new TransactWriteItemsCommand({
      TransactItems: transactItems,
    })
  );

  return newTweet;
};

async function getUserIdsToReplyTo(tweet) {
  let userIds = [tweet.creator];
  if (tweet.__typename === 'Reply') {
    userIds = userIds.concat(tweet.inReplyToUserIds);
  } else if (tweet.__typename === 'Retweet') {
    const retweetOf = await getTweetById(tweet.retweetOf);
    userIds = userIds.concat(await getUserIdsToReplyTo(retweetOf));
  }

  return _.uniq(userIds);
}
