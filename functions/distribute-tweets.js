import {
  BatchWriteItemCommand,
  DynamoDBClient,
  GetItemCommand,
  QueryCommand,
} from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import _ from 'lodash'

const { RELATIONSHIPS_TABLE_NAME, TIMELINES_TABLE_NAME } = process.env;

const MAX_BATCH_SIZE = 10;

const client = new DynamoDBClient();

export const handler = async (event) => {
  console.log('event', event);
  for (const record of event.Records) {
    if (record.eventName === 'INSERT') {
      const tweet = unmarshall(record.dynamodb.NewImage);
      const follower = await getFollowers(tweet.creator);
      console.log('follower', follower);
      await distribute(tweet, follower);
    } else if (record.eventName === 'REMOVE') {
      const tweet = unmarshall(record.dynamodb.OldImage);
      const follower = await getFollowers(tweet.creator);
      await undistribute(tweet, follower);
    }
  }
};

async function getFollowers(userId) {
  const loop = async (acc, exclusiveStartKey) => {
    const resp = await client.send(
      new QueryCommand({
        TableName: RELATIONSHIPS_TABLE_NAME,
        KeyConditionExpression:
          'otherUserId = :otherUserId and begins_with(sk, :follows)',
        ExpressionAttributeValues: marshall({
          ':otherUserId': userId,
          ':follows': 'FOLLOWS_',
        }),
        IndexName: 'byOtherUser',
        ExclusiveStartKey: exclusiveStartKey,
      })
    );

    console.log('getFollowers:::resp', resp);

    const userIds = (resp.Items || []).map((x) => unmarshall(x).userId);

    console.log('getFolow::userIds', userIds);

    if (resp.LastEvaluatedKey) {
      return await loop(acc.concat(userIds), resp.LastEvaluatedKey);
    } else {
      return acc.concat(userIds);
    }
  };

  return await loop([]);
}

async function distribute(tweet, followers) {
  console.log('distribute:::tweet', tweet);
  console.log('distribute:::followers', followers);
  const timelineEntries = followers.map((userId) => ({
    PutRequest: {
      Item: marshall({
        userId,
        tweetId: tweet.id,
        timestamp: tweet.createdAt,
        distributedFrom: tweet.creator,
        retweetOf: tweet.retweetOf,
        inReplyToTweetId: tweet.inReplyToTweetId,
        inReplyToUserIds: tweet.inReplyToUserIds,
      }, {
        removeUndefinedValues: true,
      }),
    },
  }));

  console.log('timelineEntries', timelineEntries);

  const chunks = _.chunk(timelineEntries, MAX_BATCH_SIZE);

  const promises = chunks.map(async (chunk) => {
    console.log('chunks', JSON.stringify(chunks));
    await client.send(
      new BatchWriteItemCommand({
        RequestItems: {
          [TIMELINES_TABLE_NAME]: chunk,
        },
      })
    );
  });

  console.log('promises', promises);

  await Promise.all(promises);
}

async function undistribute(tweet, followers) {
  const timelineEntries = followers.map((userId) => ({
    DeleteRequest: {
      Key: marshall({
        userId,
        tweetId: tweet.id,
      }),
    },
  }));

  const chunks = _.chunk(timelineEntries, MAX_BATCH_SIZE);

  const promises = chunks.map(async (chunk) => {
    await client.send(
      new BatchWriteItemCommand({
        RequestItems: {
          [TIMELINES_TABLE_NAME]: chunk,
        },
      })
    );
  });

  await Promise.all(promises);
}
