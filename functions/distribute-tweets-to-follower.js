import {
  BatchWriteItemCommand,
  DynamoDBClient,
  GetItemCommand,
  QueryCommand,
} from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import _ from 'lodash'

const { TWEETS_TABLE_NAME, TIMELINES_TABLE_NAME, MAX_TWEETS } = process.env;
const MaxTweets = +MAX_TWEETS;

const MAX_BATCH_SIZE = 10;

const client = new DynamoDBClient();

export const handler = async (event) => {
  for (const record of event.Records) {
    if (record.eventName === 'INSERT') {
      const relationship = unmarshall(record.dynamodb.NewImage);
      const [relType] = relationship.sk.split('_');
      if (relType === 'FOLLOWS') {
        const tweets = await getTweets(relationship.otherUserId);
        console.log('tweets', tweets)
        await distribute(tweets, relationship.userId);
      }
    } else if (record.eventName === 'REMOVE') {
      const relationship = unmarshall(record.dynamodb.OldImage);
      const [relType] = relationship.sk.split('_');
      if (relType === 'FOLLOWS') {
        const tweets = await getTimelineEntriesBy(
          relationship.otherUserId,
          relationship.userId
        );
        await undistribute(tweets, relationship.userId);
      }
    }
  }
};

async function getTweets(userId) {
  const loop = async (acc, exclusiveStartKey) => {
    const resp = await client.send(
      new QueryCommand({
        TableName: TWEETS_TABLE_NAME,
        KeyConditionExpression: 'creator = :userId',
        ExpressionAttributeValues: marshall({
          ':userId': userId,
        }),
        IndexName: 'byCreator',
        ExclusiveStartKey: exclusiveStartKey,
      })
    );

    const tweets = (resp.Items || []).map((x) => unmarshall(x));
    console.log('getTweet:tweets::', tweets);
    const newAcc = acc.concat(tweets);

    if (resp.LastEvaluatedKey && newAcc.length < MaxTweets) {
      return await loop(newAcc, resp.LastEvaluatedKey);
    } else {
      return newAcc;
    }
  };

  return await loop([]);
}

async function getTimelineEntriesBy(distributedFrom, userId) {
  const loop = async (acc, exclusiveStartKey) => {
    const resp = await client.send(
      new QueryCommand({
        TableName: TIMELINES_TABLE_NAME,
        KeyConditionExpression:
          'userId = :userId AND distributedFrom = :distributedFrom',
        ExpressionAttributeValues: {
          ':userId': userId,
          ':distributedFrom': distributedFrom,
        },
        IndexName: 'byDistributedFrom',
        ExclusiveStartKey: exclusiveStartKey,
      })
    );

    const tweets = resp.Items || [];
    const newAcc = acc.concat(tweets);

    if (resp.LastEvaluatedKey) {
      return await loop(newAcc, resp.LastEvaluatedKey);
    } else {
      return newAcc;
    }
  };

  return await loop([]);
}

async function distribute(tweets, userId) {
  const timelineEntries = tweets.map((tweet) => ({
    PutRequest: {
      Item: marshall(
        {
          userId,
          tweetId: tweet.id,
          timestamp: tweet.createdAt,
          distributedFrom: tweet.creator,
          retweetOf: tweet.retweetOf,
          inReplyToTweetId: tweet.inReplyToTweetId,
          inReplyToUserIds: tweet.inReplyToUserIds,
        },
        {
          removeUndefinedValues: true,
        }
      ),
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

  console.log(
    'promises', promises
  )

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
