import {
  DynamoDBClient,
  PutItemCommand,
  TransactWriteItemsCommand,
} from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';
import { ulid } from 'ulid';

const client = new DynamoDBClient();
const { USERS_TABLE_NAME, TWEETS_TABLE_NAME, TIMELINES_TABLE_NAME } =
  process.env;
export const handler = async (event) => {
  console.log('EVENT: \n' + JSON.stringify(event, null, 2));

  const { text } = event.arguments;
  const { username } = event.identity;
  const id = ulid();
  const timestamp = new Date().toJSON();
  const hashTags = extractHashTags(text);

  const tweet = {
    __typename: 'Tweet',
    id,
    createdAt: timestamp,
    text,
    creator: username,
    replies: 0,
    likes: 0,
    retweets: 0,
    hashTags,
  };

  console.log('tweet: \n' + JSON.stringify(tweet, null, 2));

  await client.send(
    new TransactWriteItemsCommand({
      TransactItems: [
        {
          Put: {
            TableName: TWEETS_TABLE_NAME,
            Item: marshall(tweet),
          },
        },
        {
          Put: {
            TableName: TIMELINES_TABLE_NAME,
            Item: marshall({
              userId: username,
              tweetId: id,
              timestamp,
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
      ],
    })
  );

  return tweet;
};

function extractHashTags(text) {
  const hashTags = new Set();
  const regex = /(\#[a-zA-Z0-9_]+\b)/gm;
  let m;

  while ((m = regex.exec(text)) !== null) {
    // This is necessary to avoid infinite loops with zero-width matches
    if (m.index === regex.lastIndex) {
      regex.lastIndex++;
    }

    m.forEach((match) => hashTags.add(match));
  }

  return Array.from(hashTags);
}
