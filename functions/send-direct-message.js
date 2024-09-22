import {
  DynamoDBClient,
  TransactWriteItemsCommand,
} from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';
import { ulid } from 'ulid';

const client = new DynamoDBClient();

const { CONVERSATIONS_TABLE_NAME, DIRECT_MESSAGES_TABLE_NAME } = process.env;
export const handler = async (event) => {
  console.log('EVENT: \n' + JSON.stringify(event, null, 2));

  const { otherUserId, message } = event.arguments;
  const { username } = event.identity;
  const timestamp = new Date().toJSON();

  const conversationId =
    username < otherUserId
      ? `${username}_${otherUserId}`
      : `${otherUserId}_${username}`;

  const transactItems = [
    {
      Put: {
        TableName: DIRECT_MESSAGES_TABLE_NAME,
        Item: marshall({
          conversationId,
          messageId: ulid(),
          message,
          from: username,
          timestamp,
        }),
      },
    },
    {
      Update: {
        TableName: CONVERSATIONS_TABLE_NAME,
        Key: marshall({
          userId: username,
          otherUserId,
        }),
        UpdateExpression:
          'SET id = :id, lastMessage = :lastMessage, lastModified = :now',
        ExpressionAttributeValues: marshall({
          ':id': conversationId,
          ':lastMessage': message,
          ':now': timestamp,
        }),
      },
    },
    {
      Update: {
        TableName: CONVERSATIONS_TABLE_NAME,
        Key: marshall({
          userId: otherUserId,
          otherUserId: username,
        }),
        UpdateExpression:
          'SET id = :id, lastMessage = :lastMessage, lastModified = :now',
        ExpressionAttributeValues: marshall({
          ':id': conversationId,
          ':lastMessage': message,
          ':now': timestamp,
        }),
      },
    },
  ];

  await client.send(
    new TransactWriteItemsCommand({
      TransactItems: transactItems,
    })
  );

  return {
    id: conversationId,
    otherUserId,
    lastMessage: message,
    lastModified: timestamp,
  };
};
