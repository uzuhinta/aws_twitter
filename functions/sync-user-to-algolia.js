import {
  BatchWriteItemCommand,
  DynamoDBClient,
  GetItemCommand,
  QueryCommand,
} from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import _ from 'lodash';
import algoliasearch from "algoliasearch";

const { ALGOLIA_APP_ID, ALGOLIA_WRITE_KEY, STAGE } = process.env;

let usersIndex;

const initUsersIndex = async (appId, key, stage) => {
  if (!usersIndex) {
    const client = algoliasearch(appId, key);
    usersIndex = client.initIndex(`users_${stage}`);
    await usersIndex.setSettings({
      searchableAttributes: ['name', 'screenName', 'bio'],
    });
  }

  return usersIndex;
};

export const handler = async (event) => {
  console.log('event', event);
  const index = await initUsersIndex(ALGOLIA_APP_ID, ALGOLIA_WRITE_KEY, STAGE);
  for (const record of event.Records) {
    if (record.eventName === 'INSERT' || record.eventName === 'MODIFY') {
      const profile = unmarshall(record.dynamodb.NewImage);
      profile.objectID = profile.id;
      await index.saveObjects([profile]);
    } else if (record.eventName === 'REMOVE') {
      const profile = unmarshall(record.dynamodb.OldImage);
      await index.deleteObjects([profile.id])
    }
  }
};
