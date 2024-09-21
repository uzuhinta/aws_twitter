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

let tweetsIndex;

const initTweetsIndex = async (appId, key, stage) => {
  if (!tweetsIndex) {
    const client = algoliasearch(appId, key)
    tweetsIndex = client.initIndex(`tweets_${stage}`)
    await tweetsIndex.setSettings({
      attributesForFaceting: [
        "hashTags"
      ],
      searchableAttributes: [
        "text"
      ],
      customRanking: [
        "desc(createdAt)"
      ]
    })
  }

  return tweetsIndex
}

export const handler = async (event) => {
  console.log('event', event);
  const index = await initTweetsIndex(ALGOLIA_APP_ID, ALGOLIA_WRITE_KEY, STAGE);
  for (const record of event.Records) {
    if (record.eventName === 'INSERT' || record.eventName === 'MODIFY') {
      const tweet = unmarshall(record.dynamodb.NewImage);
      if (tweet.__typename === 'Retweet') {
        continue
      }
      tweet.objectID = tweet.id;
      await index.saveObjects([tweet]);
    } else if (record.eventName === 'REMOVE') {
      const tweet = unmarshall(record.dynamodb.OldImage);
      if (tweet.__typename === 'Retweet') {
        continue
      }
      await index.deleteObjects([tweet.id])
    }
  }
};