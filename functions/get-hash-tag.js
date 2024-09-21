import {
  BatchWriteItemCommand,
  DynamoDBClient,
  GetItemCommand,
  QueryCommand,
} from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import _ from 'lodash';
import algoliasearch from 'algoliasearch';
import { Chance } from 'chance';

const chance = new Chance();

const { ALGOLIA_APP_ID, ALGOLIA_WRITE_KEY, STAGE } = process.env;

let tweetsIndex;

const initTweetsIndex = async (appId, key, stage) => {
  if (!tweetsIndex) {
    const client = algoliasearch(appId, key);
    tweetsIndex = client.initIndex(`tweets_${stage}`);
    await tweetsIndex.setSettings({
      attributesForFaceting: ['hashTags'],
      searchableAttributes: ['text'],
      customRanking: ['desc(createdAt)'],
    });
  }

  return tweetsIndex;
};

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
  const userId = event.identity.username;
  const { hashTag, mode, limit, nextToken } = event.arguments;

  switch (mode) {
    case 'People':
      return await searchPeople(userId, hashTag, limit, nextToken);
    case 'Latest':
      return await searchLatest(hashTag, limit, nextToken);
    default:
      throw new Error(
        'Only "People" and "Latest" hash tag modes are supported right now'
      );
  }
};

async function searchPeople(userId, hashTag, limit, nextToken) {
  const index = await initUsersIndex(ALGOLIA_APP_ID, ALGOLIA_WRITE_KEY, STAGE);

  const searchParams = parseNextToken(nextToken) || {
    hitsPerPage: limit,
    page: 0,
  };

  const query = hashTag.replace('#', '');
  const { hits, page, nbPages } = await index.search(query, searchParams);
  hits.forEach((x) => {
    x.__typename = x.id === userId ? 'MyProfile' : 'OtherProfile';
  });

  let nextSearchParams;
  if (page + 1 >= nbPages) {
    nextSearchParams = null;
  } else {
    nextSearchParams = Object.assign({}, searchParams, { page: page + 1 });
  }

  return {
    results: hits,
    nextToken: genNextToken(nextSearchParams),
  };
}

async function searchLatest(hashTag, limit, nextToken) {
  const index = await initTweetsIndex(ALGOLIA_APP_ID, ALGOLIA_WRITE_KEY, STAGE);

  const searchParams = parseNextToken(nextToken) || {
    facetFilters: [`hashTags:${hashTag}`],
    hitsPerPage: limit,
    page: 0,
  };

  const { hits, page, nbPages } = await index.search('', searchParams);

  let nextSearchParams;
  if (page + 1 >= nbPages) {
    nextSearchParams = null;
  } else {
    nextSearchParams = Object.assign({}, searchParams, { page: page + 1 });
  }

  return {
    results: hits,
    nextToken: genNextToken(nextSearchParams),
  };
}

function parseNextToken(nextToken) {
  if (!nextToken) {
    return undefined;
  }

  const token = Buffer.from(nextToken, 'base64').toString();
  const searchParams = JSON.parse(token);
  delete searchParams.random;

  return searchParams;
}

function genNextToken(searchParams) {
  if (!searchParams) {
    return null;
  }

  const payload = Object.assign({}, searchParams, {
    random: chance.string({ length: 16 }),
  });
  const token = JSON.stringify(payload);
  return Buffer.from(token).toString('base64');
}
