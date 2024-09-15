import { DynamoDBClient, PutItemCommand } from "@aws-sdk/client-dynamodb";
import { marshall } from '@aws-sdk/util-dynamodb'
import { Chance } from 'chance'

const client = new DynamoDBClient();
const chance = new Chance();
const {USERS_TABLE} = process.env

export const handler =  async (event) => {
  console.log("EVENT: \n" + JSON.stringify(event, null, 2));
  if(event.triggerSource === 'PostConfirmation_ConfirmSignUp') {
    const name = event.request.userAttributes['name']
    const suffix = chance.string({ length: 8, casing: 'upper', alpha: true, numeric: true })
    const screenName = `${name.replace(/[^a-zA-Z0-9]/g, "")}${suffix}`
    const user = {
      id: event.userName,
      name,
      screenName,
      createdAt: new Date().toJSON(),
      followersCount: 0,
      followingCount: 0,
      tweetsCount: 0,
      likesCounts: 0
    }

    try {
      await client.send(new PutItemCommand({
        "TableName": USERS_TABLE,
        "Item": marshall(user),
        ConditionExpression: 'attribute_not_exists(id)'
      }))
    } catch (error) {
      console.log("e", error)
    }
    return event
  } else {
    return event
  }
};
