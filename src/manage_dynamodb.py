import boto3
import decimal
import json

from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr


def get_dynamodb_client():
    dynamodb = boto3.client("dynamodb", region_name="eu-west-3", endpoint_url="http://localhost:8000")
    """ :type : pyboto3.dynamodb """
    return dynamodb


def get_dynamodb_resource():
    dynamodb = boto3.resource("dynamodb", region_name="eu-west-3", endpoint_url="http://localhost:8000")
    """ :type : pyboto3.dynamodb """
    return dynamodb


def create_table():
    table_name = "Movies"

    attribute_definitions = [
        {
            'AttributeName': 'year',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'title',
            'AttributeType': 'S'
        }
    ]

    key_schema = [
        {
            'AttributeName': 'year',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'title',
            'KeyType': 'RANGE'
        }
    ]

    initial_iops = {
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }

    dynamodb_table_response = get_dynamodb_client().create_table(
        AttributeDefinitions=attribute_definitions,
        TableName=table_name,
        KeySchema=key_schema,
        ProvisionedThroughput=initial_iops
    )

    print("Created DynamoDB table:" + str(dynamodb_table_response))


def put_item_on_table():
    try:
        response = get_dynamodb_resource().Table("Movies").put_item(
            Item={
                'year': 2015,
                'title': "The Big New Movie",
                'info': {
                    'plot': "Nothing happens at all.",
                    'rating': decimal.Decimal(0)
                }
            }
        )

        print("A New Movie added to the collection successfully!")
        print(str(response))
    except Exception as error:
        print(error)


def update_item_on_table():
    response = get_dynamodb_resource().Table("Movies").update_item(
        Key={
            'year': 2015,
            'title': 'The Big New Movie'
        },
        UpdateExpression="set info.rating = :r, info.plot = :p, info.actors = :a",
        ExpressionAttributeValues={
            ':r': decimal.Decimal(3.5),
            ':p': "Everything happens all at once",
            ':a': ["Larry", "Moe", "David"]
        },
        ReturnValues="UPDATED_NEW"
    )

    print("Updating existing movie was success!")
    print(str(response))


def conditionally_update_an_item():
    try:
        respone = get_dynamodb_resource().Table("Movies").update_item(
            Key={
                'year': 2015,
                'title': 'The Big New Movie'
            },
            UpdateExpression="remove info.actors[0]",
            ConditionExpression="size(info.actors) >= :num",
            ExpressionAttributeValues={
                ':num': 3
            },
            ReturnValues="UPDATED_NEW"
        )
    except ClientError as error:
        if error.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(error.response['Error']['Message'])
        else:
            raise
    else:
        print("Updated item on table conditionally!")
        print(str(respone))


def get_item_on_table():
    try:
        response = get_dynamodb_resource().Table("Movies").get_item(
            Key={
                'year': 2015,
                'title': "The Big New Movie"
            }
        )
    except ClientError as error:
        print(error.response['Error']['Message'])
    else:
        item = response['Item']
        print("Got the item successfully!")
        print(str(response))


def delete_item_on_table():
    try:
        response = get_dynamodb_resource().Table("Movies").delete_item(
            Key={
                'year': 2015,
                'title': "The Big New Movie"
            }
        )
    except ClientError as error:
        if error.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(error.response['Error']['Message'])
        else:
            raise
    else:
        print("Deleted item successfully!")
        print(str(response))


def insert_sample_data():
    table = get_dynamodb_resource().Table("Movies")

    with open("moviedata.json") as json_file:
        movies = json.load(json_file, parse_float=decimal.Decimal)
        for movie in movies:
            year = int(movie['year'])
            title = movie['title']
            info = movie['info']

            print("Adding movie:", year, title)

            table.put_item(
                Item={
                    'year': year,
                    'title': title,
                    'info': info
                }
            )

    print("Sample movie data inserted successfully!")


def query_movies_released_in_1985():
    response = get_dynamodb_resource().Table("Movies").query(
        KeyConditionExpression=Key('year').eq(1985)
    )

    for movie in response['Items']:
        print(movie['year'], ":", movie['title'])


def query_movies_with_extra_conditions():
    print("Movies from 1992 - title A-L, with genres and lead actor")

    response = get_dynamodb_resource().Table("Movies").query(
        ProjectionExpression="#yr, title, info.genres, info.actors[0]",
        ExpressionAttributeNames={"#yr": "year"},
        KeyConditionExpression=Key('year').eq(1992) & Key('title').between('A', 'L')
    )

    for movie in response['Items']:
        print(str(movie))


def scan_whole_table_for_items():
    filter_expression = Key('year').between(1950, 1959)
    projection_expression = "#yr, title, info.rating"
    ean = {"#yr": "year",}

    response = get_dynamodb_resource().Table("Movies").scan(
        FilterExpression=filter_expression,
        ProjectionExpression=projection_expression,
        ExpressionAttributeNames=ean
    )

    for movie in response['Items']:
        print(str(movie))


if __name__ == '__main__':
    # create_table()
    # put_item_on_table()
    # update_item_on_table()
    # conditionally_update_an_item()
    # get_item_on_table()
    # delete_item_on_table()
    # insert_sample_data()
    # query_movies_released_in_1985()
    # query_movies_with_extra_conditions()
    scan_whole_table_for_items()