import boto3
from datetime import datetime, timezone
from py-logger import Logger
import socket
import pytz
from boto3.dynamodb.conditions import Key
import csv
import os


_DEF_PROVISIONED_THROUGHPUT = 10
_ENDPOINT_URL = "http://localhost:8000"
_ENV_HOST_NAME = str(socket.gethostbyaddr(socket.gethostname())[0])
_DEV_ETL_NODE = 'dfhdpetl02'
_PROD_ETL_NODE = 'dfhdpetl03'

py_file = os.path.splitext(os.path.split(__file__)[1])[0]
log = Logger(log_directory='/etl/log/', log_file_app=py_file, vendor_name='Internal',
                    vendor_product='KeyValueStore')


class DynamoDbDatabase:
    def __init__(self, endpoint_url=None):
        self.profile_name = 'default'

        if _DEV_ETL_NODE in _ENV_HOST_NAME:
            self.suffix = '_dev'
        elif _PROD_ETL_NODE in _ENV_HOST_NAME:
            self.suffix = '_prod'
        else:
            self.suffix = '_dev'

        if endpoint_url is not None:
            self._endpoint_url = endpoint_url
        else:
            self._endpoint_url = _ENDPOINT_URL

    # function to get value of _endpoint_url
    def get_endpoint_url(self):
        return self._endpoint_url

    endpoint_url = property(get_endpoint_url)

    def get_aws_session(self, session=None):
        if not session:
            session = boto3.Session(profile_name=self.profile_name)
        return session

    def get_database_resource(self):
        db = self.get_aws_session().resource('dynamodb')
        return db

    def get_database_client(self):
        client = self.get_aws_session().client('dynamodb')
        return client


class DynamoDbTable(DynamoDbDatabase):
    pass

    def get_complete_name(self, table_name):
        if table_name.endswith(self.suffix):
            return table_name
        else:
            return table_name + self.suffix

    def get_table_resource(self, table_name):
        complete_name = self.get_complete_name(table_name)
        t = super().get_database_resource().Table(complete_name)
        return t

    def if_table_exists(self, table_name):
        db = super().get_database_resource()
        complete_name = self.get_complete_name(table_name)
        existing_tables = [table.name for table in db.tables.all()]
        return complete_name in existing_tables

    def list_all_existing_tables(self):
        db = super().get_database_resource()

        existing_tables = [table.name for table in db.tables.all()]

        log.info('KEYSTORE - All existing Tables:')
        for table in existing_tables:
            if table.endswith(self.suffix):
                print(table)

    def delete_table(self, table_name):
        client = super().get_database_client()

        complete_name = self.get_complete_name(table_name)

        if self.if_table_exists(table_name):
            client.delete_table(TableName=complete_name)
            log.info('KEYSTORE - Table ' + complete_name + ' has been successfully deleted.')
        else:
            log.info('KEYSTORE - Table ' + complete_name + ' does not exist.')

    def create_table(self, table_name):
        table_name_low = table_name.lower()
        complete_name = self.get_complete_name(table_name_low)

        partition_key = 'key'
        sort_key = 'datetime'
        pk_key_type = 'HASH'
        sk_key_type = 'RANGE'
        pk_datatype = 'S'
        sk_datatype = 'S'

        dynamodb = super().get_database_resource()

        if self.if_table_exists(table_name_low):
            log.info('KEYSTORE - Table ' + complete_name + ' already exists.')
        else:
            table = dynamodb.create_table(
                TableName=complete_name,
                KeySchema=[
                    {
                        'AttributeName': partition_key,
                        'KeyType': pk_key_type  # Partition key
                    },
                    {
                        'AttributeName': sort_key,
                        'KeyType': sk_key_type  # Sort key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': partition_key,
                        'AttributeType': pk_datatype
                    },
                    {
                        'AttributeName': sort_key,
                        'AttributeType': sk_datatype
                    },

                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': _DEF_PROVISIONED_THROUGHPUT,
                    'WriteCapacityUnits': _DEF_PROVISIONED_THROUGHPUT
                }
            )
            log.info('KEYSTORE - Table ' + complete_name + ' has been created.')
            return table

    def put_item_into_table(self, table_name, key, value):
        table_name_low = table_name.lower()

        complete_name = self.get_complete_name(table_name_low)

        # TODO: DynamoDB does not support any date types. So we are going for String with ISO format.
        # TODO: Range queries are supported when the date is stored as String. The BETWEEN can be used on FilterExpression.
        utc_dt = datetime.now(timezone.utc)  # UTC time
        dt = utc_dt.astimezone(tz=pytz.timezone('US/Central'))  # local time

        x = dt.isoformat()
        # convert date and time to string
        datetime_now = str(x)

        table = self.get_table_resource(table_name=complete_name)

        if table:
            response = table.put_item(
                Item={
                    'key': key,
                    'datetime': datetime_now,
                    'info': {
                        'value': value
                    }
                }
            )
            log.info('KEYSTORE - A new item has been added to the table.')
            return response
        else:
            log.info('KEYSTORE - Provided table does not exist.')

    def get_latest_item_value(self, table_name, key):
        complete_name = self.get_complete_name(table_name)

        table = self.get_table_resource(table_name=complete_name)

        if table:
            response = table.query(
                KeyConditionExpression=Key('key').eq(key),
                # Limit top 1
                Limit=1,
                # Sorting by sort_key - desc
                ScanIndexForward=False
            )
            if response['Count'] == 0:
                log.info('KEYSTORE - No values found with provided key')
                return None
            else:
                return response['Items'][0]['info']['value']
        else:
            log.info('KEYSTORE - Provided table does not exist in the database.')

    def get_items_greater_than_date(self, table_name, key, yyyymmdd):
        complete_name = self.get_complete_name(table_name)

        # TODO: DynamoDB does not support any date types. So we are going for String with ISO format.
        # TODO: Range queries are supported when the date is stored as String. The BETWEEN can be used on FilterExpression.

        str_yyyymmdd = str(yyyymmdd)

        d = datetime.strptime(str_yyyymmdd, '%Y%m%d')
        from_yyyymmdd = str(d.isoformat())

        table = self.get_table_resource(table_name=complete_name)

        if table:
            response = table.query(
                # Sorting by sort_key - desc
                ScanIndexForward=False,
                # Filtering by key + Between specific dates
                KeyConditionExpression=Key('key').eq(key) & Key('datetime').gte(from_yyyymmdd)
            )
            if response['Count'] == 0:
                log.info('KEYSTORE - No values were found.')
                return None
            else:
                return response['Items']
        else:
            log.info('KEYSTORE - Provided table does not exist in the database.')

    def import_data_from_csv(self, csv_file_name):
        with open(csv_file_name, mode='r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            line_count = 0
            for row in csv_reader:
                table = row[0] + self.suffix
                if not self.if_table_exists(table_name=table):
                    self.create_table(table_name=table)

                key = row[1]
                value = row[2]
                d = datetime.strptime(row[3], '%Y%m%d')
                date_yyyymmdd = str(d.isoformat())

                self.get_table_resource(table_name=table).put_item(
                    Item={
                        'key': key,
                        'datetime': date_yyyymmdd,
                        'info': {
                            'value': value
                        }
                    }
                )
                line_count += 1
                log.info('KEYSTORE - Item: ' + key + ' has been added into ' + table + ' table.')

        log.info(
            'KEYSTORE - CSV File: ' + csv_file_name + ' content has been successfully imported into DynamoDb Tables.')

    def export_data_to_csv(self, table_name, key, csv_file_name):
        complete_name = self.get_complete_name(table_name)

        table = self.get_table_resource(table_name=complete_name)

        if table:
            response = table.query(
                KeyConditionExpression=Key('key').eq(key),
                # Sorting by sort_key - desc
                ScanIndexForward=False
            )
            if response['Count'] == 0:
                log.info('KEYSTORE - No values found with provided key')
                return None
            else:
                count = int(response['Count'])
                header = ['table', 'key', 'value', 'datetime_yyyymmdd']
                data = []
                for x in range(count):
                    data.append([complete_name,
                                 response['Items'][0]['key'],
                                 response['Items'][0]['info']['value'],
                                 response['Items'][0]['datetime']])

                # open the file in the write mode
                with open(csv_file_name, 'w', newline='') as f:
                    # create the csv writer
                    writer = csv.writer(f)
                    # write the header
                    writer.writerow(header)
                    # write rows to the csv file
                    writer.writerows(data)

                log.info('KEYSTORE - CSV File: ' + csv_file_name + ' has been exported.')
        else:
            log.info('KEYSTORE - Provided table does not exist in the database.')


class Keystore(DynamoDbTable):
    pass
