"""
    table = dynamodb_client.create_table(
    TableName=SystemModulesTableName,
    KeySchema=[
        {
            'AttributeName': 'ID',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'Time',
            'KeyType': 'RANGE'
        }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'ID',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'Time',
                'AttributeType': 'S'
            },
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    # Wait until the table exists.
    table.wait_until_exists()
    file_reader = json_object['Body'].read().decode("utf-8")
    s = json.loads(file_reader, parse_float=Decimal)

    s = s['SystemModules']

    table = dynamodb_client.Table(SystemModulesTableName)
    
    with table.batch_writer(overwrite_by_pkeys=['ID', 'Time']) as batch:
        for j in s:
            j=j['Modules']
            for i in j:
                batch.put_item(
                    Item=i
                )
                
    BatteryDetailsTableName = json_file_name[7:len(json_file_name)] + "_BatteryDetails"
    table = dynamodb_client.create_table(
    TableName=BatteryDetailsTableName,
    KeySchema=[
        {
            'AttributeName': 'SOC',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'Time',
            'KeyType': 'RANGE'
        }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'SOC',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'Time',
                'AttributeType': 'S'
            },
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    # Wait until the table exists.
    table.wait_until_exists()
    
    s = json.loads(file_reader, parse_float=Decimal)

    s = s['BatteryDetails']

    table = dynamodb_client.Table(BatteryDetailsTableName)
    
    with table.batch_writer(overwrite_by_pkeys=['SOC', 'Time']) as batch:
        for i in s:
            batch.put_item(
                Item=i
            )
            
    PackDetailsTableName = json_file_name[7:len(json_file_name)] + "_PackDetails"
    table = dynamodb_client.create_table(
    TableName=PackDetailsTableName,
    KeySchema=[
        {
            'AttributeName': 'Td',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'Time',
            'KeyType': 'RANGE'
        }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'Td',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'Time',
                'AttributeType': 'S'
            },
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    # Wait until the table exists.
    table.wait_until_exists()
    
    s = json.loads(file_reader, parse_float=Decimal)

    s = s['PackDetails']

    table = dynamodb_client.Table(PackDetailsTableName)
    
    with table.batch_writer(overwrite_by_pkeys=['Td', 'Time']) as batch:
        for i in s:
            batch.put_item(
                Item=i
            )
    """