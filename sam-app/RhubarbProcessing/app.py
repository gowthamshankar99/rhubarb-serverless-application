import json
import boto3
from rhubarb import DocAnalysis, LanguageModels
import os
import uuid

def lambda_handler(event, context):
    # Initialize boto3 session
    session = boto3.Session()

    # Initialize DynamoDB client
    dynamodb = session.client('dynamodb')
    table_name = os.environ['TABLE_NAME']
    s3 = boto3.client('s3')
    
    # JSON schema for the document analysis
    schema = {
        "type": "object",
        "properties": {
            "infrastructure_cost": {
                "description": "Cost related to infrastructure hosting",
                "type": "string"
            },
            "development_cost": {
                "description": "Cost related to application development",
                "type": "string"
            },
            "maintenance_cost": {
                "description": "Cost related to maintaining the application",
                "type": "string"
            },
            "case_study_1_overview": {
                "description": "Overview of the first case study",
                "type": "string"
            },
            "cost_calculation": {
                "description": "Cost calculation details",
                "type": "string"
            }
        },
        "required": [
            "infrastructure_cost", 
            "development_cost", 
            "maintenance_cost", 
            "case_study_1_overview", 
            "cost_calculation"
        ]
    }


    # Get the file key from the event
    file_key = event['file_key']
    
    # Assume bucket name is stored in an environment variable
    bucket_name = os.environ['BUCKET_NAME']

    file_path = f"/tmp/{file_key.split('/')[-1]}"
    s3.download_file(bucket_name, file_key, file_path)

    try:
        da = DocAnalysis(file_path=file_path, boto3_session=session, modelId=LanguageModels.CLAUDE_HAIKU_V1)
        resp = da.run(message="Give me the output based on the provided schema.", output_schema=schema)
        
        # Store the response in DynamoDB
        item = {
            'id': {'S': str(uuid.uuid4())},
            'full_json': {'S': json.dumps(resp)}
        }
        
        print(resp)
        print("******")
        print(item)
        # Process extracted fields and add them to the DynamoDB item
        if 'output' in resp:
            output = resp['output']

            # Infrastructure cost
            if 'infrastructure_cost' in output:
                item['infrastructure_cost'] = {'S': output['infrastructure_cost']}
            
            # Development cost
            if 'development_cost' in output:
                item['development_cost'] = {'S': output['development_cost']}
            
            # Maintenance cost
            if 'maintenance_cost' in output:
                item['maintenance_cost'] = {'S': output['maintenance_cost']}
            
            # Case Study 1 Overview
            if 'case_study_1_overview' in output:
                item['case_study_1_overview'] = {'S': output['case_study_1_overview']}
            
            # Cost Calculation
            if 'cost_calculation' in output:
                item['cost_calculation'] = {'S': output['cost_calculation']}

        # Token usage tracking (optional, as in original code)
        if 'token_usage' in resp:
            if 'input_tokens' in resp['token_usage']:
                item['input_tokens'] = {'N': str(resp['token_usage']['input_tokens'])}
            if 'output_tokens' in resp['token_usage']:
                item['output_tokens'] = {'N': str(resp['token_usage']['output_tokens'])}


        # Put item in DynamoDB
        dynamodb.put_item(
            TableName=table_name,
            Item=item
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps("Data written to DynamoDB successfully.")
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error processing file: {str(e)}")
        }
    
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)