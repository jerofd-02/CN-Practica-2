import json
import base64
import datetime

def lambda_handler(event, context):
    output = []
    for record in event['records']:
        payload = base64.b64decode(record['data']).decode('utf-8')
        data_json = json.loads(payload)
        
        # Get the partition key (YYYY-MM-DD format)
        processing_date = data_json.get('processing_date', '2025-01-01') 

        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode((json.dumps(data_json) + '\n').encode('utf-8')).decode('utf-8'),
            'metadata': {
                'partitionKeys': {
                    'processing_date': processing_date
                }
            }
        }
        output.append(output_record)
    
    return {'records': output}
