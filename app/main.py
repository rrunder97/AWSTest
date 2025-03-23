import boto3
import pandas as pd
import json

# AWS S3 Config (Only needed if running in Lambda)
S3_BUCKET = "your-s3-bucket-name"  # Replace with your S3 bucket
S3_KEY = "ec2_instances.csv"  # File name in S3

def flatten_tags(tags, prefix=""):
    """Recursively flattens nested tags into a dictionary with dot notation keys."""
    flattened = {}
    for tag in tags:
        key = f"{prefix}{tag['Key']}"
        value = tag['Value']
        
        if isinstance(value, dict):  # If value is nested, recursively flatten
            nested_flattened = flatten_tags([{"Key": k, "Value": v} for k, v in value.items()], prefix=f"{key}.")
            flattened.update(nested_flattened)
        else:
            flattened[key] = value
    return flattened

def get_ec2_instances():
    """Fetch all EC2 instances and their details."""
    ec2 = boto3.client('ec2')
    response = ec2.describe_instances()
    
    instance_list = []
    all_tags = set()
    
    # First pass to collect all possible tag keys
    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            tags = instance.get('Tags', [])
            print(tags)
            tag_dict = flatten_tags(tags)
            all_tags.update(tag_dict.keys())
    
    # Second pass to collect instance details
    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            instance_id = instance.get('InstanceId', '')
            private_ip = instance.get('PrivateIpAddress', 'N/A')
            public_ip = instance.get('PublicIpAddress', 'N/A')
            
            # Extract tags into a dictionary
            tags = instance.get('Tags', [])
            tag_dict = flatten_tags(tags)
            name = tag_dict.get('Name', 'N/A')  # Default to 'N/A' if no Name tag
            
            # Create a row with instance details
            instance_data = {
                'Name': name,
                'Instance ID': instance_id,
                'Private IP': private_ip,
                'Public IP': public_ip,
            }
            
            # Add all known tag keys to ensure consistent columns
            for tag in all_tags:
                instance_data[tag] = tag_dict.get(tag, 'N/A')
            
            instance_list.append(instance_data)
    
    return instance_list

def export_to_csv_local(instances):
    """Export EC2 data to a CSV file locally."""
    df = pd.DataFrame(instances)
    df.to_csv("ec2_instances.csv", index=False)
    print("CSV file saved: ec2_instances.csv")

def export_to_s3(instances):
    """Export EC2 data to a CSV and upload it to S3 (for Lambda)."""
    df = pd.DataFrame(instances)
    csv_filename = "/tmp/ec2_instances.csv"  # Temp storage in Lambda
    
    # Save CSV locally in /tmp (Lambda's writable directory)
    df.to_csv(csv_filename, index=False)
    
    # Upload to S3
    s3 = boto3.client('s3')
    s3.upload_file(csv_filename, S3_BUCKET, S3_KEY)

    return f"s3://{S3_BUCKET}/{S3_KEY}"

def lambda_handler(event, context):
    """AWS Lambda entry point."""
    instances = get_ec2_instances()
    s3_path = export_to_s3(instances)
    
    return {
        "statusCode": 200,
        "body": json.dumps({"message": "CSV uploaded", "s3_path": s3_path})
    }

if __name__ == "__main__":
    # Running locally
    instances = get_ec2_instances()
    export_to_csv_local(instances)
