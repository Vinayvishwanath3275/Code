import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
import boto3
import json



def get_secret(secret_name, region_name):
    # Create a Secrets Manager client
    client = boto3.client('secretsmanager', region_name=region_name)

    try:
        # Retrieve the secret
        response = client.get_secret_value(SecretId=secret_name)

        # Parse and return the secret if the response contains a 'SecretString'
        if 'SecretString' in response:
            secret = response['SecretString']
            return json.loads(secret)
        else:
            # If the secret is binary
            secret = response['SecretBinary']
            return secret
    except Exception as e:
        # Handle errors
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            print("Secrets Manager can't decrypt the protected secret text using the provided KMS key.")
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            print("An error occurred on the server side.")
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print("The request had invalid parameters.")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print("The request was invalid due to incorrect parameters.")
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("The requested secret was not found.")
        else:
            print("An error occurred: ", e)
        return None



# Function to send an email
def send_birthday_email(name, email):

    
    # Email account credentials
    sender_email = "vinayvishwa3275@outlook.com"
    
    secret_name = "vinay_microsoft_acc"
    region_name = "us-east-1"
    secret = get_secret(secret_name, region_name)
    
    if secret:
        #Getting password
        # Access the specific password key if your secret is a JSON object
        sender_password = secret[sender_email]
        
    
    # Email content
    subject = "Happy Birthday!"
    body = f"Dear {name},\n\nWishing you a wonderful birthday filled with love, joy, and happiness!\n\nBest regards,\nVinay Vishwanath\nAccenture"
    
    # Setup the MIME
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = email
    message['Subject'] = subject
    message.attach(MIMEText(body, 'plain'))
    
    # Send the email
    try:
        server = smtplib.SMTP('smtp.office365.com',587)  # Use the appropriate SMTP server and port
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, email, message.as_string())
        server.quit()
        print(f"Birthday email sent to {name} at {email}")
    except Exception as e:
        print(f"Failed to send email to {name}. Error: {str(e)}")
        
        

def main():
    
    # Set up S3 client
    s3 = boto3.client('s3')
    
    event_dict = {"bucket_name" : "myaws-test-bucket-vinay","s3_folder": '',"file_name": 'birthdays.xlsx'}
    print("event_dict :",event_dict)

    # Extract the bucket name, folder, and file name from the event_dict
    bucket_name = event_dict['bucket_name']
    s3_folder = event_dict['s3_folder']
    file_name = event_dict['file_name']
    
    # Set the download path in the /tmp/ directory of Lambda
    download_path = f"/tmp/{file_name}"
    
    # Construct the full S3 object key
    s3_object_key = f"{s3_folder}/{file_name}" if s3_folder else file_name

    # Download the file from S3 to /tmp/
    s3.download_file(bucket_name, s3_object_key, download_path)
    
    # Process the file (optional) - for now, just log the download path
    print(f"Downloaded {file_name} from S3 bucket {bucket_name} to {download_path}")
    
   # Define the file path in the /tmp directory
    #file_path = '/tmp/{file_name}'

    # Load the Excel sheet
    df = pd.read_excel(download_path)

    # Get today's date
    today = datetime.now().strftime('%m-%d')

    for index, row in df.iterrows():
       dob = row['Date of Birth'].strftime('%m-%d')
       if dob == today:
            send_birthday_email(row['Name'], row['Email'])

def lambda_handler(event, context):
    
    main()

    return {
        'statusCode': 200,
        'body': f"Execution Completed"
    }
