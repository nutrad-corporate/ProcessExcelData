import os,csv
import json
import boto3
import pandas as pd
import numpy as np
import requests
from io import BytesIO, StringIO
from botocore.exceptions import ClientError

API_KEY = os.getenv('API_KEY')
API_URL = "https://api.openai.com/v1/chat/completions"
model_id = 'us.meta.llama3-2-90b-instruct-v1:0'

def create_mapping(input_bucket_name, file_key):
    try:
        print("Initializing the function...")
        
        # Initialize clients
        s3_client = boto3.client('s3')
        bedrock_client = boto3.client(
            'bedrock-runtime',
            region_name='us-east-1',
            config=boto3.session.Config(read_timeout=1200)
        )
        print("S3 and Bedrock clients initialized...")

        # Validate event structure
        if 'Records' not in event or not event['Records']:
            raise KeyError("The event does not contain 'Records' or is empty.")

        # Get the bucket and key from the event
        
        print(f"Processing file: {file_key} from bucket: {input_bucket_name}")

        # PART 1: DEFINITIONS GENERATION
        if file_key.endswith('.xlsx') or file_key.endswith('.xls'):
            print("Processing Excel file for column definitions...")
            
            # Fetch and read the file from S3
            file_object = s3_client.get_object(Bucket=input_bucket_name, Key=file_key)
            excel_content = BytesIO(file_object['Body'].read())

            # Read the Excel file
            print("Reading the Excel file...")
            excel_data = pd.read_excel(excel_content, engine='openpyxl')

            # Split columns into chunks to handle large datasets
            def split_columns(dataframe, chunk_size):
                columns = dataframe.columns.tolist()
                for i in range(0, len(columns), chunk_size):
                    yield dataframe[columns[i:i + chunk_size]]

            chunk_size = 30  # Adjust this based on model limits
            chunks = list(split_columns(excel_data, chunk_size))

            responses = []
            for idx, chunk in enumerate(chunks, start=1):
                print(f"Processing Chunk {idx}...")

                # Extract column names and sample data
                target_columns = chunk.columns.tolist()
                sample_data = {
                    col: chunk[col].astype(str).head(2).tolist()
                    for col in chunk.columns
                }

                # Prepare the prompt
                prompt = f"""
                You are an expert in analyzing CSV file column names and their contents. 
                Provide a JSON-formatted response that describes each column in CSV.

                Column Names: {', '.join(target_columns)}
                Sample Data: {json.dumps(sample_data)}

                Instructions:
                1. Generate a JSON object with one key: 'CSV2Columns'.
                2. Include an array of objects for each column.
                3. Each JSON object should follow this format: 
                   {{"ColumnName": "<column name>", "Description": "<description>"}}.
                4. Respond ONLY with the JSON object.
                """

                formatted_prompt = f"""
                <|begin_of_text|><|start_header_id|>user<|end_header_id|>
                {prompt}
                <|eot_id|>
                <|start_header_id|>assistant<|end_header_id|>
                """

                native_request = {
                    'prompt': formatted_prompt,
                    'temperature': 0.9,
                    'max_gen_len': 8192
                }

                request = json.dumps(native_request)

                # Invoke the model and process the response
                print("Invoking the Bedrock model...")
                try:
                    streaming_response = bedrock_client.invoke_model_with_response_stream(
                        modelId=model_id,
                        body=request
                    )
                    full_response = ""
                    for event in streaming_response["body"]:
                        chunk = json.loads(event["chunk"]["bytes"])
                        if "generation" in chunk:
                            full_response += chunk["generation"]
                            print(chunk["generation"], end="", flush=True)

                    print("\nModel response received. Parsing...")
                    parsed_response = json.loads(full_response)
                    csv2_columns = parsed_response.get('CSV2Columns', [])
                    responses.extend(csv2_columns)

                except (ClientError, json.JSONDecodeError, Exception) as e:
                    print(f"ERROR: Can't invoke '{model_id}' or parse response. Reason: {e}")
                    raise

            # Combine all responses into a DataFrame
            result_df = pd.DataFrame(responses)

            # Save the final DataFrame to CSV
            csv_buffer = StringIO()
            result_df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            # Upload the result to the S3 bucket
            output_key = 'Target.csv'
            s3_client.put_object(
                Bucket=input_bucket_name,
                Key=output_key,
                Body=csv_buffer.getvalue()
            )
            print(f"Target.csv successfully saved to {input_bucket_name}/{output_key}")
            
            # Check if Source.csv exists in the bucket, if yes, generate mappings
            try:
                s3_client.head_object(Bucket=input_bucket_name, Key='Source.csv')
                print("Source.csv found in bucket. Generating mappings...")
                create_mappings_from_s3(s3_client, input_bucket_name)
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    print("Source.csv not found in bucket. Skipping mapping generation.")
                else:
                    print(f"Error checking for Source.csv: {e}")
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'CSV processing completed successfully',
                    'output_bucket': input_bucket_name,
                    'files': [output_key]
                })
            }
        
        # If the uploaded file is Source.csv and Target.csv exists
        elif file_key == 'Source.csv':
            # Check if Target.csv exists
            try:
                s3_client.head_object(Bucket=input_bucket_name, Key='Target.csv')
                print("Target.csv found in bucket. Generating mappings...")
                create_mappings_from_s3(s3_client, input_bucket_name)
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    print("Target.csv not found in bucket. Skipping mapping generation.")
                else:
                    print(f"Error checking for Target.csv: {e}")
                    
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Source.csv processing completed',
                    'output_bucket': input_bucket_name
                })
            }
    except Exception as e:
        print(f"Failed to process: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Error: {str(e)}'
            })
        }


# Create mappings from S3 stored files
def create_mappings_from_s3(s3_client, bucket_name):
    try:
        print("Starting mapping generation from S3 files...")
        
        # Download Source.csv and Target.csv from S3
        source_obj = s3_client.get_object(Bucket=bucket_name, Key='Source.csv')
        target_obj = s3_client.get_object(Bucket=bucket_name, Key='Target.csv')
        
        source_csv = source_obj['Body'].read().decode('utf-8')
        target_csv = target_obj['Body'].read().decode('utf-8')
        
        # Save to temporary files or in-memory
        source_data = StringIO(source_csv)
        target_data = StringIO(target_csv)
        
        # Read CSVs with pandas
        source_df = pd.read_csv(source_data)
        target_df = pd.read_csv(target_data)
        
        print("\nLoaded Source CSV:")
        print(source_df)
        print("\nLoaded Target CSV:")
        print(target_df)
        
        # Build prompt and get OpenAI response
        prompt = build_prompt(source_df, target_df)
        response_text = get_openai_response(prompt)
        
        print("\nFinal Mapping Output:")
        print(response_text)
        
        # Process mapping output
        data = json.loads(response_text.strip('```json').strip('```'))
        mappings = data.get("mapped_columns", [])
        
        # Create mapping CSV content
        mapping_rows = [["Source_Column", "Mapped_Target_Column"]]
        for mapping in mappings:
            for source_col, target_cols in mapping.items():
                target_string = ", ".join(target_cols) if target_cols else ""
                mapping_rows.append([source_col, target_string])
                
        mapping_content = StringIO()
        writer = csv.writer(mapping_content)
        writer.writerows(mapping_rows)
        mapping_content.seek(0)
        
        # Upload Mapping.csv to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key='Mapping.csv',
            Body=mapping_content.getvalue()
        )
        print(f"Mapping.csv successfully saved to {bucket_name}/Mapping.csv")
        
        # Calculate and store mapping statistics
        stats = calculate_mapping_statistics_s3(source_df, target_df, response_text)
        
        # Upload statistics as a JSON file
        s3_client.put_object(
            Bucket=bucket_name,
            Key='MappingStats.json',
            Body=json.dumps(stats),
            ContentType='application/json'
        )
        print(f"Mapping statistics saved to {bucket_name}/MappingStats.json")
        
        return True
    except Exception as e:
        print(f"Error in create_mappings_from_s3: {e}")
        return False

# Load CSVs from local files
def load_csv_files(source_path, target_path):
    source_df = pd.read_csv(source_path)
    target_df = pd.read_csv(target_path)
    print("\nLoaded Source CSV:")
    print(source_df)
    print("\nLoaded Target CSV:")
    print(target_df)
    return source_df, target_df

# Build mapping prompt
def build_prompt(source_df, target_df):
    prompt = "You are providing CSV1 and CSV2, analyze the column names and definitions to create mappings.\n\n"
    
    prompt += "CSV1 (Source) Column Analysis:\n"
    for _, row in source_df.iterrows():
        col, description = row['ColumnName'], row['Description']
        prompt += f"- {col}: {description}\n"
    prompt += "\n"
    
    prompt += "CSV2 (Target) Column Analysis:\n"
    for _, row in target_df.iterrows():
        col, description = row['ColumnName'], row['Description']
        prompt += f"- {col}: {description}\n"
    prompt += "\n"

    prompt += """
    You are an expert in analyzing CSV file column names and their contents. Your task is to generate a comprehensive mapping strategy between columns of CSV1 (source) and CSV2 (target). The result must be structured as a strict JSON output, containing **all possible column relationships**. 

    Instructions:
    1. **Mapping Types**:
        - **One-to-One Mapping**: A direct relationship between one column in CSV1 and one column in CSV2.
        - **One-to-Many Mapping**: A column in CSV1 maps to multiple columns in CSV2.
        - **Every column in CSV1** must have **at least one mapping** to one or more columns in CSV2.
    
    2. **Mapping Exploration**:
        - **Generate all possible mapping relationships** between CSV1 columns and CSV2 columns.
        - For **each column** in CSV1, explore and list **every possible relationship** with **one or more columns** in CSV2.
        - **Consider all columns in both CSV1 and CSV2** for possible mappings, ensuring **no column is left out**.

    3. **Strict Format Requirements**:
        - The output must be strictly in the **following JSON format**:
        ```json
        {
            "mapped_columns": [
                {"CSV1_column1": ["CSV2_column1"]},
                {"CSV1_column2": ["CSV2_column2", "CSV2_column3"]},
                ...
            ]
        }
        ```
        - Do not include any explanations, commentary, or extra text in the output.

    4. **Consistency**:
        - The mappings must explore **all possible relationships**.
        - **Maintain consistent formatting**.

    5. **Final Output**:
        - The JSON mapping should contain **only** the required mapping structure.
        - **Ensure all mappings are accounted for**.

    Ensure that mappings are strictly between columns of CSV1 (source) and CSV2 (target). Do not create mappings between columns within CSV1 or within CSV2.
    It is mandatory to cover all columns of CSV1 and CSV2 rather than missing anyone.
    """

    print("\nPrompt built successfully.")
    return prompt


# Call OpenAI API using requests
def get_openai_response(prompt):
    try:
        print("\nSending prompt to model ...")
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {API_KEY}"
        }
        data = {
            "model": "gpt-4.1",
            "messages": [
                {"role": "system", "content": "You are an expert data mapper that generates JSON mapping output only."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 4096,
            "temperature": 0.2
        }
        response = requests.post(API_URL, headers=headers, json=data)

        if response.status_code == 200:
            result = response.json()
            print("Response received.")
            return result["choices"][0]["message"]["content"]
        else:
            print(f"API call failed: {response.status_code}\n{response.text}")
            return ""
    except Exception as e:
        print(f"Request exception: {e}")
        return ""

def save_json_to_csv(json_text, output_file):
    try:
        data = json.loads(json_text.strip('```json').strip('```'))
        mappings = data.get("mapped_columns", [])

        rows = []
        for mapping in mappings:
            for source_col, target_cols in mapping.items():
                target_string = ", ".join(target_cols) if target_cols else ""
                rows.append([source_col, target_string])

        with open(output_file, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["Source_Column", "Mapped_Target_Column"])
            writer.writerows(rows)

        print(f"Output saved to {output_file}")
    except Exception as e:
        print(f"Failed to save output: {e}")

def calculate_mapping_statistics(source_df, target_df, json_text):
    try:
        data = json.loads(json_text.strip('```json').strip('```'))
        mappings = data.get("mapped_columns", [])

        mapped_targets = []
        for mapping in mappings:
            for _, targets in mapping.items():
                mapped_targets.extend(targets)

        unique_mapped_targets = set(mapped_targets)

        source_column_count = source_df.shape[0]
        target_column_count = target_df.shape[0]
        unique_mapped_count = len(unique_mapped_targets)
        percentage_mapped = (unique_mapped_count / target_column_count) * 100 if target_column_count else 0

        print("\nMapping Statistics:")
        print(f"Number of columns in source: {source_column_count}")
        print(f"Number of columns in target: {target_column_count}")
        print(f"Count of unique columns which are mapped: {unique_mapped_count}")
        print(f"Percentage of target columns mapped: {percentage_mapped:.2f}%")
        
        return {
            "source_column_count": source_column_count,
            "target_column_count": target_column_count,
            "unique_mapped_count": unique_mapped_count,
            "percentage_mapped": round(percentage_mapped, 2)
        }
    except Exception as e:
        print(f"Failed to calculate mapping stats: {e}")
        return {}

def calculate_mapping_statistics_s3(source_df, target_df, json_text):
    stats = calculate_mapping_statistics(source_df, target_df, json_text)
    
    # Create a formatted statistics string for reporting
    stats_text = f"""
        Mapping Statistics Report:
        =========================
        Number of columns in source: {stats.get('source_column_count', 'N/A')}
        Number of columns in target: {stats.get('target_column_count', 'N/A')}
        Count of unique columns which are mapped: {stats.get('unique_mapped_count', 'N/A')}
        Percentage of target columns mapped: {stats.get('percentage_mapped', 'N/A')}%
        """
    
    # Add the formatted text to the stats dictionary
    stats['report_text'] = stats_text
    return stats
