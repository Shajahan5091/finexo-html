from googleapiclient import discovery
from google.oauth2 import service_account

# Path to your service account key file
service_account_key_file = "C:/MY/ELAIT/SNOWFLAKE/POC/MIGRATION/sfdatamigration-91d4f366b4dd2.json"

# Load the service account credentials
credentials = service_account.Credentials.from_service_account_file(service_account_key_file)

# Build the IAM service
service = discovery.build('iam', 'v1', credentials=credentials)

# The resource name of the parent resource in the format 'projects/{PROJECT_ID}'
project_id = 'sfdatamigration'

# Make a request to list all custom roles in the project
request = service.projects().roles().list(parent=f'projects/{project_id}')

while True:
    # Execute the request
    response = request.execute()

    # Process each custom role
    for role in response.get('roles', []):
        print(f"Custom Role: {role['name']}")

        # Extract role details
        role_name = role['name']
        role_details = service.projects().roles().get(name=role_name).execute()

        # Extract permissions from role details
        permissions = role_details.get('includedPermissions', [])

        # Print permissions associated with the role
        print("Permissions:")
        for permission in permissions:
            print(f"- {permission}")

    # Check if there are more results
    request = service.projects().roles().list_next(
        previous_request=request,
        previous_response=response
    )
    if request is None:
        break


