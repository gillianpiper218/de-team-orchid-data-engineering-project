# Totebag Sales Data Warehouse Project

## Project Aim
The aim of this project is to extract operational sales data of totebag designs and transform it before loading it into a data warehouse. This will enable the company to run queries on the data and gain valuable insights.

## Project Setup

### Requirements
- Python 3.11.1
- AWS Account
- Terraform
- AWS CLI

### Setup

1. Make requirements, make dev-setup, make run checks:

2. Set up AWS credentials:
- export AWS_ACCESS_KEY_ID=
- export AWS_SECRET_ACCESS_KEY=
- export AWS_DEFAULT_REGION=

3. Navigate to the Terraform directory:
- to clear terraform cache, in case of errors eg creating zip file 
`rm -rf ~/.terraform.d/plugins`

4. Initialize Terraform:

5. Plan Terraform changes:

6. Apply Terraform changes:

7. Terraform destroy:
    The S3 ingestion bucket is configured to be ommitted from terraform destroy.  To run the destroy without destroying the S3 bucket you can do the following, where 'resource' is the S3 bucket'
    - terraform plan | grep <resource> | grep id 
    - terraform state rm <resource>
    - terraform destroy
    - terraform import <resource> <ID>



