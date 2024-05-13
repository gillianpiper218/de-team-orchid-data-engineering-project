data "aws_iam_policy_document" "s3_ingestion_put_policy_document" {
    statement {
        actions = ["s3:PutObject"]
        resources = [
            "${aws_s3_bucket.ingestion_s3_bucket.arn}/*"
        ]         
    }
}

data "aws_iam_policy_document" "s3_ingestion_get_policy_document" {
    statement {
        actions = ["s3:GetObject"]
        resources = [
            "${aws_s3_bucket.ingestion_s3_bucket.arn}/*"
        ]         
    }
}

resource "aws_iam_role" "s3_put_access" {
    name = "lambda_put_object_role"
    assume_role_policy = data.aws_iam_policy_document.s3_ingestion_put_policy_document.trustpolicy.json
  
}

resource "aws_iam_policy" "s3_ingestion_put_policy" {
    name = "s3_ingestion_put_policy"
    policy = data.aws_iam_policy_document.s3_ingestion_put_policy_document
  
}

resource "aws_iam_role_policy_attachment" "attach_s3_put_access" {
    role = aws_iam_role.s3_put_access.name
    policy_arn = data.aws_iam_policy.s3_ingestion_put_policy.arn
}


resource "aws_iam_role" "s3_get_access" {
    name = "lambda_get_object_role"
    assume_role_policy = data.aws_iam_policy_document.s3_ingestion_get_policy_document.trustpolicy.json
  
}

resource "aws_iam_policy" "s3_ingestion_get_policy" {
    name = "s3_ingestion_get_policy"
    policy = data.aws_iam_policy_document.s3_ingestion_get_policy_document
  
}

resource "aws_iam_role_policy_attachment" "attach_s3_get_access" {
    role = aws_iam_role.s3_get_access.name
    policy_arn = data.aws_iam_policy.s3_ingestion_get_policy.arn
}


data "aws_iam_role_policy_document" "trust_policy" {
    statement {
        effect = "Allow"
        principals = {
            type = "Service"
            identifiers = ["lambda.amazonaws.com"]
        } 
        actions = ["sts:AssumeRole"]
    }
}