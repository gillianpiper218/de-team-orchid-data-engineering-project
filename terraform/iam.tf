# Ingestion Function role and S3 permissions

resource "aws_iam_role" "ingestion_function_role" {
    name_prefix = "role-${var.ingestion_function_name}"
    assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

data "aws_iam_policy_document" "trust_policy" {
    statement {
        effect = "Allow"
        principals {
            type = "Service"
            identifiers = ["lambda.amazonaws.com"]
        } 
        actions = ["sts:AssumeRole"]
    }
}

data "aws_iam_policy_document" "s3_ingestion_policy_document" {
    statement {
        actions = ["s3:GetObject", "s3:ListBucket", "s3:DeleteObject","s3:PutObject"]
        resources = [
            "${aws_s3_bucket.ingestion_s3_bucket.arn}/*",
            "${aws_s3_bucket.ingestion_s3_bucket.arn}"
        ]         
    }
}

resource "aws_iam_policy" "s3_ingestion_policy" {
    name = "s3_ingestion_policy"
    policy = data.aws_iam_policy_document.s3_ingestion_policy_document.json
  
}

resource "aws_iam_role_policy_attachment" "attach_s3_ingestion_policy" {
    role = aws_iam_role.ingestion_function_role.name
    policy_arn = aws_iam_policy.s3_ingestion_policy.arn
}

# Ingestion lambda Cloudwatch Permissions

data "aws_iam_policy_document" "cw_document" {
  statement {

    actions = [ "logs:CreateLogGroup" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.ingestion_function_name}:*"
    ]
  }

  statement {

    actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.ingestion_function_name}:*"
    ]
  }
}

resource "aws_iam_policy" "cw_policy" {
    name_prefix = "cw-policy-${var.ingestion_function_name}"
    policy = data.aws_iam_policy_document.cw_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
    role = aws_iam_role.ingestion_function_role.name
    policy_arn = aws_iam_policy.cw_policy.arn
}

