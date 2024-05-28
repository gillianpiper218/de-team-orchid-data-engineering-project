# loading lambda role and s3 permissions

resource "aws_iam_role" "loading_function_role" {
    name_prefix = "role-${var.loading_function_name}"
    assume_role_policy = data.aws_iam_policy_document.trust_policy_processing_lambda.json
}

data "aws_iam_policy_document" "trust_policy_loading_lambda" {
    statement {
        effect = "Allow"
        principals {
            type = "Service"
            identifiers = ["lambda.amazonaws.com"]
        } 
        actions = ["sts:AssumeRole"]
    }
}

data "aws_iam_policy_document" "s3_loading_policy_document" {
    statement {
        actions = ["s3:GetObject", "s3:ListBucket", "s3:DeleteObject","s3:PutObject","s3:GetObjectTagging","s3:PutObjectTagging","s3:PutObjectAcl"]
        resources = [
            "${aws_s3_bucket.processed_s3_bucket.arn}/*",
            "${aws_s3_bucket.processed_s3_bucket.arn}"
        ]         
    }
}

resource "aws_iam_policy" "s3_loading_policy" {
    name = "s3_loading_policy"
    policy = data.aws_iam_policy_document.s3_loading_policy_document.json
  
}

resource "aws_iam_role_policy_attachment" "attach_s3_loading_policy" {
    role = aws_iam_role.loading_function_role.name
    policy_arn = aws_iam_policy.s3_loading_policy.arn
}

# loading lambda cloudwatch permissions

data "aws_iam_policy_document" "cw_loading_document" {
  statement {

    actions = [ "logs:CreateLogGroup" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.loading_function_name}:*"
    ]
  }

  statement {

    actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.loading_function_name}:*"
    ]
  }
}

resource "aws_iam_policy" "cw_loading_policy" {
    name_prefix = "cw-policy-${var.loading_function_name}"
    policy = data.aws_iam_policy_document.cw_loading_document.json
}

resource "aws_iam_role_policy_attachment" "loading_lambda_cw_policy_attachment" {
    role = aws_iam_role.loading_function_role.name
    policy_arn = aws_iam_policy.cw_loading_policy.arn 
}