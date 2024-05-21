resource "aws_iam_role" "processing_function_role" {
    name_prefix = "role-${var.processing_function_name}"
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


resource "aws_iam_role_policy_attachment" "processing_lambda_get_policy_attachment" {
    role = aws_iam_role.processing_function_role.name
    policy_arn = aws_iam_policy.s3_ingestion_get_policy.arn 
}




resource "aws_iam_role_policy_attachment" "processing_lambda_put_policy_attachment" {
    role = aws_iam_role.processing_function_role.name
    policy_arn = aws_iam_policy.s3_processed_put_policy.arn
}



data "aws_iam_policy_document" "cw_processing_document" {
  statement {

    actions = [ "logs:CreateLogGroup" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.processing_function_name}:*"
    ]
  }

  statement {

    actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.processing_function_name}:*"
    ]
  }
}



resource "aws_iam_policy" "cw_processing_policy" {
    name_prefix = "cw-policy-${var.processing_function_name}"
    policy = data.aws_iam_policy_document.cw_processing_document.json
}
  


resource "aws_iam_role_policy_attachment" "processing_lambda_cw_policy_attachment" {
    role = aws_iam_role.processing_function_role.name
    policy_arn = aws_iam_policy.cw_processing_policy.arn
}

