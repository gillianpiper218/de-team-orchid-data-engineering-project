resource "aws_lambda_function" "ingestion_function" {
    function_name = var.ingestion_function_name
    filename = data.archive_file.lambda.output_path
    role = aws_iam_role.ingestion_function_role.arn
    handler = "ingestion_lambda.lambda_handler"
    source_code_hash = data.archive_file.lambda.output_base64sha256

    depends_on    = [aws_cloudwatch_log_group.ingestion_function_log_group]
    layers = [
    "arn:aws:lambda:eu-west-2:770693421928:layer:Klayers-p311-boto3:10",
    "arn:aws:lambda:eu-west-2:770693421928:layer:Klayers-p311-numpy:6",
    "arn:aws:lambda:eu-west-2:770693421928:layer:Klayers-p311-pandas:10",
    aws_lambda_layer_version.modules.arn
]
    runtime = "python3.11"
  }

data "archive_file" "lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/ingestion_lambda.py"
  output_path = "${path.module}/../function.zip"
}


resource "aws_lambda_layer_version" "modules" {
  filename   = "${path.module}/../modules.zip"
  layer_name = "modules"
  compatible_runtimes = ["python3.11"]

  source_code_hash = filebase64sha256("${path.module}/../modules.zip")
}




resource "aws_lambda_permission" "allow_eventbridge" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion_function.function_name
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}



resource "aws_iam_policy" "lambda_layer_policy" {
  name        = "LambdaLayerPolicy"
  description = "Policy to allow Lambda function access to Lambda layers"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = [
        "lambda:GetLayerVersion",
        "lambda:GetLayerVersionPolicy",
        "lambda:ListLayerVersions"
      ],
      Resource = "*"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_layer_policy_attachment" {
  role       = aws_iam_role.ingestion_function_role.name
  policy_arn = aws_iam_policy.lambda_layer_policy.arn
}

# resource "aws_iam_role" "lambda_exec_role" {
#   name               = "lambda_exec_role"
#   assume_role_policy = jsonencode({
#     Version   = "2012-10-17",
#     Statement = [{
#       Effect    = "Allow",
#       Principal = {
#         Service = "lambda.amazonaws.com"
#       },
#       Action    = "sts:AssumeRole"
#     }]
#   })
# }

resource "aws_iam_policy" "secret_manager_policy" {
  name = "sm_access_permissions"
  description = "policy to allow lambda to retrieve secret"
 

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "secretsmanager:GetSecretValue",
        ]
        Effect   = "Allow"
        Resource = "*"
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "secret_manager_policy_attachment" {
  role       = aws_iam_role.ingestion_function_role.name
  policy_arn = aws_iam_policy.secret_manager_policy.arn
}


