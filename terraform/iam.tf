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

data "aws_iam_policy_document" "s3_ingestion_get_policy_document" {
    statement {
        actions = ["s3:GetObject", "s3:ListBucket", "s3:DeleteObject","s3:PutObject"]
        resources = [
            "${aws_s3_bucket.ingestion_s3_bucket.arn}/*",
            "${aws_s3_bucket.ingestion_s3_bucket.arn}"
        ]         
    }
}

data "aws_iam_policy_document" "s3_ingestion_put_policy_document" {
    statement {
        actions = ["s3:PutObject", "s3:ListBucket", "s3:DeleteObject"]
        resources = [
            "${aws_s3_bucket.ingestion_s3_bucket.arn}/*",
            "${aws_s3_bucket.ingestion_s3_bucket.arn}"
        ]        
    }
}

resource "aws_iam_policy" "s3_ingestion_get_policy" {
    name = "s3_ingestion_get_policy"
    policy = data.aws_iam_policy_document.s3_ingestion_get_policy_document.json
  
}

resource "aws_iam_policy" "s3_ingestion_put_policy" {
    name = "s3_ingestion_put_policy"
    policy = data.aws_iam_policy_document.s3_ingestion_put_policy_document.json
  
}

resource "aws_iam_role_policy_attachment" "attach_s3_get_access_ingestion" {
    role = aws_iam_role.ingestion_function_role.name
    policy_arn = aws_iam_policy.s3_ingestion_get_policy.arn
}

resource "aws_iam_role_policy_attachment" "attach_s3_put_access_ingestion" {
    role = aws_iam_role.s3_put_access_ingestion.name
    policy_arn = aws_iam_policy.s3_ingestion_put_policy.arn
}


resource "aws_iam_role" "s3_put_access_ingestion" {
    name = "lamba_put_ingestion_object_role"
    assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

# resource "aws_iam_role" "s3_put_access" {
#     name = "lambda_put_object_role"
#     assume_role_policy = data.aws_iam_policy_document.s3_ingestion_put_policy_document.trustpolicy.json
  
# }






# resource "aws_iam_role_policy_attachment" "lambda_s3_put_policy_attachment" {
#     role = aws_iam_role.ingestion_function_role.name
#     policy_arn = aws_iam_policy.s3_ingestion_put_policy.arn
# }

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


#processed s3

# data "aws_iam_policy_document" "s3_processed_get_policy_document" {
#     statement {
#         actions = ["s3:GetObject"]
#         resources = [
#             "${aws_s3_bucket.processed_s3_bucket.arn}/*"
#         ]         
#     }
# }

# resource "aws_iam_policy" "s3_processed_get_policy" {
#     name = "s3_processed_get_policy"
#     policy = data.aws_iam_policy_document.s3_processed_get_policy_document.json
  
# }


# resource "aws_iam_role_policy_attachment" "attach_s3_get_access" {
#     role = aws_iam_role.ingestion_function_role.name
#     policy_arn = aws_iam_policy.s3_ingestion_get_policy.arn
# }


# resource "aws_iam_policy" "s3_processed_put_policy" {
#     name = "s3_processed_put_policy"
#     policy = data.aws_iam_policy_document.s3_processed_put_policy_document.json
  
# }

# resource "aws_iam_role_policy_attachment" "attach_s3_put_access_ingestion" {
#     role = aws_iam_role.s3_put_access_ingestion.name
#     policy_arn = data.aws_iam_policy.s3_ingestion_put_policy.arn
# }

# resource "aws_iam_role_policy_attachment" "attach_s3_put_access_processed" {
#     role = aws_iam_role.attach_s3_put_access_processed.name
#     policy_arn = data.aws_iam_policy.s3_processed_put_policy.arn
# }


# data "aws_iam_policy_document" "s3_processed_put_policy_document" {
#     statement {
#         actions = ["s3:PutObject"]
#         resources = [
#             "${aws_s3_bucket.processed_s3_bucket.arn}/*"
#         ]        
#     }
# }