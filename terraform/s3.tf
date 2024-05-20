resource "aws_s3_bucket" "ingestion_s3_bucket" {
    bucket = "${var.ingestion_s3_bucket_name}"
    lifecycle {
    prevent_destroy = true
}
}

resource "aws_s3_bucket_lifecycle_configuration" "bucket-lifecycle" {
  bucket = aws_s3_bucket.ingestion_s3_bucket.bucket


  rule {
    status = "Enabled"
    id = "file-lifespan"

    expiration {
      days = 90
    }
  }
}

resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "layer-code"
}

resource "aws_s3_object" "layer_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key = "lambda_layer.zip"
  source = "${path.module}/../lambda_layer/lambda_layer.zip"
}


resource "aws_lambda_layer_version" "dependancies" {
  layer_name = "dependacnies"
  compatible_runtimes = ["python3.11"]
  s3_bucket = aws_s3_bucket.code_bucket.bucket
  s3_key = "lambda_layer.zip"
}
