resource "aws_s3_bucket" "ingestion_s3_bucket" {
    bucket = "${var.ingestion_s3_bucket_name}"
    lifecycle {
    prevent_destroy = true
}
}

resource "aws_s3_bucket_lifecycle_configuration" "bucket-lifecycle" {
  bucket = aws_s3_bucket.ingestion_s3_bucket.id

  rule {
    status = "Enabled"
    id = "file-lifespan"

    expiration {
      days = 90
    }
  }
}


resource "aws_s3_bucket" "processed_s3_bucket" {
    bucket = "${var.processed_s3_bucket_name}"
}
