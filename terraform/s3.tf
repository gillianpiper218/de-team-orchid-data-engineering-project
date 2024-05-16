resource "aws_s3_bucket" "ingestion_s3_bucket" {
    bucket = "${var.ingestion_s3_bucket_name}"
}