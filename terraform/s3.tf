resource "aws_s3_bucket" "ingestion_s3_bucket" {
    bucket = "${var.ingestion_s3_bucket_name}"
}

# attribute of resource called life-cycle
# if terraform destroy, don't get rid of historical data

resource "aws_s3_bucket" "ingestion-lambda-requirements-layer" {
    bucket = "ingestion-lambda-requirements-orchid786"
}