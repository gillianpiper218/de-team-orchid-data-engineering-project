resource "aws_s3_bucket" "ingestion_s3_bucket" {
    bucket = "${var.ingestion_s3_bucket_name}"
    lifecycle {
    prevent_destroy = true
}
}

# attribute of resource called life-cycle
# if terraform destroy, don't get rid of historical data


