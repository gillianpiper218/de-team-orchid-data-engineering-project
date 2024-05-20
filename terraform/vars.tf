variable "ingestion_function_name" {
    type = string
    default = "lambda-ellen"
}


variable "ingestion_s3_bucket_name" {
    type = string
    default = "de-team-orchid-totesys-ingestion"
}

variable "processed_s3_bucket_name" {
    type = string
    default = "de-team-orchid-totesys-processed"
}


variable "processing_function_name" {
    type = string
    default = "lambda-ellen"
}



variable "sns_alert_topic_name" {
    type = string
    default = "LambdaErrors"
}