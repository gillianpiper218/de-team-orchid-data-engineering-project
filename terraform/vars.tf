variable "ingestion_function_name" {
    type = string
    default = "ingestion_function"
}


variable "ingestion_s3_bucket_name" {
    type = string
    default = "de-team-orchid-totesys-ingestion"
}

variable "processed_s3_bucket_name" {
    type = string
    default = "de-team-orchid-totesys-processed"
}