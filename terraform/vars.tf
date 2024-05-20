variable "ingestion_function_name" {
    type = string
    default = "lambda-ellen"
}


variable "ingestion_s3_bucket_name" {
    type = string
    default = "de-team-orchid-totesys-ingestion"
}

variable "DB_USER" {
  type        = string
  description = "Username of the totesys database"
  sensitive   = true
}

variable "DB_PASSWORD" {
  type        = string
  description = "Password for the totesys database"
  sensitive   = true
}

variable "DB_NAME" {
  type        = string
  description = "totesys database"
  sensitive   = true
}

variable "DB_HOST" {
  type        = string
  description = "Host for totesys database"
  sensitive   = true
}

variable "DB_PORT" {
  type        = string
  description = "Port for the totesys database"
  sensitive   = true
}





