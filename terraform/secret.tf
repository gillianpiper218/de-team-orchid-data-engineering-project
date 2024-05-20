resource "aws_secretsmanager_secret" "environment_secret" {
  name = "environment_secret"
}



resource "aws_secretsmanager_secret_version" "environment_version" {
  secret_id     = aws_secretsmanager_secret.environment_secret.id
  secret_string = jsonencode({default = {
    DB_USER = var.DB_USER
    DB_PASSWORD = var.DB_PASSWORD
    DB_NAME = var.DB_NAME
    DB_HOST = var.DB_HOST
    DB_PORT = var.DB_PORT
  }})
   
}