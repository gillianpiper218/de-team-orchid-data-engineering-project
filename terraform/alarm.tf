resource "aws_cloudwatch_metric_alarm" "lambda_error_alarm" {
  alarm_name                = "terraform-lambda-error-alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 2
  metric_name               = "Errors"
  namespace                 = "AWS/Lambda"
  period                    = 120
  statistic                 = "Sum"
  threshold                 = 80
  alarm_description         = "This Alarm is for when lambda function gives an error"
  insufficient_data_actions = []
}


resource "aws_sns_topic" "sns_alerts" {
  name_prefix = "${var.sns_alert_topic_name}"
}

locals {
  emails = ["souad.alkhaledi@gmail.com","anita.amena@icloud.com"]
}



resource "aws_sns_topic_subscription" "lambda_error_subs" {
  count = length(local.emails)
  protocol = "email"
  endpoint =local.emails[count.index]
  topic_arn = aws_sns_topic.sns_alerts.arn 
}