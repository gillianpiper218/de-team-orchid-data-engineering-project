resource "aws_sns_topic" "sns_alerts" {
  name = "${var.sns_alert_topic_name}"
}

locals {
  emails = [ "souad.alkhaledi@gmail.com", 
  "anita.amena@icloud.com", 
  "ellen12008@hotmail.co.uk", 
  "gillianpiper218@btinternet.com" 
  ]
}

resource "aws_sns_topic_subscription" "lambda_error_subs" {
  count = length(local.emails)
  protocol = "email"
  endpoint = local.emails[count.index]
  topic_arn = aws_sns_topic.sns_alerts.arn 
  depends_on = [ aws_sns_topic.sns_alerts ]
}

resource "aws_cloudwatch_metric_alarm" "ingestion_lambda_alarm" {
    alarm_name = "IngestionLambdaCloudwatchAlarm"
    namespace = "IngestionLambdaLogErrors"
    metric_name = "ErrorSampleCount"
    comparison_operator = "GreaterThanOrEqualToThreshold"
    evaluation_periods = 1
    threshold = 1
    statistic = "SampleCount"
    period = 120
    alarm_description = "This triggers an alarm when the ingestion lambda function errors"
    alarm_actions = [ "arn:aws:sns:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:${var.sns_alert_topic_name}" ]
}

resource "aws_cloudwatch_log_metric_filter" "ingestion_lambda_error_filter" {
	name = "IngestionLambdaErrorFilter"
	pattern = "Error"
	log_group_name = "/aws/lambda/${var.ingestion_function_name}"
	
	metric_transformation {
		name = "ErrorSampleCount"
		namespace = "IngestionLambdaLogErrors"
		value = "1"
		}
    depends_on = [ aws_cloudwatch_log_group.ingestion_function_log_group ]
	}

resource "aws_cloudwatch_metric_alarm" "processing_lambda_alarm" {
    alarm_name = "ProcessingLambdaCloudwatchAlarm"
    namespace = "ProcessingLambdaLogErrors"
    metric_name = "ErrorSampleCount"
    comparison_operator = "GreaterThanOrEqualToThreshold"
    evaluation_periods = 1
    threshold = 1
    statistic = "SampleCount"
    period = 120
    alarm_description = "This triggers an alarm when the processing lambda function errors"
    alarm_actions = [ "arn:aws:sns:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:${var.sns_alert_topic_name}" ]
}

resource "aws_cloudwatch_log_metric_filter" "processing_lambda_error_filter" {
	name = "ProcessingLambdaErrorFilter"
	pattern = "Error"
	log_group_name = "/aws/lambda/${var.processing_function_name}"
	
	metric_transformation {
		name = "ErrorSampleCount"
		namespace = "ProcessingLambdaLogErrors"
		value = "1"
		}
    depends_on = [ aws_cloudwatch_log_group.processing_function_log_group ]
	}


