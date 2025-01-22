variable "project_id" {
  description = "GCP Project ID where to create the resources"
}
variable "notification_channel_email_address" {
  description = "Email address that must receive alerts when xcom threshold is exceeded"
}
variable "composer_xcom_table_size_threshold" {
  description = "Configurable threshold for custom metric and alerting, in bytes."
  default = 4294967296  # 4 GiB
}