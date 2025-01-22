resource "google_monitoring_alert_policy" "xcom_table_size_alert" {
  project = var.project_id
  display_name = "Composer XCOM table size"
  documentation {
    content = "The $${metric.display_name} of the composer environment $${resource.label.environment} in project $${resource.project} has exceeded the configured threshold (${var.composer_xcom_table_size_threshold} bytes). This might prevent operations such as snapshotting and upgrade from successful completition."
  }
  combiner     = "OR"
  conditions {
    display_name = "XCOM table above threshold"
    condition_threshold {
        comparison = "COMPARISON_GT"
        duration = "0s"
        filter = "resource.type = \"global\" AND metric.type = \"custom.googleapis.com/total_xcom_table_size_bytes\""
        threshold_value = var.composer_xcom_table_size_threshold
        trigger {
          count = "1"
        }
    }
  }

  alert_strategy {
    notification_channel_strategy {
        renotify_interval = var.renotification_interval
        notification_channel_names = [google_monitoring_notification_channel.email.name]
    }
  }

  notification_channels = [google_monitoring_notification_channel.email.name]

  user_labels = {
    severity = "warning"
  }
}

resource "google_monitoring_notification_channel" "email" {
  project = var.project_id
  enabled = true
  display_name = "Notify composer team"
  type = "email"
  labels = {
    email_address = var.notification_channel_email_address
  }
}