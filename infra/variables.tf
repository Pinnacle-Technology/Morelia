variable "grafana_admin_user" {
  type = string
  description = "Admin username for Grafana"
  default = "admin"
}

variable "grafana_admin_password" {
  type = string
  description = "Admin password for Grafana"
  sensitive = true
}

variable "grafana_internal" {
  type = number
  description = "Internal port for Grafana"
}

variable "grafana_external" {
  type = number
  description = "External port for Grafana"
}


variable "grafana_url" {
  type = string
  description = "URL for Grafana instance"
  default = "http://localhost:3000"
}

variable "use_influxdb" {
  description = "Deploy and connect to InfluxDB"
  type        = bool
  default     = true
}

variable "use_questdb" {
  description = "Deploy and connect to QuestDB"
  type        = bool
  default     = false
}

variable "influxdb_admin_user" {
  type = string
  description = "Admin username for InfluxDB"
  sensitive = true
}

variable "influxdb_admin_password" {
  type = string
  description = "Admin password for InfluxDB"
  sensitive = true
}

variable "influxdb_token" {
  type = string
  description = "API token for InfluxDB"
  sensitive = true
}

variable "influxdb_read_token" {
  type        = string
  description = "Bucket-scoped read-only InfluxDB token used only by Grafana"
  sensitive   = true
}

variable "influxdb_org" {
  type = string
  description = "Organization for InfluxDB"
  default = "default-org"
}

variable "influxdb_bucket" {
  type = string
  description = "Initial bucket for InfluxDB"
  default = "default"
}

variable "influxdb_internal" {
  type = number
  description = "Internal port for InfluxDB"
}

variable "influxdb_external" {
  type = number
  description = "External port for InfluxDB"
}

variable "influxdb_url" {
  type = string
  description = "URL for InfluxDB instance"
  sensitive = true
}


variable "questdb_admin_user" { 
	type = string 
	description = "Admin username for QuestDB"
	sensitive = true
}

variable "questdb_admin_password" {
	type = string
	description = "Admin password for QuestDB"
	sensitive = true
}

variable "questdb_ui_port" {
	type = number
	description = "Internal port for QuestDB"
}

variable "questdb_pg_port" {
	type = number 
	description = "External port for QuestDB"
}

variable "questdb_ilp_port"{
	type = number
	description = "Influx Line Protocol port for QuestDB"
}
