resource "docker_image" "grafana" {
  name = "grafana/grafana-oss:11.6.5"
}

resource "docker_container" "grafana" {
  name = "grafana_server"
  image = docker_image.grafana.image_id

  restart = "unless-stopped"

  ports {
    internal = var.grafana_internal
    external = var.grafana_external
  }

  networks_advanced {
    name = docker_network.monitoring.name
    aliases = ["grafana"]
  }

  env = [
    "GF_SECURITY_ADMIN_USER=${var.grafana_admin_user}",
    "GF_SECURITY_ADMIN_PASSWORD=${var.grafana_admin_password}",
    # Trusted-lab deployment: anonymous Viewer can read this Grafana org.
    "GF_SECURITY_ALLOW_EMBEDDING=true",
    "GF_AUTH_ANONYMOUS_ENABLED=true",
    "GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer",
    "GF_DASHBOARDS_MIN_REFRESH_INTERVAL=500ms"
  ] 

  //potentially add volumes here for logs or other persistent data

  mounts {
    target = "/etc/grafana/provisioning/dashboards"
    source = abspath("${path.module}/grafana/provisioning/dashboards")
    type = "bind"
  }

  mounts {
     target = "/var/lib/templates/"
     source = abspath("${path.module}/templates/")
     type = "bind"
   }

  mounts {
    target = "/var/lib/grafana/dashboards"
    source = abspath("${path.module}/grafana/dashboards")
    type   = "bind"
  }
}

provider "grafana" {
  url = var.grafana_url
  auth = "${var.grafana_admin_user}:${var.grafana_admin_password}"
}


resource "grafana_data_source" "influxdb" {
  count = var.use_influxdb ? 1 : 0

  name = "InfluxDB"
  uid = "fepg8hwzyq9dsd"
  type = "influxdb"
  
  url = "http://host.docker.internal:${var.influxdb_internal}"
  access_mode = "proxy"
  is_default = var.use_questdb ? false : true
  
  json_data_encoded = jsonencode({
    version       = "Flux"
    organization  = var.influxdb_org
    defaultBucket = var.influxdb_bucket
  })

  secure_json_data_encoded = jsonencode({
    # Grafana reads with a bucket-scoped token; acquisition keeps its writer token.
    token = var.influxdb_read_token
  })

  depends_on = [docker_container.grafana]

}


resource "grafana_data_source" "questdb" {
	count = var.use_questdb ? 1 : 0

	name = "QuestDB" 
	type = "postgres"
	uid = "fepg8hwzyq8dsd"
	username = "admin"

	url = "questdb:${var.questdb_pg_port}"
	access_mode = "proxy"
	is_default = var.use_influxdb ? false : true
	
	json_data_encoded = jsonencode({
		database = "postgres"
		sslmode = "disable"
	})

	secure_json_data_encoded = jsonencode({
		password = var.questdb_admin_password
	})
	depends_on = [docker_container.grafana]
}
