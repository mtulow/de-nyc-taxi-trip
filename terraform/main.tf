
resource "snowflake_database" "db" {
  name     = var.database_name
}

resource "snowflake_warehouse" "warehouse" {
  name           = var.warehouse_name
  warehouse_size = "x-small"

  auto_suspend = 60
}
