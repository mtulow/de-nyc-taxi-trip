terraform {
  required_providers {
    snowflake = {
      source = "Snowflake-Labs/snowflake"
      version = "0.53.0"
    }
  }
}

provider "snowflake" {
  # Configuration options
}