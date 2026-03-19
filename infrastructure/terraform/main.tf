# Azure Infrastructure for Trading Analytics Platform

terraform {
  required_version = ">= 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# Resource Group
resource "azurerm_resource_group" "trading" {
  name     = "rg-trading-analytics-${var.environment}"
  location = var.location
  
  tags = {
    Environment = var.environment
    Project     = "TradingAnalytics"
    ManagedBy   = "Terraform"
  }
}

# Event Hub Namespace
resource "azurerm_eventhub_namespace" "trading" {
  name                = "evhns-trading-${var.environment}"
  location            = azurerm_resource_group.trading.location
  resource_group_name = azurerm_resource_group.trading.name
  sku                 = "Standard"
  capacity            = 2
  
  tags = {
    Environment = var.environment
  }
}

# Event Hub for Market Data
resource "azurerm_eventhub" "market_data" {
  name                = "market-data"
  namespace_name      = azurerm_eventhub_namespace.trading.name
  resource_group_name = azurerm_resource_group.trading.name
  partition_count     = 4
  message_retention   = 7
}

# Event Hub for Trade Data
resource "azurerm_eventhub" "trade_data" {
  name                = "trade-data"
  namespace_name      = azurerm_eventhub_namespace.trading.name
  resource_group_name = azurerm_resource_group.trading.name
  partition_count     = 4
  message_retention   = 7
}

# Storage Account for Data Lake
resource "azurerm_storage_account" "trading" {
  name                     = "sttrading${var.environment}"
  resource_group_name      = azurerm_resource_group.trading.name
  location                 = azurerm_resource_group.trading.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true  # Enable Data Lake Storage
  
  tags = {
    Environment = var.environment
  }
}

# Storage Container for Raw Data
resource "azurerm_storage_container" "raw" {
  name                  = "raw-data"
  storage_account_name  = azurerm_storage_account.trading.name
  container_access_type = "private"
}

# Storage Container for Processed Data
resource "azurerm_storage_container" "processed" {
  name                  = "processed-data"
  storage_account_name  = azurerm_storage_account.trading.name
  container_access_type = "private"
}

# SQL Server
resource "azurerm_mssql_server" "trading" {
  name                         = "sql-trading-${var.environment}"
  resource_group_name          = azurerm_resource_group.trading.name
  location                     = azurerm_resource_group.trading.location
  version                      = "12.0"
  administrator_login          = var.sql_admin_username
  administrator_login_password = var.sql_admin_password
  
  tags = {
    Environment = var.environment
  }
}

# SQL Database
resource "azurerm_mssql_database" "trading" {
  name           = "trading-warehouse"
  server_id      = azurerm_mssql_server.trading.id
  collation      = "SQL_Latin1_General_CP1_CI_AS"
  license_type   = "LicenseIncluded"
  max_size_gb    = 250
  sku_name       = "S0"
  
  tags = {
    Environment = var.environment
  }
}

# Redis Cache for Real-time Data
resource "azurerm_redis_cache" "trading" {
  name                = "redis-trading-${var.environment}"
  location            = azurerm_resource_group.trading.location
  resource_group_name = azurerm_resource_group.trading.name
  capacity            = 1
  family              = "C"
  sku_name            = "Standard"
  enable_non_ssl_port = false
  minimum_tls_version = "1.2"
  
  redis_configuration {
    maxmemory_reserved = 100
    maxfragmentationmemory_reserved = 50
    maxmemory_delta = 50
  }
}

# Application Insights for Monitoring
resource "azurerm_application_insights" "trading" {
  name                = "appinsights-trading-${var.environment}"
  location            = azurerm_resource_group.trading.location
  resource_group_name = azurerm_resource_group.trading.name
  application_type    = "web"
  
  tags = {
    Environment = var.environment
  }
}

# Key Vault for Secrets
resource "azurerm_key_vault" "trading" {
  name                = "kv-trading-${var.environment}"
  location            = azurerm_resource_group.trading.location
  resource_group_name = azurerm_resource_group.trading.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"
  
  tags = {
    Environment = var.environment
  }
}

# Data Factory for Orchestration
resource "azurerm_data_factory" "trading" {
  name                = "adf-trading-${var.environment}"
  location            = azurerm_resource_group.trading.location
  resource_group_name = azurerm_resource_group.trading.name
  
  tags = {
    Environment = var.environment
  }
}

# Outputs
output "resource_group_name" {
  value = azurerm_resource_group.trading.name
}

output "event_hub_namespace" {
  value = azurerm_eventhub_namespace.trading.name
}

output "sql_server_fqdn" {
  value = azurerm_mssql_server.trading.fully_qualified_domain_name
}

output "redis_hostname" {
  value = azurerm_redis_cache.trading.hostname
}

output "application_insights_key" {
  value     = azurerm_application_insights.trading.instrumentation_key
  sensitive = true
}
