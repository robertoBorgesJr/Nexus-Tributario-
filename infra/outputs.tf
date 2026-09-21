output "resource_group_name" {
  value = azurerm_resource_group.nexus.name
}

output "storage_account_name" {
  value = azurerm_storage_account.nexus.name
}

output "access_connector_principal_id" {
  value = azurerm_databricks_access_connector.nexus.identity[0].principal_id
}

output "workspace_resource_id" {
  value = local.workspace_resource_id
}

output "catalog_name" {
  value = databricks_catalog.nexus.name
}

output "external_location_url" {
  value = databricks_external_location.nexus.url
}