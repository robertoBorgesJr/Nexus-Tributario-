locals {
  name_prefix = "nexus-tributario-${var.environment}"
  common_tags = merge(var.tags, {
    Project     = "Nexus Tributario"
    Environment = var.environment
    ManagedBy   = "Terraform"
  })

  workspace_resource_id = var.create_databricks_workspace ? azurerm_databricks_workspace.nexus[0].id : data.azurerm_databricks_workspace.existing[0].id
  workspace_id          = var.create_databricks_workspace ? azurerm_databricks_workspace.nexus[0].workspace_id : data.azurerm_databricks_workspace.existing[0].workspace_id
}

resource "azurerm_resource_group" "nexus" {
  name     = var.resource_group_name
  location = var.location
  tags     = local.common_tags
}

resource "azurerm_storage_account" "nexus" {
  name                            = var.storage_account_name
  resource_group_name             = azurerm_resource_group.nexus.name
  location                        = azurerm_resource_group.nexus.location
  account_kind                    = "StorageV2"
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  is_hns_enabled                  = true
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = false
  tags                            = local.common_tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "nexus" {
  name               = "nexus-data"
  storage_account_id = azurerm_storage_account.nexus.id
}

resource "azurerm_databricks_access_connector" "nexus" {
  name                = "nexus-uc-connector-${var.environment}"
  resource_group_name = azurerm_resource_group.nexus.name
  location            = azurerm_resource_group.nexus.location
  identity {
    type = "SystemAssigned"
  }
  tags = local.common_tags
}

resource "azurerm_role_assignment" "storage_contributor" {
  scope                = azurerm_storage_account.nexus.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.nexus.identity[0].principal_id
}

resource "azurerm_virtual_network" "nexus" {
  name                = "vnet-nexus-tributario-${var.environment}"
  location            = azurerm_resource_group.nexus.location
  resource_group_name = azurerm_resource_group.nexus.name
  address_space       = [var.vnet_cidr]
  tags                = local.common_tags
}

resource "azurerm_network_security_group" "databricks" {
  name                = "nsg-nexus-databricks-${var.environment}"
  location            = azurerm_resource_group.nexus.location
  resource_group_name = azurerm_resource_group.nexus.name
  tags                = local.common_tags
}

resource "azurerm_subnet" "private" {
  name                 = "nexus-private"
  resource_group_name  = azurerm_resource_group.nexus.name
  virtual_network_name = azurerm_virtual_network.nexus.name
  address_prefixes     = [var.private_subnet_cidr]
  delegation {
    name = "databricks-private"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action"
      ]
    }
  }
}

resource "azurerm_subnet" "public" {
  name                 = "nexus-public"
  resource_group_name  = azurerm_resource_group.nexus.name
  virtual_network_name = azurerm_virtual_network.nexus.name
  address_prefixes     = [var.public_subnet_cidr]
  delegation {
    name = "databricks-public"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action"
      ]
    }
  }
}

resource "azurerm_subnet_network_security_group_association" "private" {
  subnet_id                 = azurerm_subnet.private.id
  network_security_group_id = azurerm_network_security_group.databricks.id
}

resource "azurerm_subnet_network_security_group_association" "public" {
  subnet_id                 = azurerm_subnet.public.id
  network_security_group_id = azurerm_network_security_group.databricks.id
}

resource "azurerm_databricks_workspace" "nexus" {
  count                       = var.create_databricks_workspace ? 1 : 0
  name                        = var.workspace_name
  resource_group_name         = azurerm_resource_group.nexus.name
  location                    = azurerm_resource_group.nexus.location
  sku                         = "premium"
  managed_resource_group_name  = "${var.resource_group_name}-managed"
  public_network_access_enabled = true
  network_security_group_rules_required = "NoAzureDatabricksRules"
  custom_parameters {
    virtual_network_id                                   = azurerm_virtual_network.nexus.id
    public_subnet_name                                   = azurerm_subnet.public.name
    private_subnet_name                                  = azurerm_subnet.private.name
    public_subnet_network_security_group_association_id  = azurerm_subnet_network_security_group_association.public.id
    private_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.private.id
  }
  tags = local.common_tags
}

data "azurerm_databricks_workspace" "existing" {
  count               = var.create_databricks_workspace ? 0 : 1
  name                = var.workspace_name
  resource_group_name = var.workspace_resource_group_name
}

resource "databricks_storage_credential" "nexus" {
  name = "nexus_azure_storage_credential"
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.nexus.id
  }
  comment = "Credencial gerenciada para os dados tributários do Nexus."
}

resource "databricks_external_location" "nexus" {
  name            = "nexus_${var.environment}_storage"
  url             = "abfss://${azurerm_storage_data_lake_gen2_filesystem.nexus.name}@${azurerm_storage_account.nexus.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.nexus.name
  force_destroy   = false
  comment         = "Local externo ADLS Gen2 do Nexus Tributario."
  depends_on      = [azurerm_role_assignment.storage_contributor]
}

resource "databricks_catalog" "nexus" {
  name          = "nexus_tributario_${var.environment}"
  storage_root  = databricks_external_location.nexus.url
  force_destroy = false
  comment       = "Catalogo de dados tributarios do Nexus."
}

resource "databricks_schema" "layers" {
  for_each     = toset(["bronze", "silver", "gold", "controle"])
  catalog_name = databricks_catalog.nexus.name
  name         = each.value
  comment      = each.value == "controle" ? "Metadados operacionais dos pipelines." : "Camada ${each.value} da arquitetura medallion tributaria."
}

resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.nexus.name
  grant {
    principal  = var.databricks_admin_group
    privileges = ["USE_CATALOG", "CREATE_SCHEMA", "MANAGE"]
  }
  grant {
    principal  = var.databricks_data_engineer_group
    privileges = ["USE_CATALOG", "CREATE_SCHEMA"]
  }
}

resource "databricks_grants" "schemas" {
  for_each = databricks_schema.layers
  schema   = "${databricks_catalog.nexus.name}.${each.value.name}"
  grant {
    principal  = var.databricks_admin_group
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "CREATE_VOLUME", "MODIFY"]
  }
  grant {
    principal  = var.databricks_data_engineer_group
    privileges = each.value.name == "gold" ? ["USE_SCHEMA", "SELECT"] : ["USE_SCHEMA", "CREATE_TABLE", "MODIFY"]
  }
}