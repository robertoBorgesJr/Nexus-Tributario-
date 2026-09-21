variable "azure_subscription_id" {
  type        = string
  description = "ID da assinatura Azure onde o Nexus será provisionado."
}

variable "location" {
  type        = string
  description = "Região Azure dos recursos do Nexus."
  default     = "eastus2"
}

variable "environment" {
  type        = string
  description = "Ambiente do Nexus."
  default     = "prod"
}

variable "resource_group_name" {
  type        = string
  description = "Grupo de recursos compartilhado pelos recursos do Nexus."
  default     = "rg-nexus-tributario-prod"
}

variable "storage_account_name" {
  type        = string
  description = "Nome globalmente único do ADLS Gen2 do Nexus."
  default     = "stnexustributarioprod"
}

variable "workspace_resource_group_name" {
  type        = string
  description = "Grupo de recursos do workspace Databricks existente."
  default     = ""
}

variable "workspace_name" {
  type        = string
  description = "Nome do workspace Databricks existente ou a ser criado."
  default     = ""
}

variable "create_databricks_workspace" {
  type        = bool
  description = "Cria um workspace Databricks VNet-injected. Desabilitado por padrão para reutilizar o workspace existente."
  default     = false
}

variable "metastore_id" {
  type        = string
  description = "ID do metastore Unity Catalog já criado na conta Databricks."
}

variable "databricks_admin_group" {
  type        = string
  description = "Grupo Databricks que administra o catálogo Nexus."
  default     = "nexus-tributario-admins"
}

variable "databricks_data_engineer_group" {
  type        = string
  description = "Grupo Databricks que grava nas camadas Bronze e Silver."
  default     = "nexus-tributario-data-engineers"
}

variable "vnet_cidr" {
  type        = string
  default     = "10.20.0.0/16"
}

variable "private_subnet_cidr" {
  type        = string
  default     = "10.20.1.0/24"
}

variable "public_subnet_cidr" {
  type        = string
  default     = "10.20.2.0/24"
}

variable "tags" {
  type        = map(string)
  description = "Tags adicionais aplicadas aos recursos Azure."
  default     = {}
}