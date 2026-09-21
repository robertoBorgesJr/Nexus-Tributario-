# Nexus Tributario - Terraform

Este stack provisiona a fundacao Azure e Unity Catalog do Nexus Tributario:

- ADLS Gen2 com filesystem dedicado;
- Access Connector com identidade gerenciada e RBAC `Storage Blob Data Contributor`;
- VNet com subnets delegadas ao Azure Databricks e NSG;
- workspace Databricks existente por padrao, ou novo workspace VNet-injected quando `create_databricks_workspace = true`;
- credencial, external location, metastore assignment e catalogo Unity Catalog;
- schemas `bronze`, `silver`, `gold` e `controle`;
- grants separados para administradores e engenheiros de dados.

## Uso

1. Copie `terraform.tfvars.example` para `terraform.tfvars` e preencha a assinatura, workspace e metastore.
2. Autentique o Terraform com uma identidade que tenha permissao para Azure RBAC e Databricks Account/Workspace.
3. Execute:

```powershell
terraform init
terraform fmt -check
terraform validate
terraform plan -out nexus.tfplan
terraform apply nexus.tfplan
```

Quando o workspace existente for reaproveitado, `workspace_resource_group_name` e `workspace_name` devem apontar para ele. Para criar um workspace novo, defina `create_databricks_workspace = true`; nesse caso o `workspace_name` precisa ser informado e a identidade usada pelo Terraform deve poder criar o recurso Databricks.

O ID do metastore deve ser obtido no Unity Catalog Account Console. Grupos usados nos grants precisam existir no workspace/account antes do apply.