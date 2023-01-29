# Initialize state file (.tfstate)
terraform init

# Check changes to new infra plan
terraform plan -var="project=awesome-tensor-375009"
terraform apply -var="project=awesome-tensor-375009"
