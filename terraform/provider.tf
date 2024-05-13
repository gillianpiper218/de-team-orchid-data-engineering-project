provider "aws" {
    region = "eu-west-2"

default_tags {
        tags = {
           ProjectTeam = "Team Orchid"
           git = "https://github.com/gillianpiper218/de-team-orchid-data-engineering-project"
           DeployedFrom = "Terraform"
        }
    }
}

terraform{
    backend "s3"{
        bucket = "orchid-terraform-state-bucket-nc"
        key = ".terraform/terraform.tfstate"
        region = "eu-west-2"
    }
}

