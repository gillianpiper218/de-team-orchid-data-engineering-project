provider "aws" {
    region = "eu-west-2"
}

terraform{
    backend "s3"{
        bucket = "orchid-terraform-state-bucket-nc"
        key = ".terraform/terraform.tfstate"
        region = "eu-west-2"
    }
}