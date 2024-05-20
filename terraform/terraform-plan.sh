#!/bin/bash

# The properties in the .env file must be prefixed with TF_VAR_ so that Terraform picks them up.

# Load environment variables from .env file

if [ -f .env ]; then
  set -a
  . .env
else
  echo "Error: .env file not found."
  exit 1
fi

# terraform fmt
terraform plan