#Alex Day 2024/12/06
#This is the main terraform file that will create the resources needed to move data from an RDS instance to S3, and then combine the data in S3 using a Glue job.
#The data is being moved to S3, then to a glue database so that we can query it using Athena for easier analysis.
provider "aws" {
  alias  = "northeast"
  region = var.region
}


#Creating buckets with versioning to store the raw data and combined data exported from RDS, and glue scripts for the glue jobs.

resource "aws_s3_bucket" "raw_data_bucket" {
  bucket = var.rawbucket
}

resource "aws_s3_bucket_versioning" "rawversioning" {
  bucket = aws_s3_bucket.raw_data_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket" "combined_bucket" {
  bucket = var.combinedbucket
}

resource "aws_s3_bucket_versioning" "combinedversioning" {
  bucket = aws_s3_bucket.combined_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket" "store-script-bucket" {
  bucket = var.scriptstore
}

resource "aws_s3_bucket_versioning" "scripbucketversioning" {
  bucket = aws_s3_bucket.store-script-bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

#need to write in the assume role policy here. Perhaps use a variable to make it easier to read the main.tf

#Creating roles for the glue jobs to access the S3 buckets
#need to write in the assume role policy that is written above
resource "aws_iam_role" "combine_job_role" {
  name = "Glue_Job_Combine_Role"
  assume_assume_role_policy = data.
}

#Need to write the iam policies to attach to the role here. Maybe use variable to make the main.tf easier to read.

resource "aws_glue_job" "combine-job" {
  name     = "combine-tables-order"
  role_arn = aws_iam_role.combine_job_role.arn
# The script location may have to be on my local computer so I can upload it when I run the script.
# Ask Tomas if that is the best method
  command {
    script_location = "s3://${aws_s3_bucket.example.bucket}/${var.pathtoscript}"
  }
}

resource "aws_glue_workflow" "Bcart-workflow" {
  name = "Bcart-combinetables-workflow"
}

resource "aws_glue_trigger" "start-workflow" {
  name          = "trigger-start"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.Bcart-workflow.name

#Everything after this is in progress too. Need to finish the code for the glue job and the trigger.
  actions {
    job_name = aws_glue_job.example.name
  }
}
