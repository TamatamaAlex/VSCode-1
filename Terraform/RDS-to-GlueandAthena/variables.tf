variable "region" {
    description = "The region in which the resources will be created"
    type        = string
    default     = "ap-northeast-1"
}

variable "rawbucket" {
  description = "The name of the S3 bucket to store raw data"
  type        = string
  default     = "test-alex-terraformbucket2"
}

variable "combinedbucket" {
  description = "The name of the S3 bucket to store combined data"
  type        = string
  default     = "test-alex-terraformbucket"
}

variable "scriptstore" {
  description = "The name of the S3 bucket to store glue scripts"
  type        = string
  default     = "test-alex-terraformbucket3"
}

variable "pathtoscript"{
    description = "The path to the script in the S3 bucket"
    type        = string
    default     = "example.py"
}