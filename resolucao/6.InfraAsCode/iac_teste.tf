variable "regions" {
  type = map(string)
  default = {
    dev  = "us-east-1"
    hom  = ""
    prod = ""
  }
}

variable "aws_acc_id" {
  type      = map(string)
  sensitive = true
}

variable "job_name_spark_test_glue5" {
  default = "spark_test_glue5"
}

# Criação da role do Glue automaticamente por ambiente
resource "aws_iam_role" "glue_service_role" {
  name = "AWSGlueServiceRole-${terraform.workspace}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = {
        Service = "glue.amazonaws.com"
      },
      Action = "sts:AssumeRole"
    }]
  })
}

# Anexar a política gerenciada padrão do Glue
resource "aws_iam_role_policy_attachment" "glue_service_role_attach" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Módulo S3 para armazenar scripts e dados do Glue
module "s3_bucket_glue_resources" {
  source = "terraform-aws-modules/s3-bucket/aws"

  bucket = "aws-glue-assets-${var.aws_acc_id[terraform.workspace]}"
  acl    = "private"

  control_object_ownership = true
  object_ownership         = "ObjectWriter"

  versioning = {
    enabled = true
  }
}

# Upload de script Glue para o S3
resource "aws_s3_object" "glue_script" {
  bucket       = module.s3_bucket_glue_resources.s3_bucket_id
  key          = "scripts/${var.job_name_spark_test_glue5}.py"
  source       = "${path.module}/scripts_etl/spark_test_glue5.py"
  content_type = "application/x-python"
  etag         = filebase64sha256("${path.module}/scripts_etl/spark_test_glue5.py")
}

# Upload de CSV de teste para o S3
resource "aws_s3_object" "csv_data" {
  bucket       = module.s3_bucket_glue_resources.s3_bucket_id
  key          = "DATA/clientes_sinteticos.csv"
  source       = "${path.module}/scripts_etl/clientes_sinteticos.csv"
  content_type = "text/csv"
  etag         = filebase64sha256("${path.module}/scripts_etl/clientes_sinteticos.csv")
}

# Grupo de logs do CloudWatch
resource "aws_cloudwatch_log_group" "LOG_spark_test_glue5" {
  name              = var.job_name_spark_test_glue5
  retention_in_days = 14
}

# Job AWS Glue
resource "aws_glue_job" "spark_test_glue5" {
  name              = "${var.job_name_spark_test_glue5}-${terraform.workspace}"
  role_arn          = aws_iam_role.glue_service_role.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 10
  timeout           = 600

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--S3PATH"                        = "s3://${module.s3_bucket_glue_resources.s3_bucket_id}/DATA/clientes_sinteticos.csv"
    "--enable-spark-ui"              = "true"
    "--spark-event-logs-path"        = "s3://${module.s3_bucket_glue_resources.s3_bucket_id}/sparkHistoryLogs/"
    "--continuous-log-logGroup"      = aws_cloudwatch_log_group.LOG_spark_test_glue5.name
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = "true"
    "--TempDir"                          = "s3://${module.s3_bucket_glue_resources.s3_bucket_id}/temporary/"
    "--enable-glue-datacatalog"         = "true"
  }

  command {
    script_location = "s3://${module.s3_bucket_glue_resources.s3_bucket_id}/scripts/${var.job_name_spark_test_glue5}.py"
  }

  tags = {
    Nome  = "projeto"
    Valor = "teste_eng_dados"
  }
}
