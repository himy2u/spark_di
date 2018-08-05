package com.di.utility

import org.apache.spark.sql.{DataFrame, SaveMode}

object UtilityRedshift {
def write (df: DataFrame, tempdir : String, redshiftJdbcUrl : String, tableName:String  ) ={

    df.write
      .format("com.databricks.spark.redshift")
      .option(Constants.url, redshiftJdbcUrl)
      .option("tempdir", tempdir)
      .option("forward_spark_s3_credentials", "true")
      .option("extracopyoptions", "EMPTYASNULL")
      .option("tempformat", "CSV")
      .option("csvnullstring", "@NULL@")
      .option("dbtable", tableName)
      .mode(SaveMode.Overwrite)
      .save()

  }
}
