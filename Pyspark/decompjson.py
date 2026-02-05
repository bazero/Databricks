json_data = {"requests": {
      "channel": "store",
      "type": "API request",
      "data": {
          "serverId": 13,
          "serialNumber": "xzd145qs",
          "status": "active",
          "statusTime": "2023-12-23 21:57:25.024593",
          "transactionStatus": "complete"
        }
      }
}

##spark = SparkSession.builder.appName("extractJSON").getOrCreate()
df = spark.read.json(spark.sparkContext.parallelize([json_data]))
# use star expand to extract requests, and to select the column fields
df1 = df.selectExpr("requests.*").selectExpr("type","channel","data.*")
df1.show(truncate=False)
