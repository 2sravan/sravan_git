import dlt

#Sales Expectations
sales_rules = {
    "rule_1" : "sales_id IS NOT NULL"
}

#Empty streaming table
dlt.create_streaming_table(
    name = "sales_stg",
    expect_all_or_drop = sales_rules
)


#Create East Sales flow
@dlt.append_flow(target = "sales_stg")
def east_sales():

    df = spark.readStream.table("mydatabricks.source.sales_east")
    return df

#Create West Sales flow
@dlt.append_flow(target = "sales_stg")
def west_sales():

    df = spark.readStream.table("mydatabricks.source.sales_west")
    return df
    