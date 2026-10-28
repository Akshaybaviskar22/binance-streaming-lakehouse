import pyspark.sql.types as T

crypto_trades_schema = T.StructType([
    T.StructField("e", T.StringType()),
    T.StructField("E", T.LongType()),
    T.StructField("s", T.StringType()),
    T.StructField("t", T.LongType()),
    T.StructField("p", T.StringType()),
    T.StructField("q", T.StringType()),
    T.StructField("T", T.LongType()),
    T.StructField("m", T.BooleanType()),
    T.StructField("M", T.BooleanType())
])