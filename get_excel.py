from pandas import *
from parser_bb.database import *

df: DataFrame = read_sql(
        select(ProductModel), engine.connect(), index_col="article"
    )
df.to_excel("data/results.xlsx", sheet_name="result")