import polars as pl

batch_df =  pl.read_csv("./the_oscar_award.csv")

print(len(batch_df))

before_1975_df = batch_df.filter(
    pl.col("year_ceremony") <= 1975
)

after_1975_df = batch_df.filter(
    pl.col("year_ceremony") > 1975
)

print(len(before_1975_df))
print(len(after_1975_df))

print(before_1975_df.head(20))
print(after_1975_df.head(20))