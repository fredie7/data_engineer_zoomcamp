import sys
import pandas as pd
print("arguments",sys.argv)
month = int(sys.argv[1])
print(f"arguments, month={month}")
print("hello")

df = pd.DataFrame(
    {
        "day":[1,2],
        "num_of_passengers":[10,20]
    },
)
df['month'] = month
print(df)
df.to_parquet(f"output_data_{sys.argv[1]}.parquet")