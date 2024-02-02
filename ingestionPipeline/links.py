import pandas as pd


data={
    "2022":{
        "january":{"yellow":"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet",
                         "green":"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet",
                         "fhv":"https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2022-01.parquet",
                         "hvfhv":"https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-01.parquet"},
         "february":{"yellow":"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-02.parquet",
                    "green":"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-02.parquet",
                    "fhv":"https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2022-02.parquet",
                    "hvfhv":"https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-02.parquet"},
        "march":{"yellow":"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-03.parquet",
                 "green":"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-03.parquet",
                  "fhv":"https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-03.parquet",
                  "hvfhv":"https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2022-03.parquet"}                
    },
    "2021":{ "january":{"yellow":"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet",
                         "green":"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2021-01.parquet",
                         "fhv":"https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2021-01.parquet",
                         "hvfhv":"https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-01.parquet"},
            "february":{"yellow":"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-02.parquet",
                    "green":"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2021-02.parquet",
                    "fhv":"https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2021-02.parquet",
                    "hvfhv":"https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-02.parquet"},
           "march":{"yellow":"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-03.parquett",
                 "green":"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2021-03.parquet",
                  "fhv":"https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2021-03.parquet",
                  "hvfhv":"https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-03.parquet"}
                  }
}


df=pd.DataFrame(data)
tlc_data=df.to_dict()
