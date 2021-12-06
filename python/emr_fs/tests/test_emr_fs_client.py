from emr_fs.feature_store import FeatureStore
from emr_fs.client import Client

if __name__ == '__main__':
   client=Client()
   #test create feature store
   emr_fs01 = client.create_feature_store("emr_fs01","s3://bigdatademobucket/emr_feature_store/emr_fs01","emr feature store test")
   #test connect to feature store
   emr_fs01 = client.connect_to_feature_store("emr_fs01")

   #test create feature group
   features01={"city_code":"int","state_code":"int","country_code":"int","dt","timestamp","customer_id","int"}
   emr_fg01 = client.create_feature_group("cust01","","customer_id","dt",features01)

   features02 = {"customer_id":"int",	"age":"int","diabetes":"int","ejection_fraction":"int",	"high_blood_pressure":"int","platelets":"int","sex":"int","smoking":"init","DEATH_EVENT":"int","dt","timestamp"}
   emr_fg02 = client.create_feature_group("cust02","","customer_id","dt",features02)

   #test feature group ingestion
   source_feature_group_dataset = "s3://emrfssampledata/feature_store_introduction_custs.csv"
   emr_fs01.ingestion()

   #test query feature group
   emr_fg01.select_all().show()
   emr_fs01.select(["customer_id","city_code","state_code"]).show(5)
   #test feature group time travel query
   emr_fs01.timeQuery("2021-01-12 00:00:00","2021-01-20 00:00:00").show()
   #test join
   emr_fs01.select(["customer_id","city_code","state_code"]).join(emr_fg02.select_all()).show(5)
   #test train dataset retrive
   emr_fg02.create_training_dataset(name = "userProfile dataset",
               data_format = "tfrecord",
               startDt="2020-10-20 07:31:38",
               endDt= "2020-10-20 07:34:11“,
               outputLoc = "s3://emr_fs/output/userProfile")







