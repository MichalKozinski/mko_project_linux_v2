from pyspark import SparkContext, SparkConf

# Konfiguracja Sparka
conf = SparkConf().setAppName("MojaAplikacja").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Tworzenie RDD (Resilient Distributed Dataset)
rdd = sc.parallelize([1, 2, 3, 4, 5])

# Przykładowe operacje na RDD
print("Suma elementów RDD:", rdd.sum())

# Zamykanie SparkContext
sc.stop()