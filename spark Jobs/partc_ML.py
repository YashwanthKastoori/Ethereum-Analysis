df = sc.textFile("hdfs://andromeda.student.eecs.qmul.ac.uk/user/yk309/input")
df = df.map(lambda line: line.split(","))
Dataframe = df.map(lambda line: Row(AdrActCnt = line[1],
BlkCnt = line[2],
BlkSize = line[3],
DiffMean = line[6],
BlkSizeMean = line[4],
issCountUSD = line[15],
price = line[21])).toDF()

def convertColumn(Dataframe, names, newType):
  for name in names: 
     Dataframe = Dataframe.withColumn(name, Dataframe[name].cast(newType))
  return Dataframe 

columns = ['AdrActCnt', 'BlkCnt', 'BlkSize', 'DiffMean', 'BlkSizeMean', 'issCountUSD', 'price']

Dataframe = convertColumn(Dataframe, columns, FloatType())

from pyspark.ml.linalg import DenseVector

input_data = Dataframe.rdd.map(lambda x: (x[0], DenseVector(x[1:])))

# Replace `df` with the new DataFrame
Dataframe = spark.createDataFrame(input_data, ["label", "features"])
from pyspark.ml.feature import StandardScaler
standardScaler = StandardScaler(inputCol="features", outputCol="features_scaled")

scaler = standardScaler.fit(Dataframe)
scaled_df = scaler.transform(Dataframe)

train_data, test_data = scaled_df.randomSplit([.8,.2],seed=1234)

from pyspark.ml.regression import LinearRegression

lr = LinearRegression(labelCol="label", maxIter=10, regParam=0.3, elasticNetParam=0.8)

linearModel = lr.fit(train_data)

Got error here 

predicted = linearModel.transform(test_data)
predictions = predicted.select("prediction").rdd.map(lambda x: x[0])
labels = predicted.select("label").rdd.map(lambda x: x[0])
predictionAndLabel[:5]
linearModel.coefficients
linearModel.intercept


# Get the RMSE
linearModel.summary.rootMeanSquaredError
#The RMSE measures how much error there is between two datasets comparing a predicted value and an observed or known value. 
#The smaller an RMSE value, the closer predicted and observed values are.
#Expected 0.0440556428891

# Get the R2
linearModel.summary.r2
#The R2 (“R squared”) or the coefficient of determination is a measure that shows how close the data are to the fitted regression line. 
#This score will always be between 0 and a 100% (or 0 to 1 ), where 0% indicates that the model explains none of the variability of the response data around its mean, and 100% indicates the opposite: it explains all the variability. That means that, in general, the higher the R-squared, the better the model fits your data.














