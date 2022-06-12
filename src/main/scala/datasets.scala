import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DatasetTransformations extends App {

    //Create a spark context, using a local master so Spark runs on the local machine
    val spark = SparkSession.builder().master("local[*]").appName("DatasetTransformations").getOrCreate()

    //Importing spark implicits to use functions such as dataframe.as[T]
    import spark.implicits._

    //Set logger level to Warn
    Logger.getRootLogger.setLevel(Level.WARN)

    //Create source DataFrames
    val customerDF: DataFrame = spark.read.option("header", "true").csv("src/main/resources/customer_data.csv")
    val accountDF = spark.read.option("header", "true").csv("src/main/resources/account_data.csv")

    //Input format
    case class CustomerData(
        customerId: String,
        forename: String,
        surname: String
    )

    case class AccountData(
        customerId: String,
        accountId: String,
        balance: Long
    )

    //Output format
    case class CustomerAccountOutput(
        customerId: String,
        forename: String,
        surname: String,
        accounts: Seq[AccountData],
        numberAccounts: Int,
        totalBalance: Long,
        averageBalance: Double
    )

    val customerDS: Dataset[CustomerData] = customerDF.as[CustomerData]
    val accountDS: Dataset[AccountData] = accountDF.withColumn("balance", 'balance.cast("long")).as[AccountData]

    val accountCount: DataFrame = accountDS.groupBy(accountDS("customerID")).count().withColumnRenamed("count", "numberAccounts").withColumn("numberAccounts", 'numberAccounts.cast("int"))
    val balanceTotal: DataFrame = accountDS.groupBy(accountDS("customerID")).sum("balance").withColumnRenamed("sum(balance)", "totalBalance")
    val balanceAverage: DataFrame = accountDS.groupBy(accountDS("customerID")).avg("balance").withColumnRenamed("avg(balance)", "averageBalance")

    //Outputs "accounts" column with values of type Seq[AccountData] through groupByKey and map
    val accountsSeq = accountDS.groupByKey(x => x.customerId).mapGroups{case(key, iter) => (key, iter.map(x => x).toSeq)}.withColumnRenamed("_1", "customerId").withColumnRenamed("_2", "accounts")

    //Where there are NA values i.e. no accounts, fill with 0
    val results = DataFrame = customerDS.join(accountsSeq, Seq("customerID"), "left").join(accountCount, Seq("customerID"), "left").join(balanceTotal, Seq("customerID"), "left").join(balanceAverage, Seq("customerID"), "left").sort("customerId")
}