import pyspark
from pyspark.sql.functions import col, sum, countDistinct, when, max, round, count
from pyspark.sql.window import Window


def task1(dataframe: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    """
    Calculate percent of logged-in user per day
    percent = logged-in-users / total-number-of-users-in-particular-day * 100

    """
    # get only logged-in users
    df_logged_in = dataframe.filter(
        col('is_logged_in') == True
    ).groupBy('date').agg(countDistinct(col('user_id')).alias('logged_in'))

    # get total users
    df = dataframe.groupBy('date').agg(countDistinct(col('user_id')).alias('total'))

    # join and calculate percentage
    df = df.join(df_logged_in, on='date').withColumn(
        'percent_logged_in [%]', round(col('logged_in') / col('total') * 100, 3)
    ).drop('total').drop('logged_in')

    return df.orderBy('date')


def task2(dataframe: pyspark.sql.dataframe.DataFrame) -> str:
    """
    Calculate site with the most logged-in users
    df -> filter only logged-in -> group by site -> count distinct users -> order by desc -> take first row -> take site

    """
    df = dataframe.filter(col('is_logged_in') == True).groupBy('site').agg(
        countDistinct('user_id').alias('user_id_distinct')
    ).orderBy(col('user_id_distinct').desc())

    return df.first()['site']


def task3(dataframe: pyspark.sql.dataframe.DataFrame) -> str:
    """
    Calculate percent of logged-in users on mobile app
    percent = logged-in-users / total-number-of-users-on-mobile-app

    --------------------------------------------
    I'm not sure if I understand the task correctly. The other option is to calculate percent of:
    logged-in users on mobile app / total number of users; in this case the result is calculated as below:

    df.filter(df.is_mobile_app == True).filter(df.is_logged_in == True).count() / df.count() * 100
    where df is dataframe for inventory.parquet file

    result 4.73%

    """
    df_mobile_app = dataframe.filter(dataframe.is_mobile_app == True)
    percent = df_mobile_app.filter(df_mobile_app.is_logged_in == True).count() / df_mobile_app.count() * 100
    return '{:.2f} %'.format(percent)


def task4(dataframe: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    """
    Create a new column identity_type based on device_type and is_mobile_app columns

    """
    df_new = dataframe.withColumn(
        "identity_type",
        when((col("device_type") == "Mobile Phone") & (col("is_mobile_app") == True), "Mobile Phone App").
        when((col("device_type") == "Mobile Phone") & (col("is_mobile_app") == False), 'Mobile Phone Web').
        when((col("device_type") == "Desktop"), 'Desktop').
        otherwise("Unknown")
    )

    return df_new


def task5(dataframe: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    """
    Calulate max order_id for each identity_type; use window function to fill new column max_order_id and keep
    original dataframe shape

    dataframe is the result of task4 function

    """
    window_func = Window.partitionBy("identity_type")
    return dataframe.withColumn("max_order_id", max(col("order_id")).over(window_func))


def task6(
        dataframe_inventory: pyspark.sql.dataframe.DataFrame, dataframe_users: pyspark.sql.dataframe.DataFrame
) -> pyspark.sql.dataframe.DataFrame:
    """
    Calculate total clicks for people in campaign per day

    """
    df = dataframe_inventory.join(
        dataframe_users, on="user_id", how="inner"
    ).filter(col('event') == 'click').groupBy('date').agg(
        count(col('event')).alias('total clicks for people in campaign')
    )

    return df.orderBy(col('date'))


def task7(
        dataframe_inventory: pyspark.sql.dataframe.DataFrame, dataframe_users: pyspark.sql.dataframe.DataFrame
) -> pyspark.sql.dataframe.DataFrame:
    """
    Calculate total clicks for people not in campaign per day

    """
    df_login = dataframe_inventory.join(
        dataframe_users, on="user_id", how="inner"
    ).filter(col('event') == 'click').groupBy('date').agg(count(col('event')).alias('login'))

    df_total = dataframe_inventory.join(
        dataframe_users, on="user_id", how="outer"
    ).filter(col('event') == 'click').groupBy('date').agg(count(col('event')).alias('total'))

    df = df_total.join(df_login, on='date').withColumn(
        'ptotal clicks for people not in campaign', col('total') - col('login')
    ).drop('total').drop('login')

    return df.orderBy('date')


if __name__ == '__main__':
    pass
