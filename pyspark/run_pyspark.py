from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand
import argparse

import tasks


def main(task_id: int, filepath_inventory: Path, filepath_users: Path):

    # -------- check files --------
    if not filepath_inventory.exists():
        raise FileNotFoundError(f'File {filepath_inventory} not found')
    if not filepath_users.exists():
        raise FileNotFoundError(f'File {filepath_users} not found')

    # -------- create spark session --------
    spark = SparkSession.builder.appName("sparkTasks").getOrCreate()
    print()

    # -------- read csv files --------
    df_inventory = spark.read.parquet(str(filepath_inventory))
    df_users = spark.read.parquet(str(filepath_users))

    # -------- run task --------
    match task_id:
        case 1:
            print('-------- task 1 --------')
            df_result = tasks.task1(df_inventory)
            df_result.show(10)

        case 2:
            print('-------- task 2 --------')
            result = tasks.task2(df_inventory)
            print(result)

        case 3:
            print('-------- task 3 --------')
            result = tasks.task3(df_inventory)
            print(result)

        case 4:
            print('-------- task 4 --------')
            df_result = tasks.task4(df_inventory)
            df_result.show(10)

        case 5:
            print('-------- task 5 --------')
            print(f'Original dataframe shape: {df_inventory.count()}')
            df_result = tasks.task5(tasks.task4(df_inventory))
            print(f'Result dataframe shape:   {df_result.count()}')
            df_result.orderBy(rand()).show(10)          # use rand() to shuffle rows

        case 6:
            print('-------- task 6 --------')
            df_result = tasks.task6(df_inventory, df_users)
            df_result.show(10)

        case 7:
            print('-------- task 7 --------')
            df_result = tasks.task7(df_inventory, df_users)
            df_result.show(10)

        case _:
            raise ValueError(f'Invalid task_id: {task_id}')


def create_argpaser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--task', type=int, required=True, help='task id')
    parser.add_argument(
        '-i', '--inventory', type=str, required=False, default='data/inventory.parquet',
        help='filepath to inventory parquet'
    )
    parser.add_argument(
        '-u', '--users', type=str, required=False, default='data/selected_users.parquet',
        help='filepath to users parquet'
    )
    return parser


if __name__ == '__main__':
    parser = create_argpaser()
    args = parser.parse_args()

    main(args.task, Path(args.inventory), Path(args.users))
