# schibsted-interview
python version: ```3.12.0``` <br>


## 1. pyspark
enter pyspark folder:
```shell
cd pyspark
```

establish venv:
```shell
python3 -m venv venv
source venv/bin/activate
```

install requirements: 
```shell
pip install poetry
poetry install
```

example run: <br>
```shell
python3 run_pyspark.py -t 1
```

### args
- `-t / --task` task number; default: `1`
- `-i / --inventory` fileptah to inventory parquet; defalt `data/inventory.parquet`
- `-u / --users` fileptah to inventory parquet; defalt `data/selected_users.parquet`


## 2. python
enter python folder:
```shell
cd python
```

establish venv:
```shell
python3 -m venv venv
source venv/bin/activate
```

install requirements: 
```shell
pip install -r requirements.txt
``` 

### to run: <br>
```python run_python.py -i path/to/input/csv/file.csv```

#### example: 
```python run_python.py -i data/sales_report_input.csv```

#### args:
- `-i / --input` fileptah to input csv
- `-o / --output` filepath to output csv
- `-s / --separator` csv separator; default: `;`
- `-k / --key` api key

on default settings ```run_python.py``` save result csv in ```data``` folder <br>
example output: ```data/sales_report_output_20220919_020001.csv```
