# schibsted-interview

## 1. SQL

### 1.1
```
select date, campaign_id from
(select date(hour) as date, campaign_id, sum(impressions) from campaign_stats group by date(hour), campaign_id order by sum(impressions) desc)
group by date;

```

### 1.2
```to do```

### 1.3
```to do```

### 1.4
```to do```

### 1.5
```to do```

### 1.6
```to do```

## 2. Python

used python version: ```3.9.9``` <br>
install requirements: ```pip install -r requirements.txt``` <br>

### to run: <br>
```python3 main.py -i path/to/input/csv/file.csv```

#### example: 
```python3 main.py -i data/sales_report_input.csv```

#### args:
- `-i / --input` fileptah to input csv
- `-o / --output` filepath to output csv
- `-s / --separator` csv separator; default: `;`
- `-k / --key` api key



