# schibsted-interview

## 1. SQL

### 1.1
```
select date, campaign_id from
(
  select date(hour) as date, campaign_id, sum(impressions)
  from campaign_stats 
  group by date(hour), campaign_id 
  order by sum(impressions) desc
)
as inner_table
group by date;
```

### 1.2
```
select * from
(
  select
  date(hour) as day,
  campaign_id,
  advertiser.name as advertiser_name,
  publisher.name as publisher_name,
  campaign.name as campaign_name,
  creative.name as creative_name
  from campaign_stats
  join advertiser
  on campaign_stats.advertiser_id = advertiser.id
  join publisher
  on campaign_stats.publisher_id = publisher.id
  join campaign
  on campaign_stats.campaign_id = campaign.id
  join creative
  on campaign_stats.creative_id = creative.id
  group by date(hour), campaign_id
  order by sum(impressions) desc
) as inner_table
group by day
;
```

### 1.3
```
select campaign_id, date(hour), 100 * (sum(clicks) / sum(impressions)) as ctr from campaign_stats
where date(hour) = '2021-04-20'
group by date(hour), campaign_id
order by ctr desc
limit 10;
```

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



