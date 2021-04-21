
## 建库语句
```sql
CREATE DATABASE IF NOT EXISTS lannister DEFAULT CHARSET utf8 COLLATE utf8_general_ci;
```




## 常用查询
```sql
select * from app_heuristic_result where heuristic_class<>'NoDataReceived' limit 10;
```