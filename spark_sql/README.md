# spark sql


-plan 확인
```

df_min.explain()


== Physical Plan ==
*(1) ColumnarToRow
+- FileScan parquet [close#618,high#619,jdiff_vol#620,low#621,open#622,time#623,value#624,date#625,shcode#626] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex[file:/d:/dw/stock_min], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<close:string,high:string,jdiff_vol:string,low:string,open:string,time:string,value:string>

```
> PartitionFilters: [], PushedFilters: [] 실제 파일 베이스의 load 진행 시 filter 적용 가능 



1.  filter push down 

!()[https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdn%2FHmwRZ%2FbtqFGMooyYX%2FOTWk50SkbEDxM5Gy39Kv21%2Fimg.png]

2. partition pruning

!()[https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdn%2FCJiVH%2FbtqFINUt1c8%2FtUzhjYShsA3HXSrZkeJVZ1%2Fimg.png]





