# sql-masker

A SQL constants masker for TiDB / MySQL, based on analysis of `Planner` module of TiDB.

## Roadmap

- [x] constant type inference
  - [x] inference based on physical plan
  - [x] constant type casting
- [x] db / table / column name masking
- [x] a just-works mask function
- [x] support MySQL Events from [zyguan/mysql-replay](https://github.com/zyguan/mysql-replay)
- [x] test on TPC-C workloads
