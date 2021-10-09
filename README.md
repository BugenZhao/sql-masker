# sql-masker

A naive SQL data masker for TiDB, under very early development.

## Roadmap

- [x] constant type inference according to DDL
  - [x] inference based on physical plan
  - [x] constant type casting
  - [ ] "handle" columns (like primary key, index...)
- [ ] a just-works mask function
- [ ] test on TPC-C workloads
