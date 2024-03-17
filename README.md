# Rulink
A toy project for streaming process inspired by [Apache Flink](https://github.com/apache/flink) and [RisingLight](https://github.com/risinglightdb/risinglight), writen by Rust just for learning purpose. 

## Demo

![](src/pic/demo.gif)



## Quick Start
Only supports Linux or macOs. You will need to [install Rust](https://www.rust-lang.org/tools/install) firstly. After that, you can using the follow command to run it. 
```
cargo run
```
Or you may want to build it and run:
```
cargo build --release
cd target/release/
./rulink
```

### Example
```
## create two tables, and insert into one table
create table source(a int, b int) with ('connector' = 'datagen');

create table blackhole_sink(a int, b int) with ('connector' = 'blackhole');

insert into blackhole_sink select * source;

# query the source table 
select * from source;

# ctrl + c to cancle the query job

exit;
```


## Acknowledgement

Thanks to [Apache Flink](https://github.com/apache/flink) and [RisingLight](https://github.com/risinglightdb/risinglight).



