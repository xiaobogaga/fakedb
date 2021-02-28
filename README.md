fakedb, a transactional database, based on aries and s2pl, can save two key-value pairs. Key, value size cannot be larger than 8K.
Built for learning purpose.

## Install:

```shell script
go get github.com/xiaobogaga/fakedb
go install github.com/xiaobogaga/fakedb/fakedb
```

## Usage

### start server:

```shell script
fakedb
```

### start client cli

```shell script
fakedb -client
hi :)
fakedb> help
* set key value
* get key
* del key
* begin
* commit
* rollback
* help
fakedb>
```
and then type help for more information. begin, commit, rollback to begin, commit or rollback a transaction.

