module example

go 1.21

require (
	github.com/go-sql-driver/mysql v1.8.1
	github.com/jackwangfeng/dtmrs/clients/go v0.0.0
)

require filippo.io/edwards25519 v1.1.0 // indirect

replace github.com/jackwangfeng/dtmrs/clients/go => ../
