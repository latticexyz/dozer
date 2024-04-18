set -ex
curl http://localhost:3000/records \
	-w '\n\nnbytes=%{size_download}\n' \
	--compressed \
	-H 'Accept-Encoding: gzip' \
	-H 'Content-Type: application/json' \
	-d '{
	"table_id": "0x74620000000000000000000000000000436861726163746572496e76656e746f",
	"key": ["0x0000000000000000000000000000000000000000000000000000000000000000"]
}'
printf '\n'
