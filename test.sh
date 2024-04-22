set -e
curl http://localhost:3000/q \
	--compressed \
	-H 'Accept-Encoding: gzip' \
	-H 'Content-Type: application/json' \
	-d '{
	"query": "select block_num from records order by block_num desc limit 1",
	"values": []
}'
