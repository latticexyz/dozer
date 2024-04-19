set -e
curl http://localhost:3000/q \
	--compressed \
	-H 'Accept-Encoding: gzip' \
	-H 'Content-Type: application/json' \
	-d '{
	"query": "select count(*) from records where b2i8(key[1]) > 42",
	"values": []
}'
