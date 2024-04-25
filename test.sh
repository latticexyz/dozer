set -e
curl http://localhost:3000/q \
	--compressed \
	-H 'Accept-Encoding: gzip' \
	-H 'Content-Type: application/json' \
	-d '{
	"query": "select value from Counter",
	"values": []
}'
