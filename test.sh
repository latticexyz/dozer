set -e
curl 'http://localhost:8000/api/logs-live?block_num=42&input=%7B%22address%22%3A%220x9d05cc196c87104a7196fcca41280729b505dbbf%22%7D' \
	-H 'Content-Type: application/json'
