set -e
curl http://localhost:8000/tables \
	--compressed \
	-H 'Accept-Encoding: gzip' \
	-H 'Content-Type: application/json' \
-d '{"address": "0x5ec5e453f110d853123b5a47e64dd6692d4d374d","query": {"id": "0x74626465657200000000000000000000506c6179657200000000000000000000"}}'
