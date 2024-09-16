KUBECONFIG=/Users/ericwhite/.kube/testbed-config
PROMETHEUS_ADDR=http://45.113.232.150:9090 
APP_LABEL="deployment=mubench" 
GATEWAY_SERVICE=client
go run main.go
