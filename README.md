# cedar-engine

[Contributors |](https://github.com/karthiklsarma/cedar-engine/graphs/contributors)
[Forks |](https://github.com/karthiklsarma/cedar-engine/network/members)
[Issues |](https://github.com/karthiklsarma/cedar-engine/issues)
[MIT License |](https://github.com/karthiklsarma/cedar-engine/blob/main/LICENSE)

## To build and run cedar-server project on localhost:

From ./cedar-listener Directory:

### To build cedar-listener

- Execute
  > go build -o ./bin/cedar-listener

### To run

- Execute from ./cedar-listener
  > /bin/cedar-listener

## To build and run project on docker container on local:

From ./cedar-listener Directory:

### To build

- Execute
  > docker build . -t cedar-listener:latest

### To run

- Execute
  > docker run -d -p 8080:8080 `<IMAGE ID from previous step>`

## To deploy on azure

- Execute deploy cedar [script](https://github.com/karthiklsarma/cedar-deploy/blob/main/cedar-deploy.sh)
- Once the Kubernetes cluster and Container registry is deployed, Execute
  > az acr build --registry cedarcr --image cedar-listener:v1 .
