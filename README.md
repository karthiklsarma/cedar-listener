# cedar-listener

[Contributors |](https://github.com/karthiklsarma/cdear-listener/graphs/contributors)
[Forks |](https://github.com/karthiklsarma/cedar-listener/network/members)
[Issues |](https://github.com/karthiklsarma/cedar-listener/issues)
[MIT License |](https://github.com/karthiklsarma/cedar-listener/blob/main/LICENSE)

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
- Once the Kubernetes cluster and Container registry is deployed, Execute
  > az acr build --registry cedarcr --image cedar-listener:v1 .
