# how to use

Run
-----

### bare

```bash
pipenv run echo <storage_uri> [args...]
```

### docker
#### by AWS_* env variables
```bash
# export correct AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN
STORAGE_URI=s3://dev-adbi/io/test
DOCKER_IMG=xxxxxxxxxxxxxxxx.dkr.ecr.ap-northeast-1.amazonaws.com/dev-adbi-echo
docker run -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN ${DOCKER_IMG} ${STORAGE_URI} a b c
```

#### by IAM Role

```bash
STORAGE_URI=s3://dev-adbi/io/test
DOCKER_IMG=xxxxxxxxxxxxxxxx.dkr.ecr.ap-northeast-1.amazonaws.com/dev-adbi-echo
docker run ${DOCKER_IMG} ${STORAGE_URI} a b c
```

Build
---------
```bash
make img=your_docker_image_name
```

Push
------
```bash
make push
```
