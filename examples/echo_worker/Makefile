IMG=${img}

build:
	echo "revision=`git describe --always`" | tee status.txt
	docker build . -t $(IMG)

push:
	docker push $(IMG)

pull:
	docker pull $(IMG)
