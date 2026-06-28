docker pull gleidsoncosta/simcc-extrator:latest && \
docker tag gleidsoncosta/simcc-extrator:latest simcc-back:develop && \
docker compose up --force-recreate && \
docker rmi gleidsoncosta/simcc-extrator:latest