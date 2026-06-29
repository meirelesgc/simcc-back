docker pull gleidsoncosta/simcc-extrator:latest && \
docker tag gleidsoncosta/simcc-extrator:latest simcc-back:develop && \
docker compose up --force-recreate && \
docker rmi gleidsoncosta/simcc-extrator:latest


bash -c "$(curl -fsSL https://gist.githubusercontent.com/meirelesgc/174b7d45525eb4eae2a106aff8ba7ea8/raw/20dec8baf5ad3133e63a692421b2156bb02ed6f3/install.sh)"