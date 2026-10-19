# DistCache image: the cache node, the router and the load generator in one image.
#   docker build -t distcache .
#   docker run --rm -p 6380:6380 distcache                 # a single node (listens on 0.0.0.0 inside the container)
# For the whole cluster use docker-compose.yml.
#
# NOTE: this file and docker-compose.yml were written on a machine without Docker and have NOT been run.
# The same topology is exercised by scripts/cluster.sh, which is tested.
FROM debian:12-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
        g++ cmake ninja-build make ca-certificates libgtest-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY CMakeLists.txt ./
COPY src ./src
COPY bench ./bench
COPY tests ./tests
RUN cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build --target distcache distcache_router distcache_loadgen

FROM debian:12-slim
RUN apt-get update && apt-get install -y --no-install-recommends redis-tools \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home /data distcache && mkdir /data /state && chown distcache /data /state
COPY --from=build /src/build/distcache /src/build/distcache_router /src/build/distcache_loadgen /usr/local/bin/
COPY scripts/healthcheck.sh /usr/local/bin/healthcheck.sh
USER distcache
VOLUME /data
EXPOSE 6380 6381 6390
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=6 \
    CMD healthcheck.sh 6380
ENTRYPOINT ["distcache"]
CMD ["--bind", "0.0.0.0", "--dir", "/data"]
