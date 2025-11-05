FROM  containers.ligo.org/greg/sgnl-base-container:0.0.1
##
## Install system packages.
##
WORKDIR /src

COPY *.whl /tmp/
RUN git clone --depth 1 https://git.ligo.org/greg/strike.git && \
    git clone --depth 1 https://git.ligo.org/greg/stillsuit.git && \
    git clone --depth 1 https://git.ligo.org/greg/sgn.git && \
    git clone --depth 1 https://git.ligo.org/greg/sgn-ts.git && \
    git clone --depth 1 https://git.ligo.org/greg/sgn-ligo.git && \
    git clone --depth 1 https://git.ligo.org/greg/sgn-event.git && \
    (cd stillsuit && pip3 --no-cache-dir install .) && \
    (cd strike && pip3 --no-cache-dir install .) && \
    (cd sgn && pip3 --no-cache-dir install .) && \
    (cd sgn-event && pip3 --no-cache-dir install .) && \
    (cd sgn-ts && pip3 --no-cache-dir install .) && \
    (cd sgn-ligo && pip3 --no-cache-dir install .) && \
    WHEEL=$(ls /tmp/sgnl-*.whl) && pip3 --no-cache-dir install "${WHEEL}[dag]" && \
    echo "Git commit SHAs at build time:" > /src/build_versions.txt && \
    (cd /src/strike && echo "strike: $(git rev-parse HEAD)" >> /src/build_versions.txt) && \
    (cd /src/stillsuit && echo "stillsuit: $(git rev-parse HEAD)" >> /src/build_versions.txt) && \
    (cd /src/sgn && echo "sgn: $(git rev-parse HEAD)" >> /src/build_versions.txt) && \
    (cd /src/sgn-event && echo "sgn-event: $(git rev-parse HEAD)" >> /src/build_versions.txt) && \
    (cd /src/sgn-ts && echo "sgn-ts: $(git rev-parse HEAD)" >> /src/build_versions.txt) && \
    (cd /src/sgn-ligo && echo "sgn-ligo: $(git rev-parse HEAD)" >> /src/build_versions.txt) && \
    rm -rf /src/stillsuit /src/sgn /src/sgn-ts /src/sgn-ligo /src/sgn-event /tmp/*.whl && \
    find /src/strike -mindepth 1 -maxdepth 1 ! -name 'data' -exec rm -rf {} +
RUN (strike-config set --path /src/strike/data --setup)
