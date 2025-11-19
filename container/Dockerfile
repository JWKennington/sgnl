FROM  containers.ligo.org/greg/sgnl-base-container:0.0.1
##
## Install system packages.
##
WORKDIR /src

COPY . sgnl/
RUN git clone https://git.ligo.org/greg/strike.git && \
    git clone https://git.ligo.org/greg/stillsuit.git && \
    git clone https://git.ligo.org/greg/sgn.git && \
    git clone https://git.ligo.org/greg/sgn-ts.git && \
    git clone https://git.ligo.org/greg/sgn-ligo.git && \
    git clone https://git.ligo.org/greg/sgn-event.git && \
    (cd stillsuit && pip3 --no-cache-dir install .) && \
    (cd strike && pip3 --no-cache-dir install .) && \
    (cd sgn && pip3 --no-cache-dir install .) && \
    (cd sgn-event && pip3 --no-cache-dir install .) && \
    (cd sgn-ts && pip3 --no-cache-dir install .) && \
    (cd sgn-ligo && pip3 --no-cache-dir install .) && \
    (cd sgnl && pip3 --no-cache-dir install .[dag])
RUN (strike-config set --path /src/strike/data --setup)
