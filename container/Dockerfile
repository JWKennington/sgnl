FROM  rockylinux/rockylinux:9
##
## Install system packages.
##
RUN dnf install -y yum-utils && \
    dnf config-manager --set-enabled crb && \
    dnf install -y   https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm && \
    dnf install -y git emacs-nox git-lfs.x86_64 python3.12 python3.12-pip python3.12-devel kcat && \
    dnf install -y gcc gcc-c++ && \
    dnf clean all
WORKDIR /usr/local/src
##
## Download the required repositories. When this is migrated to
## sgnl repository, no need to clone sgnl.
##
RUN git lfs install && \
    git clone https://git.ligo.org/greg/strike.git && \
    git clone https://git.ligo.org/greg/stillsuit.git && \
    git clone https://git.ligo.org/greg/sgn.git && \
    git clone https://git.ligo.org/greg/sgn-ts.git && \
    git clone https://git.ligo.org/greg/sgn-ligo.git && \
    git clone https://git.ligo.org/greg/sgn-event.git && \
    git clone https://git.ligo.org/greg/sgnl.git
RUN ln -s /usr/bin/pip3.12 /usr/bin/pip3 && \
    rm -f /usr/bin/python3 && \
    ln -s /usr/bin/python3.12 /usr/bin/python3
RUN pip3 --no-cache-dir uninstall numpy && \
    pip3 --no-cache-dir install --force-reinstall 'numpy<2'
RUN (cd stillsuit && pip3 --no-cache-dir install -e .)
RUN (cd strike && pip3 --no-cache-dir install -e .)
RUN (cd sgn && pip3 --no-cache-dir install -e .)
RUN (cd sgn-event && pip3 --no-cache-dir install -e .)
RUN (cd sgn-ts && pip3 --no-cache-dir install -e .)
RUN (cd sgn-ligo && pip3 --no-cache-dir install -e .)
RUN (cd sgnl  && pip3 --no-cache-dir install -e .)
