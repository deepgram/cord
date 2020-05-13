# For useful tips on developing this image:
#   https://docs.docker.com/develop/develop-images/dockerfile_best-practices
#   https://blog.replicated.com/refactoring-a-dockerfile-for-image-size/
# For more information on Ubuntu's minimal images:
#   https://blog.ubuntu.com/2018/07/09/minimal-ubuntu-released

FROM ubuntu:18.04 as builder

LABEL maintainer="Nikola Whallon <nikola@deepgram.com>"

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        clang \
        curl \
        libssl-dev \
        pkg-config

#ARG rust_toolchain=nightly-2018-12-18

#RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain $rust_toolchain && \
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y && \
    . $HOME/.cargo/env

COPY . /src

RUN . $HOME/.cargo/env && \
    cargo install --path /src --root /

RUN . $HOME/.cargo/env && \
    rustup self uninstall -y && \
    DEBIAN_FRONTEND=noninteractive apt-get purge -y --autoremove \
        clang \
        curl \
        pkg-config && \
    apt-get clean && \
    rm -rf /src

FROM ubuntu:18.04

LABEL maintainer="Nikola Whallon <nikola@deepgram.com>"

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl1.0.0

COPY --from=builder /bin/cord /bin/cord

RUN apt-get clean

ENTRYPOINT ["/bin/cord"]
