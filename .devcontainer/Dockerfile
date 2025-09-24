#
# Stage 1: Storm (cached by version)
#
FROM alpine:3.21 AS storm
ARG STORM_VERSION=2.8.2
ARG APACHE_MIRROR=https://dlcdn.apache.org/storm

RUN --mount=type=cache,target=/var/cache/apk \
    apk add --no-cache curl ca-certificates tar gzip && update-ca-certificates

WORKDIR /opt
RUN set -eux; \
  curl -fL "${APACHE_MIRROR}/apache-storm-${STORM_VERSION}/apache-storm-${STORM_VERSION}.tar.gz" -o storm.tgz; \
  tar -xzf storm.tgz; rm -f storm.tgz

#
# Stage 2: Neovim builder (official Make flow)
#
FROM alpine:3.21 AS nvim-builder

ARG NVIM_REF=stable
ARG CMAKE_BUILD_TYPE=RelWithDebInfo
ENV CMAKE_INSTALL_PREFIX=/nvim-out

# Build prereqs per wiki (cmake+ninja+gettext, toolchain, git, curl, unzip); ccache is auto-used
RUN --mount=type=cache,target=/var/cache/apk \
    apk add --no-cache \
      build-base cmake ninja gettext gettext-dev \
      libtool autoconf automake \
      pkgconfig coreutils curl unzip git ccache

WORKDIR /src/neovim

# NOTE: Result goes to /nvim-out (bin/nvim, share/runtime, etc.)
RUN git clone --filter=blob:none --depth 1 --branch "${NVIM_REF}" https://github.com/neovim/neovim . && \
    make CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} && \
    make CMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX} install


############################
## Final image
############################
FROM alpine:3.21

# Base tools (runtime); keep dev stuff out of final image
RUN --mount=type=cache,target=/var/cache/apk \
    apk add --no-cache \
      bash curl ca-certificates tar gzip tmux git openssh zip unzip \
      openjdk21-jdk \
      python3 py3-pip \
      nodejs npm \
      ripgrep fzf lazygit \
      maven gradle \
      # minimal runtime libs Neovim may link against
      libstdc++ gettext-libs ncurses-terminfo-base \
    && update-ca-certificates

# QoL
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Java/Gradle env
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk
RUN ln -sfn /usr/lib/jvm/java-21-openjdk /usr/lib/jvm/default-jvm && \
    ln -sfn /usr/lib/jvm/java-21-openjdk /usr/lib/jvm/default-jre
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV GRADLE_OPTS="-Dorg.gradle.java.home=/usr/lib/jvm/java-21-openjdk"

# ---- Apache Storm (from cached stage) ----
ARG STORM_VERSION=2.8.2
ENV STORM_HOME=/opt/storm
COPY --from=storm /opt/apache-storm-${STORM_VERSION} /opt/apache-storm-${STORM_VERSION}
RUN ln -s "/opt/apache-storm-${STORM_VERSION}" "${STORM_HOME}" && \
    ln -s "${STORM_HOME}/bin/storm" /usr/local/bin/storm

# Minimal Storm client config (nimbus by service name)
RUN mkdir -p /root/.storm && \
    printf 'nimbus.seeds: ["nimbus"]\n' > /root/.storm/storm.yaml

WORKDIR /root

# ---- Neovim (built from source) ----
COPY --from=nvim-builder /nvim-out/ /usr/local/
# sanity check (optional)
RUN nvim --version | head -n 3

# Tmux TPM + SSH key (optional; consider moving to runtime init if you want deterministic images)
RUN git clone https://github.com/tmux-plugins/tpm /root/.tmux/plugins/tpm && \
    TMUX_PLUGIN_MANAGER_PATH=/root/.tmux/plugins/tpm /root/.tmux/plugins/tpm/bin/install_plugins && \
    mkdir -p /root/.ssh && \
    ssh-keygen -t ed25519 -f /root/.ssh/id_ed25519 -N ""

# Default workspace
WORKDIR /workspace
