FROM alpine:3.21

# Install required packages + Neovim from registry
RUN apk add --no-cache \
  bash curl tar gzip tmux git openssh zip unzip openjdk21 \
  build-base gcc g++ cmake gettext gettext-dev \
  python3 py3-pip nodejs npm \
  ripgrep fzf lazygit neovim \
  maven gradle

# Settings
ARG STORM_VERSION=2.8.2
ENV STORM_HOME=/opt/storm
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk

# Set Java 21 as the default JDK/JRE
RUN ln -sfn /usr/lib/jvm/java-21-openjdk /usr/lib/jvm/default-jvm && \
  ln -sfn /usr/lib/jvm/java-21-openjdk /usr/lib/jvm/default-jre

# Put JDK 21 **before** /usr/bin on PATH
ENV PATH="$JAVA_HOME/bin:$PATH"

# Make Gradle use JDK 21 (daemon + toolchains)
ENV GRADLE_OPTS="-Dorg.gradle.java.home=/usr/lib/jvm/java-21-openjdk"

# Install Apache Storm CLI directly from Apache official mirror
# NOTE:
# - The official apache archive is too slow (https://archive.apache.org/dist/storm/apache-storm-${STORM_VERSION}/apache-storm-${STORM_VERSION}.tar.gz)
# - Running from github source is not allowed (storm crashes and warns about this)
RUN set -eux; \
  curl -fL https://dlcdn.apache.org/storm/apache-storm-${STORM_VERSION}/apache-storm-${STORM_VERSION}.tar.gz -o /tmp/storm.tgz; \
  mkdir -p /opt; \
  tar -xzf /tmp/storm.tgz -C /opt; \
  ln -s "/opt/apache-storm-${STORM_VERSION}" "${STORM_HOME}"; \
  ln -s "${STORM_HOME}/bin/storm" /usr/local/bin/storm; \
  rm -f /tmp/storm.tgz

WORKDIR /root

# Install tmux plugin manager + plugins + generate SSH keys
RUN git clone https://github.com/tmux-plugins/tpm .tmux/plugins/tpm && \
  TMUX_PLUGIN_MANAGER_PATH=~/.tmux/plugins/tpm ~/.tmux/plugins/tpm/bin/install_plugins && \
  ssh-keygen -t ed25519 -f /root/.ssh/id_ed25519 -N ""

# WARNING: currently the two installations below are commented out as we may not need them immediately!
# NOTE: Install PMD (Static Code Analyzer)
# WORKDIR /tmp
# RUN wget https://github.com/pmd/pmd/releases/download/pmd_releases%2F7.7.0/pmd-dist-7.7.0-bin.zip && \
#   unzip pmd-dist-7.7.0-bin.zip && \
#   mv pmd-bin-7.7.0 /opt/pmd && \
#   rm pmd-dist-7.7.0-bin.zip

# NOTE: Install SonarQube Scanner (Static Code Analysis Tool)
# RUN wget https://binaries.sonarsource.com/Distribution/sonar-scanner-cli/sonar-scanner-cli-6.2.1.4610.zip && \
#   unzip sonar-scanner-cli-6.2.1.4610.zip && \
#   mv sonar-scanner-6.2.1.4610 /opt/sonar-scanner && \
#   rm sonar-scanner-cli-6.2.1.4610.zip
# Add binaries to PATH
# ENV PATH=$PATH:/opt/pmd/bin:/opt/sonar-scanner/bin

# Set the default working directory
WORKDIR /workspace
