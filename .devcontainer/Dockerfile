FROM amazoncorretto:21-alpine

# Install required packages + Neovim from registry
RUN apk add --no-cache \
  bash curl tar gzip tmux git openssh zip unzip \
  build-base gcc g++ cmake gettext gettext-dev \
  python3 py3-pip nodejs npm \
  ripgrep fzf lazygit neovim \
  maven gradle

# Set working directory for subsequent operations
WORKDIR /root

# Install tmux plugin manager + plugins + generate SSH keys
RUN git clone https://github.com/tmux-plugins/tpm .tmux/plugins/tpm && \
  TMUX_PLUGIN_MANAGER_PATH=~/.tmux/plugins/tpm ~/.tmux/plugins/tpm/bin/install_plugins && \
  ssh-keygen -t ed25519 -f /root/.ssh/id_ed25519 -N ""

# Install PMD (Static Code Analyzer)
# NOTE: this may be required later
# WORKDIR /tmp
# RUN wget https://github.com/pmd/pmd/releases/download/pmd_releases%2F7.7.0/pmd-dist-7.7.0-bin.zip && \
#   unzip pmd-dist-7.7.0-bin.zip && \
#   mv pmd-bin-7.7.0 /opt/pmd && \
#   rm pmd-dist-7.7.0-bin.zip

# Install SonarQube Scanner (Static Code Analysis Tool)
# NOTE: this may be required later
# RUN wget https://binaries.sonarsource.com/Distribution/sonar-scanner-cli/sonar-scanner-cli-6.2.1.4610.zip && \
#   unzip sonar-scanner-cli-6.2.1.4610.zip && \
#   mv sonar-scanner-6.2.1.4610 /opt/sonar-scanner && \
#   rm sonar-scanner-cli-6.2.1.4610.zip

# Add binaries to PATH
# ENV PATH=$PATH:/opt/pmd/bin:/opt/sonar-scanner/bin

# Set the default working directory
WORKDIR /workspace
