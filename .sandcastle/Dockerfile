FROM node:22-bookworm

# Install system dependencies
RUN apt-get update && apt-get install -y \
  git \
  curl \
  jq \
  && rm -rf /var/lib/apt/lists/*

# Install GitHub CLI
RUN curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
  | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg \
  && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
  | tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
  && apt-get update && apt-get install -y gh \
  && rm -rf /var/lib/apt/lists/*

# Build-args for UID/GID alignment: sandcastle docker build-image
# defaults these to the host user's UID/GID so image-built files
# and bind-mounted files share an owner without runtime chown.
ARG AGENT_UID=1000
ARG AGENT_GID=1000

# Rename the base image's "node" user to "agent" and align UID/GID.
# Evict any existing group/user that already owns the target GID/UID
# (e.g. Debian's "dialout" holds GID 20, which clashes with macOS staff GID).
RUN set -e; \
    existing_group=$(getent group "$AGENT_GID" | cut -d: -f1 || true); \
    if [ -n "$existing_group" ] && [ "$existing_group" != "node" ]; then \
      groupmod -g "$(($AGENT_GID + 100))" "$existing_group"; \
    fi; \
    existing_user=$(getent passwd "$AGENT_UID" | cut -d: -f1 || true); \
    if [ -n "$existing_user" ] && [ "$existing_user" != "node" ]; then \
      usermod -u "$(($AGENT_UID + 100))" "$existing_user"; \
    fi; \
    groupmod -g "$AGENT_GID" node && \
    usermod -u "$AGENT_UID" -g "$AGENT_GID" -d /home/agent -m -l agent node
USER ${AGENT_UID}:${AGENT_GID}

# Install Claude Code CLI
RUN curl -fsSL https://claude.ai/install.sh | bash

# Add Claude to PATH
ENV PATH="/home/agent/.local/bin:$PATH"

WORKDIR /home/agent

# In worktree sandbox mode, Sandcastle bind-mounts the git worktree at /home/agent/workspace
# and overrides the working directory to /home/agent/workspace at container start.
# Structure your Dockerfile so that /home/agent/workspace can serve as the project root.
ENTRYPOINT ["sleep", "infinity"]
