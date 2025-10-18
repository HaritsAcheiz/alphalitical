# syntax=docker/dockerfile:1.7
#
# Gateway container:
# - Debian bookworm
# - Python 3.11 (+ dev headers)
# - uv-based virtual environment at /opt/venvs/news_scraper
# - OpenSSH server for Airflow SSHOperator
# - Project bind-mount at /opt/news_scraper (from your host ./news_scraper)

FROM python:3.11.14-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

RUN apt-get update && apt-get install -y --no-install-recommends \
    gnupg lsb-release openssh-server ca-certificates curl sudo netcat-openbsd bash \
    && rm -rf /var/lib/apt/lists/*

# Ensure sshd runtime directory
RUN mkdir -p /var/run/sshd

# Create non-root user `scraper` for SSH login
RUN useradd -m -s /bin/bash scraper \
 && mkdir -p /home/scraper/.ssh \
 && chown -R scraper:scraper /home/scraper/.ssh \
 && chmod 700 /home/scraper/.ssh

# Harden SSH: only pubkey auth, no root login
RUN printf '%s\n' \
      'PubkeyAuthentication yes' \
      'PasswordAuthentication no' \
      'PermitRootLogin no' \
    > /etc/ssh/sshd_config.d/10-gateway.conf

# Install uv (Astral) and put it on PATH
ENV UV_LINK_MODE=copy
RUN curl -Ls https://astral.sh/uv/install.sh | sh \
 && echo 'export PATH="$HOME/.local/bin:$PATH"' >> /etc/profile \
 && echo 'export PATH="/root/.local/bin:$PATH"' >> /root/.bashrc
ENV PATH="/root/.local/bin:${PATH}"

# Explicitly pin uv default python and our venv target to 3.11
ENV UV_PYTHON=/usr/bin/python3.11

# Locations
ENV PROJECT_DIR=/opt/news_scraper \
    VENV_DIR=/opt/venvs/news_scraper

# Copy project for dependency resolution at build
COPY . ${PROJECT_DIR}
WORKDIR ${PROJECT_DIR}

# Create venv with Python 3.11 and install dependencies with uv
RUN uv venv --python /usr/bin/python3.11 "${VENV_DIR}" \
 && /bin/bash -lc '\
      if [ -f "pyproject.toml" ]; then \
          uv sync --python "${VENV_DIR}/bin/python" --frozen || uv sync --python "${VENV_DIR}/bin/python"; \
      elif [ -f "requirements.txt" ]; then \
          uv pip install --python "${VENV_DIR}/bin/python" -r requirements.txt; \
      else \
          echo "No dependency manifest found; continuing with empty venv"; \
      fi \
    '

# Convenience: auto-activate venv in interactive shells
RUN echo "source ${VENV_DIR}/bin/activate" >> /etc/bash.bashrc

# Expose SSH port
EXPOSE 22

# Start OpenSSH server in foreground
CMD ["/usr/sbin/sshd", "-D", "-e"]