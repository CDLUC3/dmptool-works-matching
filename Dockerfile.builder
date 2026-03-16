# -----------------------------------------------------
# Compile Rust extension and build dmpworks wheel
# -----------------------------------------------------

FROM amazonlinux:2023

# Build arguments
ARG RUST_TARGET_CPU="x86-64-v3"
ARG PYTHON_VERSION="3.12"
ENV RUST_TARGET_CPU=${RUST_TARGET_CPU}
ENV PYTHON_VERSION=${PYTHON_VERSION}

# Pull uv into builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install system dependencies
RUN dnf -y install \
    wget \
    git \
    python${PYTHON_VERSION} \
    python${PYTHON_VERSION}-pip
RUN dnf group install -y "Development Tools"

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install maturin
RUN uv tool install maturin

# Go to dmpworks
WORKDIR /app/dmptool-works-matching

# Sync dependencies
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --python ${PYTHON_VERSION}

COPY . .
RUN RUSTFLAGS="-C target-cpu=${RUST_TARGET_CPU}" uv tool run maturin build --interpreter python${PYTHON_VERSION} --release
