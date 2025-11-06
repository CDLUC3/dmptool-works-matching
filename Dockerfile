# OpenSearch versions: https://hub.docker.com/r/opensearchproject/opensearch
ARG OS_VERSION=3.0.0
FROM opensearchproject/opensearch:${OS_VERSION}

# Install plugins
RUN /usr/share/opensearch/bin/opensearch-plugin install analysis-icu
