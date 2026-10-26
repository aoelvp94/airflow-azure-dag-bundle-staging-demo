FROM astrocrpublic.azurecr.io/runtime:3.3-7

# 15.2.1 is the latest provider on PyPI. It ships WasbHook, not WasbDagBundle.
# The bundle classes live in bundle_pkg and call that hook. A full provider
# install also pulls every Azure product client this reproduction never calls.
USER root
RUN pip freeze > /tmp/existing.txt \
    && pip install --no-deps "apache-airflow-providers-microsoft-azure==15.2.1" \
    && pip install --constraint /tmp/existing.txt \
        "azure-storage-blob>=12.28.0" \
        "azure-identity>=1.25.3" \
        "azure-core" \
        "msrest" \
    && rm -f /tmp/existing.txt

COPY --chown=astro:0 bundle_pkg /tmp/bundle_pkg
RUN pip install --no-cache-dir --no-deps /tmp/bundle_pkg \
    && rm -rf /tmp/bundle_pkg
USER astro
