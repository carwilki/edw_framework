def getPrepBucket(env) -> str:
    if env == "prod":
        return "gs://petm-bdpl-dev-shared-p1-gcs-gbl/"
    if env == "qa":
        return "gs://petm-bdpl-dev-shared-p1-gcs-gbl/"
    if env == "dev":
        return "gs://petm-bdpl-dev-shared-p1-gcs-gbl/"
