def getPrepBucket(env: str) -> str:
    """
    Returns the GCS bucket path for the given environment.

    Args:
        env (str): The environment name.

    Returns:
        str: The GCS bucket path.
    """
    if env == "prod":
        return "gs://petm-bdpl-dev-shared-p1-gcs-gbl/"
    if env == "qa":
        return "gs://petm-bdpl-dev-shared-p1-gcs-gbl/"
    if env == "dev":
        return "gs://petm-bdpl-dev-shared-p1-gcs-gbl/"


def getArchiveBucket(env: str) -> str:
    """
    Returns the GCS bucket path for the given environment.

    Args:
        env (str): The environment name.

    Returns:
        str: The GCS bucket path.
    """
    if env == "prod":
        return "gs://petm-bdpl-dev-shared-p1-gcs-gbl/"
    if env == "qa":
        return "gs://petm-bdpl-dev-shared-p1-gcs-gbl/"
    if env == "dev":
        return "gs://petm-bdpl-dev-shared-p1-gcs-gbl/"
