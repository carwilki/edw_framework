import json
from jinja2 import (
    Environment,
    PackageLoader,
    select_autoescape,
    DebugUndefined,
)

_jenv = Environment(
    loader=PackageLoader(package_name="Datalake.utils.files", package_path="templates"),
    autoescape=select_autoescape(),
    undefined=DebugUndefined,
)
_jenv.filters["jsonify"] = json.dumps


def get_file_driver_payload(
    name, env, job_id, parameter_file, driver_cluster, run_as_user, time_out="2h"
) -> str:
    # Template file at ./app/templates/example.json
    template = _jenv.get_template("driver_template.json")
    return template.render(
        name=name,
        env=env,
        job_id=job_id,
        parameter_file=parameter_file,
        driver_cluster=driver_cluster,
        run_as_user=run_as_user,
        time_out=time_out,
    )


def get_file_driver_cluster_payload(driver_cluster_name,service_account) -> str:
    connection_password = "{{secrets/metastore/password}}"
    connection_uri = "{{secrets/metastore/connectionuri}}"
    connection_username = "{{secrets/metastore/userid}}"
    template = _jenv.get_template("driver_cluster_template.json")
    
    return template.render(
        service_account_name=service_account,
        cluster_name=driver_cluster_name,
        connection_password=connection_password,
        connection_uri=connection_uri,
        connection_username=connection_username,
    )
