import json
from jinja2 import Environment, FileSystemLoader, PackageLoader, Template, select_autoescape, DebugUndefined
from Datalake.utils.files.templates import driver_template
_jenv = Environment(
    loader=FileSystemLoader(searchpath="./templates"),
    autoescape=select_autoescape(),
    undefined=DebugUndefined,
)
_jenv.filters["jsonify"] = json.dumps


def get_file_driver_payload(
    name,env, job_id, parameter_file, driver_cluster, run_as_user, timeout="2h"
) -> str:
    # Template file at ./app/templates/example.json
    template = Environment(loader=FileSystemLoader("templates/")).from_string(driver_template)
    return template.render(
        name=name,
        env=env,
        job_id=job_id,
        parameter_file=parameter_file,
        driver_cluster=driver_cluster,
        run_as_user=run_as_user,
        timeout=timeout,
    )


def get_file_driver_cluster_payload():
    with open("Datalake/utils/files/templates/driver_cluster_template.json") as json_file:
        job_payload = json.load(json_file)

    payload = json.dumps(job_payload)

    return payload
