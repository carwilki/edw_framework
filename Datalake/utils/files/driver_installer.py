import json
from jinja2 import Environment, PackageLoader, select_autoescape, DebugUndefined

_jenv = Environment(
    loader=PackageLoader("Datalake.utils.files", "templates"),
    autoescape=select_autoescape(),
    undefined=DebugUndefined,
)
_jenv.filters["jsonify"] = json.dumps


def get_file_driver_payload(
    env, job_id, parameter_file, driver_cluster, run_as_user, timeout="2H"
) -> str:
    # Template file at ./app/templates/example.json
    template = _jenv.get_template("driver_template.json")

    return template.render(
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
