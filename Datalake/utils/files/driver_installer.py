from Datalake.utils.json_templates import JsonTemplates


def install_file_driver(
    env, job_id, parameter_file, driver_cluster, run_as, timeout="2H"
)-> dict:
    json_tmp = JsonTemplates()
    result = json_tmp.load("driver_template.json")

    if result[0]:
        new_dict = json_tmp.generate(
            {
                "env": env,
                "job_id": job_id,
                "parameter_file": parameter_file,
                "driver_cluster": driver_cluster,
                "run_as": run_as,
                "timeout": timeout,
            }
        )
