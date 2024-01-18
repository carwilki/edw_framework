from Datalake.utils import secrets

nz_jdbc_driver = "org.netezza.Driver"
nz_jdbc_num_part = 9
nz_jdbc_url = "jdbc:netezza://172.16.73.181:5480/"
nz_username = secrets.get(scope="netezza_petsmart_keys", key="username")
nz_password = secrets.get(scope="netezza_petsmart_keys", key="password")
nz_fetch_size = 1_000_000
