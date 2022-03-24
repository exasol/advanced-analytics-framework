import os
from getpass import getpass

DB_PASSWORD_ENVIRONMENT_VARIABLE = f"AFF_DB_PASSWORD"
BUCKETFS_PASSWORD_ENVIRONMENT_VARIABLE = f"MLL_BUCKETFS_PASSWORD"


def get_password(pwd: str, user: str, env_var: str, descr: str) -> str:
    if pwd is None:
        if env_var in os.environ:
            print(f"Using password from environment variable {env_var}")
            pwd = os.environ[env_var]
        else:
            pwd = getpass(f"{descr} for User {user}")
    return pwd
