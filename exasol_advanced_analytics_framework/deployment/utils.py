import os
from getpass import getpass
import logging
logger = logging.getLogger(__name__)

DB_PASSWORD_ENVIRONMENT_VARIABLE = f"AAF_DB_PASSWORD"
BUCKETFS_PASSWORD_ENVIRONMENT_VARIABLE = f"AAF_BUCKETFS_PASSWORD"


def get_password(pwd: str, user: str, env_var: str, descr: str) -> str:
    if pwd is None:
        if env_var in os.environ:
            logger.debug(f"Use password from environment variable {env_var}")
            pwd = os.environ[env_var]
        else:
            pwd = getpass(f"{descr} for User {user}")
    return pwd
