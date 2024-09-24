import pyexasol
from typing import List
from pathlib import Path, PurePosixPath
from exasol_bucketfs_utils_python.bucket_config import BucketConfig
from exasol_bucketfs_utils_python.bucketfs_config import BucketFSConfig
from exasol_bucketfs_utils_python.bucketfs_location import BucketFSLocation
from exasol_bucketfs_utils_python.bucketfs_connection_config import \
    BucketFSConnectionConfig
import logging
logger = logging.getLogger(__name__)


# Should this be replaced by PEC?
class LanguageContainerDeployer:
    def __init__(self,
                 pyexasol_connection: pyexasol.ExaConnection,
                 language_alias: str,
                 bucketfs_location: BucketFSLocation,
                 container_file: Path):
        self._container_file = container_file
        self._bucketfs_location = bucketfs_location
        self._language_alias = language_alias
        self._pyexasol_conn = pyexasol_connection
        logger.debug(f"Init {LanguageContainerDeployer.__name__}")

    def deploy_container(self):
        path_in_udf = self._upload_container()
        for alter in ["SESSION", "SYSTEM"]:
            alter_command = self._generate_alter_command(alter, path_in_udf)
            self._pyexasol_conn.execute(alter_command)
            logging.debug(alter_command)

    def _upload_container(self) -> PurePosixPath:
        if not self._container_file.is_file():
            raise RuntimeError(f"Container file {self._container_file} "
                               f"is not a file.")
        with open(self._container_file, "br") as f:
            upload_uri, path_in_udf = \
                self._bucketfs_location.upload_fileobj_to_bucketfs(
                    fileobj=f, bucket_file_path=self._container_file.name)
        logging.debug("Container is uploaded to bucketfs")
        return PurePosixPath(path_in_udf)

    def _generate_alter_command(self, alter_type: str,
                                path_in_udf: PurePosixPath) -> str:
        new_settings = \
            self._update_previous_language_settings(alter_type, path_in_udf)
        alter_command = \
            f"ALTER {alter_type} SET SCRIPT_LANGUAGES='{new_settings}';"
        return alter_command

    def _update_previous_language_settings(
            self, alter_type: str, path_in_udf: PurePosixPath) -> str:
        prev_lang_settings = self._get_previous_language_settings(alter_type)
        prev_lang_aliases = prev_lang_settings.split(" ")
        self.check_if_requested_language_alias_already_exists(prev_lang_aliases)
        new_definitions_str = self._generate_new_language_settings(
            path_in_udf, prev_lang_aliases)
        return new_definitions_str

    def _generate_new_language_settings(self, path_in_udf: PurePosixPath,
                                        prev_lang_aliases: List[str]) -> str:
        other_definitions = [
            alias_definition for alias_definition in prev_lang_aliases
            if not alias_definition.startswith(self._language_alias + "=")]
        path_in_udf_without_bucksts = Path(*path_in_udf.parts[2:])
        new_language_alias_definition = \
            f"{self._language_alias}=localzmq+protobuf:///" \
            f"{path_in_udf_without_bucksts}?lang=python#" \
            f"{path_in_udf}/exaudf/exaudfclient_py3"
        new_definitions = other_definitions + [new_language_alias_definition]
        new_definitions_str = " ".join(new_definitions)
        return new_definitions_str

    def check_if_requested_language_alias_already_exists(
            self, prev_lang_aliases: List[str]) -> None:
        definition_for_requested_alias = [
            alias_definition for alias_definition in prev_lang_aliases
            if alias_definition.startswith(self._language_alias + "=")]
        if not len(definition_for_requested_alias) == 0:
            logging.warning(f"The requested language alias "
                            f"{self._language_alias} is already in use.")

    def _get_previous_language_settings(self, alter_type: str) -> str:
        result = self._pyexasol_conn.execute(
            f"""SELECT "{alter_type}_VALUE" FROM SYS.EXA_PARAMETERS WHERE
            PARAMETER_NAME='SCRIPT_LANGUAGES'""").fetchall()
        return result[0][0]

    @classmethod
    def run(cls, bucketfs_name: str, bucketfs_host: str, bucketfs_port: int,
            bucketfs_use_https: bool, bucketfs_user: str, container_file: Path,
            bucketfs_password: str, bucket: str, path_in_bucket: str,
            dsn: str, db_user: str, db_password: str, language_alias: str):

        pyexasol_conn = pyexasol.connect(
            dsn=dsn, user=db_user, password=db_password)

        _bucketfs_connection = BucketFSConnectionConfig(
            host=bucketfs_host, port=bucketfs_port, user=bucketfs_user,
            pwd=bucketfs_password, is_https=bucketfs_use_https)
        _bucketfs_config = BucketFSConfig(
            bucketfs_name=bucketfs_name, connection_config=_bucketfs_connection)
        _bucket_config = BucketConfig(
            bucket_name=bucket, bucketfs_config=_bucketfs_config)
        bucketfs_location = BucketFSLocation(
            bucket_config=_bucket_config,
            base_path=PurePosixPath(path_in_bucket))

        language_container_deployer = cls(
            pyexasol_conn, language_alias, bucketfs_location, container_file)
        language_container_deployer.deploy_container()
