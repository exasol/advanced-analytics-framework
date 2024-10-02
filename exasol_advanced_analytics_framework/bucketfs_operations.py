# This file originally was copied from transformers-extension
# https://github.com/exasol/transformers-extension/blob/main/exasol_transformers_extension/utils/bucketfs_operations.py

# Modifications:
# - removed functions dealing with specifics of transformers-extensions
# - disabled support for Exasol SaaS instances

from __future__ import annotations
import tarfile
import tempfile
from tempfile import NamedTemporaryFile
from pathlib import PurePosixPath, Path
from typing import Any, BinaryIO
# from exasol_udf_mock_python.connection import Connection


# import joblib
# import json
import exasol.bucketfs as bfs
# from exasol.saas.client.api_access import get_database_id   # type: ignore
# from exasol_transformers_extension.utils.model_specification import ModelSpecification

# unused
def create_bucketfs_location_from_conn_object(bfs_conn_obj: Connection) -> bfs.path.PathLike:
    bfs_params = json.loads(bfs_conn_obj.address)
    bfs_params.update(json.loads(bfs_conn_obj.user))
    bfs_params.update(json.loads(bfs_conn_obj.password))
    return bfs.path.build_path(**bfs_params)


# unused
def create_bucketfs_location(
        path_in_bucket: str = '',
        bucketfs_name: str | None = None,
        bucketfs_url: str | None = None,
        bucketfs_host: str | None = None,
        bucketfs_port: int | None = None,
        bucketfs_use_https: bool = True,
        bucketfs_user: str | None = None,
        bucketfs_password: str | None = None,
        bucket: str | None = None,
        saas_url: str | None = None,
        saas_account_id: str | None = None,
        saas_database_id: str | None = None,
        saas_database_name: str | None = None,
        saas_token: str | None = None,
        use_ssl_cert_validation: bool = False
) -> bfs.path.PathLike:

    # Infer where the database is - on-prem or SaaS.
    is_on_prem = all((any((bucketfs_url, all((bucketfs_host, bucketfs_port)))), bucketfs_name,
                      bucket, bucketfs_user, bucketfs_password))
    if is_on_prem:
        if not bucketfs_url:
            bucketfs_url = (f"{'https' if bucketfs_use_https else 'http'}://"
                            f"{bucketfs_host}:{bucketfs_port}")
        return bfs.path.build_path(backend=bfs.path.StorageBackend.onprem,
                                   url=bucketfs_url,
                                   username=bucketfs_user,
                                   password=bucketfs_password,
                                   service_name=bucketfs_name,
                                   bucket_name=bucket,
                                   verify=use_ssl_cert_validation,
                                   path=path_in_bucket)

    is_saas = all((saas_url, saas_account_id, saas_token,
                   any((saas_database_id, saas_database_name))))
    if is_saas:
        pass
        # saas_database_id = (saas_database_id or
        #                     get_database_id(
        #                         host=saas_url,
        #                         account_id=saas_account_id,
        #                         pat=saas_token,
        #                         database_name=saas_database_name
        #                     ))
        # return bfs.path.build_path(backend=bfs.path.StorageBackend.saas,
        #                            url=saas_url,
        #                            account_id=saas_account_id,
        #                            database_id=saas_database_id,
        #                            pat=saas_token,
        #                            path=path_in_bucket)
        #
    raise ValueError('Incomplete parameter list. '
                     'Please either provide the parameters [bucketfs_host, '
                     'bucketfs_port, bucketfs_name, bucket, bucketfs_user, '
                     'bucketfs_password] for an On-Prem database or [saas_url, '
                     'saas_account_id, saas_database_id or saas_database_name, '
                     'saas_token] for a SaaS database.')


def upload_model_files_to_bucketfs(
        model_directory: str,
        bucketfs_model_path: Path,
        bucketfs_location: bfs.path.PathLike) -> Path:
    """
    uploads model in tmpdir_name to model_path in bucketfs_location
    """
    with tempfile.TemporaryFile() as fileobj:
        create_tar_of_directory(Path(model_directory), fileobj)
        fileobj.seek(0)
        model_upload_tar_file_path = bucketfs_model_path.with_suffix(".tar.gz")
        bucketfs_model_location = bucketfs_location / model_upload_tar_file_path
        bucketfs_model_location.write(fileobj)
        return model_upload_tar_file_path


def create_tar_of_directory(path: Path, fileobj: BinaryIO):
    with tarfile.open(name="model.tar.gz", mode="w|gz", fileobj=fileobj) as tar:
        for subpath in path.glob("*"):
            tar.add(name=subpath, arcname=subpath.name)


def get_local_bucketfs_path(
        bucketfs_location: bfs.path.PathLike, model_path: str) -> PurePosixPath:
    """
    returns path model defined by model_path can be found at in bucket defined by bucketfs_location
    """
    bucketfs_model_location = bucketfs_location / model_path
    return PurePosixPath(bucketfs_model_location.as_udf_path())


# def create_save_pretrained_model_path(_tmpdir_name, model_specification: ModelSpecification) -> Path:
#     """
#     path HuggingFaceHubBucketFSModelTransferSP saves the model at using save_pretrained,
#     before it is uploaded to the bucketfs
#     """
#     model_specific_path_suffix = model_specification.get_model_specific_path_suffix()
#     return Path(_tmpdir_name, "pretrained", model_specific_path_suffix)
