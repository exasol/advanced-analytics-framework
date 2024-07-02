from dataclasses import dataclass


@dataclass(frozen=True)
class DBParams:
    host: str
    port: str
    user: str
    password: str

    def address(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass(frozen=True)
class BucketFSParams:
    host: str
    port: str
    real_port: str
    user: str
    password: str
    name: str
    bucket: str
    path_in_bucket: str

    def address(self, port=None) -> str:
        port = self.port if not port else port
        return f"http://{self.host}:{port}/{self.bucket}/" \
               f"{self.path_in_bucket};{self.name}"


db_params = DBParams(
    host="127.0.0.1",
    port="8888",
    user="sys",
    password="exasol")

bucketfs_params = BucketFSParams(
    host="127.0.0.1",
    port="6666",
    real_port="2580",
    user="w",
    password="write",
    name="bfsdefault",
    bucket="default",
    path_in_bucket="container")
