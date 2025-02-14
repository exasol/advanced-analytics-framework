from jinja2 import (
    Environment,
    PackageLoader,
    Template,
    select_autoescape,
)


class JinjaTemplateLocation:

    def __init__(self, package_name: str, package_path: str, template_file_name: str):
        self.template_file_name = template_file_name
        self.package_path = package_path
        self.package_name = package_name

    def get_template(self) -> Template:
        env = Environment(
            loader=PackageLoader(
                package_name=self.package_name, package_path=self.package_path
            ),
            autoescape=select_autoescape(),
        )
        return env.get_template(self.template_file_name)
