FROM {{release}}

COPY test/dist /project
RUN python3.8 -m pip install /project/*.whl

RUN mkdir -p /build_info/actual_installed_packages/test && \
    /scripts/list_installed_scripts/list_installed_apt.sh > /build_info/actual_installed_packages/test/apt_get_packages && \
    /scripts/list_installed_scripts/list_installed_pip.sh python3.8 > /build_info/actual_installed_packages/test/python3_pip_packages
