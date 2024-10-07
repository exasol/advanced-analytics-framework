FROM {{release}}

COPY test/dist /project
RUN python3.10 -m pip install --use-deprecated=legacy-resolver /project/*.whl

RUN mkdir -p /build_info/actual_installed_packages/test && \
    /scripts/list_installed_scripts/list_installed_apt.sh > /build_info/actual_installed_packages/test/apt_get_packages && \
    /scripts/list_installed_scripts/list_installed_pip.sh python3.10 > /build_info/actual_installed_packages/test/python3_pip_packages
