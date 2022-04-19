FROM exasol/script-language-container:python-3.8-minimal-EXASOL-6.2.0-build_run_DPUTJLTDYQ7Q3JW4EDOJAWGSIXZVBTQQ4BZWHVCIPT7P5E7A3B2A

RUN apt-get install --no-install-recommends -y git

COPY release/dist /project
RUN python3.8 -m pip install --use-deprecated=legacy-resolver /project/*.whl

RUN mkdir -p /build_info/actual_installed_packages/release && \
    /scripts/list_installed_scripts/list_installed_apt.sh > /build_info/actual_installed_packages/release/apt_get_packages && \
    /scripts/list_installed_scripts/list_installed_pip.sh python3.8 > /build_info/actual_installed_packages/release/python3_pip_packages
