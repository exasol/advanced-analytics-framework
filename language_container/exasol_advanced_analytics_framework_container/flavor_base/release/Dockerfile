FROM exasol/script-language-container:template-Exasol-all-python-3.10-release_BFRSH344TDRPT7LK2FBOJK4KBIDW6A253FFPYEUYT4O2ERFMTCNA

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git

COPY release/dist /project
RUN python3.10 -m pip install --use-deprecated=legacy-resolver /project/*.whl

RUN mkdir -p /build_info/actual_installed_packages/release && \
    /scripts/list_installed_scripts/list_installed_apt.sh > /build_info/actual_installed_packages/release/apt_get_packages && \
    /scripts/list_installed_scripts/list_installed_pip.sh python3.10 > /build_info/actual_installed_packages/release/python3_pip_packages
