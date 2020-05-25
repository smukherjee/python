#!/usr/bin/env bash
# POSIX

#sudo yum update -y
#sudo yum install -y git

die() {
    printf '%s\n' "$1" >&2
    exit 1
}

# Initialize all the option variables.
# This ensures we are not contaminated by variables from the environment.
eggs=
extensions=

while :; do
    case $1 in
        -p|--eggs)       # Takes an option argument; ensure it has been specified.
            if [ "$2" ]; then
                eggs=$2
                shift
            else
                die 'ERROR: "--eggs" requires a non-empty option argument.'
            fi
            ;;
        --eggs=?*)
            eggs="${1#*=}"
            for egg in ${eggs//,/ }; do
              aws s3 cp "$egg" .
              echo "Installing $egg"
              sudo python3.6 -m easy_install --no-deps "$(basename "$eggs")"
            done
            ;;
        --eggs=)         # Handle the case of an empty --eggs=
            die 'ERROR: "--eggs" requires a non-empty option argument.'
            ;;
        -e|--extensions)       # Takes an option argument; ensure it has been specified.
            if [ "$2" ]; then
                extensions=$2
                shift
            else
                die 'ERROR: "--extensions" requires a non-empty option argument.'
            fi
            ;;
        --extensions=?*)
            extensions="${1#*=}"
            # Wait some time for docker to finish getting started
            /usr/bin/sudo /usr/bin/docker exec jupyterhub conda update conda
            # /usr/bin/sudo /usr/bin/docker exec jupyterhub conda install -c conda-forge jupyterhub==1.2.0
            # /usr/bin/sudo /usr/bin/docker exec jupyterhub jupyterhub upgrade-db
            /usr/bin/sudo /usr/bin/docker exec jupyterhub conda install dask gcsfs autopep8 jupyterlab jupyter_nbextensions_configurator jupyter_contrib_nbextensions
            /usr/bin/sudo /usr/bin/docker exec jupyterhub jupyter nbextensions_configurator enable --user
            /usr/bin/sudo /usr/bin/docker exec jupyterhub jupyter labextension install @jupyterlab/hub-extension
            for extension in ${extensions//,/ }; do
              echo "Enabling Jupyter extension $extension"
              /usr/bin/sudo /usr/bin/docker exec jupyterhub jupyter nbextension enable "$extension"
            done
            ;;
        --extensions=)         # Handle the case of an empty --extensions=
            die 'ERROR: "--extensions" requires a non-empty option argument.'
            ;;
        --)              # End of all options.
            shift
            break
            ;;
        -?*)
            printf 'WARN: Unknown option (ignored): %s\n' "$1" >&2
            ;;
        *)               # Default case: No more options, so break out of the loop.
            break
    esac

    shift
done

# Rest of the program here.
# If there are input files (for example) that follow the options, they
# will remain in the "$@" positional parameters.

#git clone --single-branch --branch develop https://code.platform.aero/coe-ds/coe-ds-common.git
#git clone --single-branch --branch develop https://code.platform.aero/coe-ds/coe-ds-pipeline-flightaware.git

# PYTHONPATH=../coe-ds-common/ pyspark

# cd ~/coe-ds-pipeline-flightaware; PYTHONPATH=.:../coe-ds-common/ spark-submit scripts/flightaware_script.py > fa.log 2>&1 &
