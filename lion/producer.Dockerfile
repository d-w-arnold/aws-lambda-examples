#
# LION Producer image.
#

ARG BASE_IMG_NAME
ARG BASE_IMG_TAG
# FROM lion-producer-bin:latest
FROM $BASE_IMG_NAME:$BASE_IMG_TAG

# Install general Python dependencies.
RUN pip install --upgrade pip && pip install wheel

# Copy pip config.
WORKDIR /pip
ARG PIP_CONF_FIP
COPY $PIP_CONF_FIP ./pip.conf
ENV PIP_CONFIG_FILE=/pip/pip.conf

# Install specific versions of Boto3 and Botocore
COPY ./py_layer.zip ./layer.zip
RUN unzip layer.zip -d /opt
RUN rm layer.zip

# Install AWS Parameter and Secrets Lambda extension to cache parameters and secrets
ARG PARAMS_AND_SECRETS_EXT_ZIP
COPY "${PARAMS_AND_SECRETS_EXT_ZIP}" .
RUN unzip "${PARAMS_AND_SECRETS_EXT_ZIP}" -d /opt
RUN rm "${PARAMS_AND_SECRETS_EXT_ZIP}"

# Copy lambda function code.
ARG LAMBDA_FUNC_SOURCE
COPY "${LAMBDA_FUNC_SOURCE}/lambda_function.py" "${LAMBDA_TASK_ROOT}"

# Install sih-lion dependency (and any sub-dependencies).
ARG SIH_LION_EXTRAS=""
ARG SIH_LION_VERSION=1.5.2
RUN pip install "sih-lion${SIH_LION_EXTRAS}~=${SIH_LION_VERSION}" --target "${LAMBDA_TASK_ROOT}"

# Install other required dependencies (and any sub-dependencies).
COPY "${LAMBDA_FUNC_SOURCE}/requirements.txt" .
RUN pip3 install -r "requirements.txt" --target "${LAMBDA_TASK_ROOT}"
RUN rm "requirements.txt"

# Delete pip config.
WORKDIR /
RUN rm -rf pip

# Set lambda function handler.
CMD [ "lambda_function.lambda_handler" ]
