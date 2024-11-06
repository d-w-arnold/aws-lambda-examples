#
# Pdf (DOCX-to-PDF) Micro-service image.
#

ARG BASE_IMG_NAME
ARG BASE_IMG_TAG
# FROM foobar/lambda-libreoffice-base:7.6-node18-x86_64
FROM $BASE_IMG_NAME:$BASE_IMG_TAG

# Copy lambda function code.
ARG LAMBDA_FUNC_SOURCE
COPY "${LAMBDA_FUNC_SOURCE}/handler.js" "${LAMBDA_TASK_ROOT}"
COPY "${LAMBDA_FUNC_SOURCE}/package.json" "${LAMBDA_TASK_ROOT}"

# Install required dependencies (and any sub-dependencies).
RUN npm install

# Set lambda function handler.
CMD [ "handler.handler" ]
