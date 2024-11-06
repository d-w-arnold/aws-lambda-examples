const {convertTo, canBeConvertedToPDF} = require('@shelf/aws-lambda-libreoffice');

const AWS = require('aws-sdk');
const fs = require('fs');
const s3 = new AWS.S3({apiVersion: '2006-03-01', region: process.env.AWS_REGION});
const stream = require("stream");

const log = require('lambda-log');

const eventKeys = ['source_bucket', 'source_key', 'dest_bucket', 'dest_key'];
const pdf_ = "pdf";

const uploadFile = (filePath) => new Promise((resolve, reject) => {
    let params = {Bucket: dstBucket, Key: dstKey, ContentType: 'application/' + pdf_}

    let pass = new stream.PassThrough();
    params.Body = pass;

    const manager = s3.upload(params, (err, data) => {
        if (err) {
            log.error("## Error uploading file: " + err);
            return "Exit 3";
        } else {
            log.info("## File uploaded successfully: " + data.Location);
        }
    });

    // OPTIONAL
    // manager.on('httpUploadProgress', (progress) => {
    //     log.info('## Progress: ' + progress)
    //     // { loaded: 6472, total: 345486, part: 3, key: 'path/to/my/file.pdf' }
    // });

    manager.on("finish", resolve);

    fs.createReadStream(filePath).pipe(pass);
});

let srcBucket;
let srcKey;
let dstBucket;
let dstKey;

// Lambda Configuration:
// At least 3008 MB of RAM is recommended
// At least 45 seconds of Lambda timeout is necessary
// Set environment variable HOME to /tmp

module.exports.handler = async function (event, context) {

    // EVENT: {
    //     "source_bucket": "dogstoragedev-eu-west-2",
    //     "source_key": "dev/file.docx",
    //     "dest_bucket": "doggwdev-eu-west-2-inst-foobar",
    //     "dest_key": "path/to/my/file.pdf"
    // }
    if (eventKeys.every(key => key in event)) {
        srcBucket = event.source_bucket;
        log.info("## srcBucket: " + srcBucket);
        srcKey = event.source_key;
        log.info("## srcKey: " + srcKey);
        dstBucket = event.dest_bucket;
        log.info("## dstBucket: " + dstBucket);
        dstKey = event.dest_key;
        log.info("## dstKey: " + dstKey);
    } else {
        log.warn('## Not all ' + eventKeys + ' in EVENT: ' + event);
        return "Exit 1";
    }

    // "path/to/my/file.docx" -> "file.docx"
    let srcObjName = srcKey.replace(/^.*[\\/]/, '');
    log.info("## srcObjName: " + srcObjName);

    // "path/to/my/file.pdf" -> "file.pdf"
    let dstObjName = dstKey.replace(/^.*[\\/]/, '');
    log.info("## dstObjName: " + dstObjName);

    let tmpPath = process.env.HOME + "/";
    log.info("## tmpPath: " + tmpPath);
    let srcPath = tmpPath + srcObjName;
    log.info("## srcPath: " + srcPath);

    let file = fs.createWriteStream(srcPath);
    log.info("## Getting source S3 object and writing to: " + srcPath);
    s3.getObject({Bucket: srcBucket, Key: srcKey}).createReadStream().pipe(file);

    // Check file format
    if (!canBeConvertedToPDF(srcObjName)) {
        log.error("## Cannot be converted to PDF: " + srcObjName);
        return "Exit 2";
    }

    // Save PDF file
    let dstPath = await convertTo(srcObjName, pdf_);
    log.info("## dstPath: " + dstPath);

    // Upload PDF file to destination S3 bucket
    // TODO: 1) Update uploadFile() to successfully resolve Promise, and return the response data from s3.upload(),
    //  ref: https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#upload-property
    await uploadFile(dstPath);
    log.info('## All uploads completed');

    // Return a result
    // TODO: 2) Update Lambda response payload to include the response data from s3.upload()
    return {'RESULT': 'Success', "bucket": dstBucket, "key": dstKey};
};