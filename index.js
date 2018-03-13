const BigQuery = require('@google-cloud/bigquery');
const Storage = require('@google-cloud/storage');
const dotenv = require('dotenv').config();

const projectId = process.env.PROJECT_ID;

const bigquery = BigQuery({
  projectId: projectId
});

const storage = Storage({
  projectId: projectId,
});

exports.index = function (event, callback) {
    const file = event.data;
    if (file.resourceState === 'not_exists') {
        console.log('File ' + file.name + ' not_exists.');
    } else {
        if (file.metageneration === '1') {
            console.log('File + ' + file.name + ' created.');
        } else {
            console.log('File + ' + file.name + ' metadata updated.');
        }
        locdGCStoBigQuery(event, callback);
    }
};

function locdGCStoBigQuery(event, callback) {
  const filepaths = event.data.name.split('/');
  if (!isLogData(filepaths)) {
    callback();
    return;
  }

  const datasetId = event.data.bucket.replace(/-/g , '_');
  const bucketName = event.data.bucket;
  const tableId = getLogName(filepaths);
  const partitionId = getLogDay(filepaths);
  const filename = event.data.name;
  const metadata = {
      ignoreUnknownValues: true,
      sourceFormat: 'NEWLINE_DELIMITED_JSON'
  };

  bigquery
    .dataset(datasetId)
    .table(`${tableId}$${partitionId}`)
    .load(storage.bucket(bucketName).file(filename), metadata)
    .then(results => {
      const job = results[0];
      console.log(`Job ${datasetId} ${tableId}$${partitionId} ${bucketName}/${filename} ${job.id} started.`);

      return job;
    })
    .catch(err => {
      console.error('ERROR:', err);
    });

  callback();
}

function isLogData(filepaths) {
  return (filepaths[0] == 'data') ? true : false;
}

function getLogName(filepaths) {
  return filepaths[1];
}

function getLogDay(filepaths) {
  return filepaths[2].replace(/-/g , '');
}
