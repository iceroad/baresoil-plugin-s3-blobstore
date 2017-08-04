const _ = require('lodash'),
  assert = require('assert'),
  async = require('async'),
  fs = require('fs'),
  fse = require('fs-extra'),
  path = require('path'),
  temp = require('temp'),
  os = require('os'),
  AWS = require('aws-sdk'),
  LRU = require('lru-cache')
  ;


class BlobStore {
  init(deps, cb) {
    const config = this.config_ = deps.Config.BlobStore;
    this.makeError = deps.MakeError.make;
    this.s3_ = new AWS.S3({
      accessKeyId: config.s3.accessKeyId,
      secretAccessKey: config.s3.secretKey,
      region: config.s3.region,
      params: {
        Bucket: config.s3.bucket,
      },
    });
    this.s3Log_ = deps.EventLog.s3.bind(deps.EventLog);

    console.debug(`BlobStore: local cache size is ${config.s3.localCacheSizeBytes}`);
    this.blobCache_ = LRU({
      max: config.s3.localCacheSizeBytes,
      length: (n, key) => n.length + key.length,
    });
    return cb();
  }

  destroy(deps, cb) {
    return cb();
  }

  put(baseConnection, blobPutRequest, cb) {
    assert(this.isBlobStore);
    const appId = _.toString(_.toInteger(blobPutRequest.appId));
    const etag = encodeURIComponent(blobPutRequest.etag);
    const blobPath = path.join(appId, etag);
    const buffer = Buffer.from(blobPutRequest.data, 'base64');

    // Upload blob to S3.
    this.s3Log_('put', {
      size: buffer.length,
      key: blobPath,
    });
    const startTime = Date.now();
    return this.s3_.putObject({
      Key: blobPath,
      Body: buffer,
      ContentLength: buffer.length,
      ContentType: 'application/octet-stream',
    }, (err) => {
      if (err) {
        console.error(err);
        return cb(this.makeError('internal', {
          message: 'Cannot save blob to BlobStore.',
        }));
      }
      this.blobCache_.set(blobPath, buffer);
      this.s3Log_('put-ok', {
        size: buffer.length,
        key: blobPath,
        queryTimeMs: Date.now() - startTime,
      });
      return cb();
    });
  }

  get(baseConnection, blobGetRequest, cb) {
    assert(this.isBlobStore);
    const appId = _.toString(_.toInteger(blobGetRequest.appId));
    const etag = encodeURIComponent(blobGetRequest.etag);
    const blobPath = path.join(appId, etag);
    const makeError = this.makeError;

    const cached = this.blobCache_.get(blobPath);
    if (cached) {
      this.s3Log_('get-cached', {
        key: blobPath,
        size: cached.length,
      });
      return _.defer(() => {
        cb(null, cached.toString('base64'));
      });
    }

    // Need to retrieve from S3.
    this.s3Log_('get', {
      key: blobPath,
    });
    const startTime = Date.now();
    return this.s3_.getObject({
      Key: blobPath,
    }, (err, result) => {
      if (err) {
        if (err.code !== 'NotFound' && err.code !== 'NoSuchKey') {
          console.error(err);
          return cb(makeError('internal'));
        }

        // Key does not exist in S3, return Error.
        return cb(makeError('not_found', {
          message: 'Blob not found.',
        }));
      }

      const buffer = result.Body;
      this.blobCache_.set(blobPath, buffer);
      this.s3Log_('get-ok', {
        size: buffer.length,
        key: blobPath,
        queryTimeMs: Date.now() - startTime,
      });

      // Item retrieved from S3, return Base64 data.
      return cb(null, buffer.toString('base64'));
    });
  }
}


const MEGABYTES = 1024 * 1024;

BlobStore.prototype.$spec = {
  deps: ['Config', 'EventLog'],
  config: {
    type: 'object',
    desc: 'Options for a filesystem-based BlobStore implementation.',
    fields: {

      s3: {
        type: 'object',
        fields: {

          accessKeyId: {
            type: 'string',
            optional: true,
            desc: 'Custom AWS access key ID for BlobStore use.',
          },

          secretKey: {
            type: 'string',
            optional: true,
            desc: 'Custom AWS secret key for BlobStore use.',
          },

          bucket: {
            type: 'string',
            desc: 'AWS S3 bucket to use for blob storage.',
            minLength: 1,
            maxLength: 63
          },

          region: {
            type: 'string',
            desc: 'AWS region containing the S3 bucket.',
          },

          localCacheSizeBytes: {
            type: 'integer',
            desc: 'Local in-memory blob cache size (in bytes).',
            minValue: 0,
          },

        },
      },
    },
  },
  defaults: {
    s3: {
      localCacheSizeBytes: 256 * MEGABYTES,
    },
  },
  syslib: {
    get: {
      argument: {
        type: 'BlobGetRequest',
      },
    },
    put: {
      argument: {
        type: 'BlobPutRequest',
      },
    },
  },
};


module.exports = BlobStore;
