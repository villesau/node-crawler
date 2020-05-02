'use strict';

const path = require('path'),
  util = require('util'),
  EventEmitter = require('events').EventEmitter,
  request = require('request'),
  _ = require('lodash'),
  cheerio = require('cheerio'),
  fs = require('fs'),
  Bottleneck = require('bottleneckp'),
  seenreq = require('seenreq'),
  iconvLite = require('iconv-lite'),
  typeis = require('type-is').is;

let whacko = null,
  level;
const levels = [
  'silly',
  'debug',
  'verbose',
  'info',
  'warn',
  'error',
  'critical',
];
try {
  whacko = require('whacko');
} catch (e) {
  e.code;
}

function defaultLog() {
  //2016-11-24T12:22:55.639Z - debug:
  if (levels.indexOf(arguments[0]) >= levels.indexOf(level))
    console.log(
      new Date().toJSON() + ' - ' + arguments[0] + ': CRAWLER %s',
      util.format.apply(util, Array.prototype.slice.call(arguments, 1))
    );
}

function checkJQueryNaming(options) {
  if ('jquery' in options) {
    options.jQuery = options.jquery;
    delete options.jquery;
  }
  return options;
}

function readJqueryUrl(url, callback) {
  if (url.match(/^(file:\/\/|\w+:|\/)/)) {
    fs.readFile(url.replace(/^file:\/\//, ''), 'utf-8', function (err, jq) {
      callback(err, jq);
    });
  } else {
    callback(null, url);
  }
}

function contentType(res) {
  return get(res, 'content-type')
    .split(';')
    .filter((item) => item.trim().length !== 0)
    .join(';');
}

function get(res, field) {
  return res.headers[field.toLowerCase()] || '';
}

let log = defaultLog;

class Crawler extends EventEmitter {
  constructor(options) {
    super();
    options = options || {};
    if (['onDrain', 'cache'].some((key) => key in options)) {
      throw new Error(
        'Support for "onDrain", "cache" has been removed! For more details, see https://github.com/bda-research/node-crawler'
      );
    }

    this.init(options);
  }
  init(options) {
    const defaultOptions = {
      autoWindowClose: true,
      forceUTF8: true,
      gzip: true,
      incomingEncoding: null,
      jQuery: true,
      maxConnections: 10,
      method: 'GET',
      priority: 5,
      priorityRange: 10,
      rateLimit: 0,
      referer: false,
      retries: 3,
      retryTimeout: 10000,
      timeout: 15000,
      skipDuplicates: false,
      rotateUA: false,
      homogeneous: false,
    };

    //return defaultOptions with overriden properties from options.
    this.options = _.extend(defaultOptions, options);

    // you can use jquery or jQuery
    this.options = checkJQueryNaming(this.options);

    // Don't make these options persist to individual queries
    this.globalOnlyOptions = [
      'maxConnections',
      'rateLimit',
      'priorityRange',
      'homogeneous',
      'skipDuplicates',
      'rotateUA',
    ];

    this.limiters = new Bottleneck.Cluster(
      this.options.maxConnections,
      this.options.rateLimit,
      this.options.priorityRange,
      this.options.priority,
      this.options.homogeneous
    );

    level = this.options.debug ? 'debug' : 'info';

    if (this.options.logger)
      log = this.options.logger.log.bind(this.options.logger);

    this.log = log;

    this.seen = new seenreq(this.options.seenreq);
    this.seen
      .initialize()
      .then(() => log('debug', 'seenreq is initialized.'))
      .catch((e) => log('error', e));

    this.on('_release', () => {
      log('debug', 'Queue size: %d', this.queueSize);

      if (this.limiters.empty) return this.emit('drain');
    });
  }

  setLimiterProperty(limiter, property, value) {
    switch (property) {
      case 'rateLimit':
        this.limiters.key(limiter).setRateLimit(value);
        break;
      default:
        break;
    }
  }

  _inject(response, options, callback) {
    let $;

    if (options.jQuery === 'whacko') {
      if (!whacko) {
        throw new Error(
          'Please install whacko by your own since `crawler` detected you specify explicitly'
        );
      }

      $ = whacko.load(response.body);
      callback(null, response, options, $);
    } else if (
      options.jQuery === 'cheerio' ||
      options.jQuery.name === 'cheerio' ||
      options.jQuery === true
    ) {
      const defaultCheerioOptions = {
        normalizeWhitespace: false,
        xmlMode: false,
        decodeEntities: true,
      };
      const cheerioOptions = options.jQuery.options || defaultCheerioOptions;
      $ = cheerio.load(response.body, cheerioOptions);

      callback(null, response, options, $);
    } else if (options.jQuery.jsdom) {
      const jsdom = options.jQuery.jsdom;
      const scriptLocation = path.resolve(
        __dirname,
        '../vendor/jquery-2.1.1.min.js'
      );

      //Use promises
      readJqueryUrl(scriptLocation, (err, jquery) => {
        try {
          jsdom.env({
            url: options.uri,
            html: response.body,
            src: [jquery],
            done: (errors, window) => {
              $ = window.jQuery;
              callback(errors, response, options, $);

              try {
                window.close();
                window = null;
              } catch (err) {
                log('error', err);
              }
            },
          });
        } catch (e) {
          options.callback(e, { options }, options.release);
        }
      });
    }
    // Jquery is set to false are not set
    else {
      callback(null, response, options);
    }
  }

  isIllegal(options) {
    return (
      _.isNull(options) ||
      _.isUndefined(options) ||
      (!_.isString(options) && !_.isPlainObject(options))
    );
  }

  direct(options) {
    if (this.isIllegal(options) || !_.isPlainObject(options)) {
      return log('warn', 'Illegal queue option: ', JSON.stringify(options));
    }

    if (!('callback' in options) || !_.isFunction(options.callback)) {
      return log(
        'warn',
        'must specify callback function when using sending direct request with crawler'
      );
    }

    options = checkJQueryNaming(options);

    // direct request does not follow the global preRequest
    options.preRequest = options.preRequest || null;

    _.defaults(options, this.options);

    // direct request is not allowed to retry
    options.retries = 0;

    // direct request does not emit event:'request' by default
    options.skipEventRequest = _.isBoolean(options.skipEventRequest)
      ? options.skipEventRequest
      : true;

    this.globalOnlyOptions.forEach(
      (globalOnlyOption) => delete options[globalOnlyOption]
    );

    this._buildHttpRequest(options);
  }

  queue(options) {
    // Did you get a single object or string? Make it compatible.
    options = _.isArray(options) ? options : [options];

    options = _.flattenDeep(options);

    for (let i = 0; i < options.length; ++i) {
      if (this.isIllegal(options[i])) {
        log('warn', 'Illegal queue option: ', JSON.stringify(options[i]));
        continue;
      }
      this._pushToQueue(
        _.isString(options[i]) ? { uri: options[i] } : options[i]
      );
    }
  }

  get queueSize() {
    return this.limiters.unfinishedClients;
  }

  _pushToQueue(options) {
    // you can use jquery or jQuery
    options = checkJQueryNaming(options);

    _.defaults(options, this.options);
    options.headers = _.assign({}, this.options.headers, options.headers);

    // Remove all the global options from our options
    // TODO we are doing this for every _pushToQueue, find a way to avoid this
    this.globalOnlyOptions.forEach(
      (globalOnlyOption) => delete options[globalOnlyOption]
    );

    // If duplicate skipping is enabled, avoid queueing entirely for URLs we already crawled
    if (!this.options.skipDuplicates) {
      this._schedule(options);
      return;
    }

    this.seen
      .exists(options, options.seenreq)
      .then((rst) => {
        if (!rst) {
          this._schedule(options);
        }
      })
      .catch((e) => log('error', e));
  }

  _schedule(options) {
    this.emit('schedule', options);

    this.limiters
      .key(options.limiter || 'default')
      .submit(options.priority, (done, limiter) => {
        options.release = () => {
          done();
          this.emit('_release');
        };
        if (!options.callback) options.callback = options.release;

        if (limiter) {
          this.emit('limiterChange', options, limiter);
        }

        if (options.html) {
          this._onContent(null, options, {
            body: options.html,
            headers: { 'content-type': 'text/html' },
          });
        } else if (typeof options.uri === 'function') {
          options.uri((uri) => {
            options.uri = uri;
            this._buildHttpRequest(options);
          });
        } else {
          this._buildHttpRequest(options);
        }
      });
  }

  _buildHTTPRequest(options) {
    log('debug', options.method + ' ' + options.uri);
    if (options.proxy) log('debug', 'Use proxy: %s', options.proxy);

    // Cloning keeps the opts parameter clean:
    // - some versions of "request" apply the second parameter as a
    // property called "callback" to the first parameter
    // - keeps the query object fresh in case of a retry

    const ropts = _.assign({}, options);

    if (!ropts.headers) {
      ropts.headers = {};
    }
    if (ropts.forceUTF8) {
      ropts.encoding = null;
    }
    // specifying json in request will have request sets body to JSON representation of value and
    // adds Content-type: application/json header. Additionally, parses the response body as JSON
    // so the response will be JSON object, no need to deal with encoding
    if (ropts.json) {
      options.encoding = null;
    }
    if (ropts.userAgent) {
      if (this.options.rotateUA && _.isArray(ropts.userAgent)) {
        ropts.headers['User-Agent'] = ropts.userAgent[0];
        // If "rotateUA" is true, rotate User-Agent
        options.userAgent.push(options.userAgent.shift());
      } else {
        ropts.headers['User-Agent'] = ropts.userAgent;
      }
    }

    if (ropts.referer) {
      ropts.headers.Referer = ropts.referer;
    }

    if (ropts.proxies && ropts.proxies.length) {
      ropts.proxy = ropts.proxies[0];
    }

    const doRequest = (err) => {
      if (err) {
        err.message =
          'Error in preRequest' +
          (err.message ? ', ' + err.message : err.message);
        switch (err.op) {
          case 'retry':
            log('debug', err.message + ', retry ' + options.uri);
            this._onContent(err, options);
            break;
          case 'fail':
            log('debug', err.message + ', fail ' + options.uri);
            options.callback(err, { options: options }, options.release);
            break;
          case 'abort':
            log('debug', err.message + ', abort ' + options.uri);
            options.release();
            break;
          case 'queue':
            log('debug', err.message + ', queue ' + options.uri);
            this.queue(options);
            options.release();
            break;
          default:
            log('debug', err.message + ', retry ' + options.uri);
            this._onContent(err, options);
            break;
        }
        return;
      }

      if (ropts.skipEventRequest !== true) {
        this.emit('request', ropts);
      }

      const requestArgs = [
        'uri',
        'url',
        'qs',
        'method',
        'headers',
        'body',
        'form',
        'formData',
        'json',
        'multipart',
        'followRedirect',
        'followAllRedirects',
        'maxRedirects',
        'removeRefererHeader',
        'encoding',
        'pool',
        'timeout',
        'proxy',
        'auth',
        'oauth',
        'strictSSL',
        'jar',
        'aws',
        'gzip',
        'time',
        'tunnel',
        'proxyHeaderWhiteList',
        'proxyHeaderExclusiveList',
        'localAddress',
        'forever',
        'agent',
        'strictSSL',
        'agentOptions',
        'agentClass',
      ];

      request(
        _.pick.apply(this, [ropts].concat(requestArgs)),
        (error, response) => {
          if (error) {
            return this._onContent(error, options);
          }

          this._onContent(error, options, response);
        }
      );
    };

    if (options.preRequest && _.isFunction(options.preRequest)) {
      options.preRequest(ropts, doRequest);
    } else {
      doRequest();
    }
  }

  _onContent(error, options, response) {
    if (error) {
      log(
        'error',
        'Error ' +
          error +
          ' when fetching ' +
          (options.uri || options.url) +
          (options.retries ? ' (' + options.retries + ' retries left)' : '')
      );

      if (options.retries) {
        setTimeout(() => {
          options.retries--;
          this._schedule(options);
          options.release();
        }, options.retryTimeout);
      } else {
        options.callback(error, { options: options }, options.release);
      }

      return;
    }

    if (!response.body) {
      response.body = '';
    }

    log(
      'debug',
      'Got ' +
        (options.uri || 'html') +
        ' (' +
        response.body.length +
        ' bytes)...'
    );

    try {
      this._doEncoding(options, response);
    } catch (e) {
      log('error', e);
      return options.callback(e, { options: options }, options.release);
    }

    response.options = options;

    if (options.method === 'HEAD' || !options.jQuery) {
      return options.callback(null, response, options.release);
    }

    const injectableTypes = [
      'html',
      'xhtml',
      'text/xml',
      'application/xml',
      '+xml',
    ];
    if (!options.html && !typeis(contentType(response), injectableTypes)) {
      log(
        'warn',
        'response body is not HTML, skip injecting. Set jQuery to false to suppress this message'
      );
      return options.callback(null, response, options.release);
    }

    log('debug', 'Injecting');

    this._inject(response, options, this._injected.bind(this));
  }

  _injected(errors, response, options, $) {
    log('debug', 'Injected');

    response.$ = $;
    options.callback(errors, response, options.release);
  }

  _doEncoding(options, response) {
    if (options.encoding === null) {
      return;
    }

    if (options.forceUTF8) {
      const charset = options.incomingEncoding || this._parseCharset(response);
      response.charset = charset;
      log('debug', 'Charset ' + charset);

      if (charset !== 'utf-8' && charset !== 'ascii') {
        // convert response.body into 'utf-8' encoded buffer
        response.body = iconvLite.decode(response.body, charset);
      }
    }

    response.body = response.body.toString();
  }

  _parseCharset(res) {
    //Browsers treat gb2312 as gbk, but iconv-lite not.
    //Replace gb2312 with gbk, in order to parse the pages which say gb2312 but actually are gbk.
    const getCharset = (str) => {
      const charset = ((str && str.match(/charset=['"]?([\w.-]+)/i)) || [
        0,
        null,
      ])[1];
      return charset && charset.replace(/:\d{4}$|[^0-9a-z]/g, '') == 'gb2312'
        ? 'gbk'
        : charset;
    };
    const charsetParser = (header, binary, default_charset = null) => {
      return getCharset(header) || getCharset(binary) || default_charset;
    };

    var charset = charsetParser(contentType(res));
    if (charset) return charset;

    if (!typeis(contentType(res), ['html'])) {
      log(
        'debug',
        'Charset not detected in response headers, please specify using `incomingEncoding`, use `utf-8` by default'
      );
      return 'utf-8';
    }

    const body = res.body instanceof Buffer ? res.body.toString() : res.body;
    charset = charsetParser(contentType(res), body, 'utf-8');

    return charset;
  }
}

module.exports = Crawler;
