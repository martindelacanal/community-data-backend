'use strict';

const { Readable } = require('node:stream');
const { pipeline } = require('node:stream/promises');

function abortError() {
  const error = new Error('Report cancelled');
  error.name = 'AbortError';
  error.code = 'ABORT_ERR';
  return error;
}

// Each query owns its connection only until its result is available. A slow
// download never monopolizes the pool, and cancellation destroys only this
// report's active connection, never a connection shared with another request.
function createReportQuery(pool, { signal, timeoutMs = 30000 } = {}) {
  return (sql, values = []) => new Promise((resolve, reject) => {
    let connection;
    let settled = false;
    let timer;
    const finish = (error, rows, fields) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      signal?.removeEventListener('abort', onAbort);
      if (error) {
        connection?.destroy();
        reject(error);
      } else {
        connection.release();
        resolve([rows, fields]);
      }
    };
    const onAbort = () => finish(abortError());
    if (signal?.aborted) return onAbort();
    signal?.addEventListener('abort', onAbort, { once: true });
    timer = setTimeout(() => {
      const error = new Error('Report query exceeded its time limit');
      error.code = 'REPORT_QUERY_TIMEOUT';
      finish(error);
    }, timeoutMs);
    timer.unref?.();
    pool.getConnection((error, acquired) => {
      if (settled) {
        acquired?.release();
        return;
      }
      if (error) return finish(error);
      connection = acquired;
      // A server limit also bounds work if the TCP connection is interrupted.
      const boundedSql = sql.replace(/^\s*SELECT\b/i,
        (select) => `${select} /*+ MAX_EXECUTION_TIME(${timeoutMs}) */`);
      try {
        connection.query({ sql: boundedSql, timeout: timeoutMs }, values, finish);
      } catch (queryError) {
        finish(queryError);
      }
    });
  });
}

function createReportReadable(generator, controller, signal) {
  const body = Readable.from(generator, { objectMode: false, highWaterMark: 64 * 1024 });
  const destroy = body._destroy;
  // Abort before Readable.from waits for an in-flight async iterator.next().
  body._destroy = function (error, callback) {
    controller.abort();
    destroy.call(this, error, (destroyError) => {
      // destroy() is an intentional, error-free stop (for example after an
      // upload fails). The query's cancellation is not a second stream error.
      callback(!error && destroyError?.code === 'ABORT_ERR' ? null : destroyError);
    });
  };
  const onAbort = () => body.destroy(abortError());
  if (signal?.aborted) queueMicrotask(onAbort);
  else signal?.addEventListener('abort', onAbort, { once: true });
  body.once('close', () => signal?.removeEventListener('abort', onAbort));
  return body;
}

async function sendReportDownload(req, res, createReport) {
  const controller = new AbortController();
  const onClose = () => {
    if (!res.writableFinished) controller.abort();
  };
  const onAborted = () => controller.abort();
  res.once('close', onClose);
  req.once('aborted', onAborted);
  let body;
  try {
    if (req.aborted || res.destroyed) controller.abort();
    controller.signal.throwIfAborted();
    const report = await createReport(controller.signal);
    body = report.body;
    controller.signal.throwIfAborted();
    res.setHeader('Content-Disposition', `attachment; filename="${report.fileName}"`);
    res.setHeader('Content-Type', report.contentType || 'text/csv; charset=utf-8');
    res.setHeader('Cache-Control', 'no-store');
    res.setHeader('X-Accel-Buffering', 'no');
    await pipeline(body, res, { signal: controller.signal });
  } catch (error) {
    if (controller.signal.aborted || error.code === 'ERR_STREAM_PREMATURE_CLOSE') return;
    if (!res.headersSent && !res.destroyed) {
      res.removeHeader('Content-Disposition');
      res.status(error.code === 'REPORT_QUERY_TIMEOUT' ? 504 : 500).json('Could not generate report');
    } else if (!res.destroyed) {
      res.destroy(error);
    }
    if (error.code !== 'REPORT_QUERY_TIMEOUT') console.error('Report download failed:', error.code || error.name);
  } finally {
    controller.abort();
    body?.destroy();
    res.removeListener('close', onClose);
    req.removeListener('aborted', onAborted);
  }
}

module.exports = { abortError, createReportQuery, createReportReadable, sendReportDownload };
