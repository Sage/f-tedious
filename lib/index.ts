import { wait } from 'f-promise';
import { devices, Reader, Writer } from 'f-streams';
import * as tds from 'tedious';

const tracer: (...args: any[]) => void = undefined;

type Callback<T> = (err: any, res?: T) => void;
type Dict<T> = { [k: string]: T };

/// !doc
/// ## ez-streams wrapper for _tedious_ driver (SQL server)
/// 
/// `var eztedious = require('ez-tedious');`
/// 
/// * `reader = eztedious.reader(connection, sql, args)`   
export interface ReaderOptions {
	close?(): void;
	fillParameters?(request: tds.Request, args: any[]): void;
}
export function reader<T>(connection: tds.Connection, sql: string, args: any[], opts?: ReaderOptions): Reader<T> {
	opts = opts || {};
	// state
	let error: any = null,
		callback: Callback<any> = null,
		stopped = false,
		done = false,
		paused = false;

	// buffering for rows that have been received but not yet read
	const received = [] as T[],
		low = 0,
		high = 2;

	// handle the pause/resume dance

	function push(record: T) {
		if (tracer) tracer('pushing ' + JSON.stringify(record));
		received.push(record);
		if (received.length === high) {
			(connection as any).socket.pause();
			paused = true;
		}
	}

	function shift() {
		if (received.length === low + 1) {
			paused = false;
			(connection as any).socket.resume();
		}
		return received.shift();
	}

	function send(err: any, result?: T) {
		if (tracer) tracer('send(' + (err && err.message) + ',' + result + ')');
		const cb = callback;
		callback = null;
		if (cb) {
			if (result) {
				cb(err, result);
			} else {
				done = true;
				cb(null);
			}
		} else {
			error = error || err;
			if (result) {
				// Enqueue the row, il will be dequeued by the generic.reader
				push(result);
			} else {
				done = true;
			}
		}
	}

	function withClose<R>(fn: () => R) {
		if (!opts.close) return fn;
		return function () {
			try {
				return fn();
			} finally {
				if (opts.close) opts.close();
				opts.close = null;
			}
		};
	}
	const rd = devices.generic.reader<T>(withClose(function () {
		return wait(cb => {

			if (tracer) tracer('READ', error, received.length, done);
			if (error) {
				if (request) request.removeAllListeners();
				request = null;
				return cb(error);
			}
			if (received.length) {
				// Dequeue the first available row.
				return cb(undefined, shift());
			}
			if (done) {
				// Notify the caller that we have nothing more to read. The caller will receive 'undefined'.				
				if (request) request.removeAllListeners();
				request = null;
				return cb(undefined);
			}

			// The request is not completed yet, we have to store the callback, it will
			// be invoked later, when a result will be available (see send() method)
			callback = cb;
		});
	}), withClose(function stop() {
		return wait(cb => {
			if (tracer) tracer('TEDIOUS READER STOP', done);
			if (typeof cb !== 'function') throw new Error('bad callback: ' + typeof cb);
			if (done) return cb(undefined);
			stopped = true;
			connection.cancel();
			callback = cb;
		});
	}));

	if (tracer) tracer('reader initialized : ' + sql);

	// create the request
	let request = new tds.Request(sql, function (err, rowCount, rows) {
		if (tracer) tracer('TDS request complete', err, rowCount, rows);
		// ignore error if we have been stopped
		request.removeAllListeners();
		request = null;
		send(stopped ? null : err);
	});

	// set the parameters
	if (typeof opts.fillParameters !== 'function') throw new Error('fillParameters option missing');
	opts.fillParameters(request, args);

	// set up listeners
	request.on('row', function (row: T) {
		if (stopped) return;
		if (tracer) tracer('ROW', row);
		send(null, row);
	});

	// execute the query
	connection.execSql(request);

	return rd;
}

/// * `writer = eztedious.writer(connection, sql, columnDefs)`
/// connection : a sql connection (created by require('tedious').Connection(...))
/// sql : the sql statement (sth like INSERT INTO)
/// columnDefs : a structure that describes the metadata of each parameter
///   should look like { "@p0" : xxx, "@p1" : yyy, ...} where xxx and yyy are objects created 
/// by sqlserver.readTableSchema(...)
export interface WriterOptions {
	describeColumns(request: tds.Request): void;
}
export function writer<T>(connection: tds.Connection, sql: string, opts: WriterOptions): Writer<T> {
	if (!connection) throw new Error('connection is missing');
	if (!sql) throw new Error('sql query is missing');
	if (!opts || typeof opts.describeColumns !== 'function') throw new Error('opts.describeColumns is missing');
	let done = false;
	let callback: Callback<void>;
	let isPreparing = true;
	let pendingParamValues: Dict<any>;
	let shouldUnprepare = false;

	if (tracer) tracer('writer initialized : ' + sql);

	function processError(err: any) {
		if (tracer) tracer('ERROR : ' + JSON.stringify(err));
		connection.removeListener('errorMessage', processError);
		return send(err);
	}

	function send(err: any) {
		const cb = callback;
		callback = null;
		if (cb) {
			if (tracer) tracer('send(' + (err && err.message) + ')');
			cb(err);
		}
	}

	const request = new tds.Request(sql, function (err, rowCount, rows) {
		// Note : this callback is invoked :
		// - when the request is executed
		// - when the request is unprepared
		if (tracer) tracer('writer request completed: err=' + err + ', rowCount=' + rowCount);
		if (isPreparing) {
			if (tracer) tracer('prepared confirmed');
			// here, the send() method must not be invoked. It will be invoked when the first execute
			// is over (so, on the next call of this callback)
			return;
		}
		if (done && tracer) tracer('last call : unlock the caller');
		send(err);
	});

	// Now, we can prepare the request
	if (tracer) tracer('preparing the request');
	opts.describeColumns(request);

	request.on('prepared', function () {
		if (tracer) tracer('request prepared');
		isPreparing = false;
		if (shouldUnprepare) {
			// The writer was closed without having written any row.
			// The write(null) was issued when the request was preparing, the request could not
			// be unprepared because the connection had an invalid state (SentClientRequest). We have 
			// to unprepare it now (once the request is unprepared, the request's callback will be invoked)
			if (tracer) tracer('unpreparing the request');
			connection.unprepare(request);
			shouldUnprepare = false;
			return;
		}
		if (pendingParamValues) {
			if (tracer) tracer('dequeing values ' + JSON.stringify(pendingParamValues));
			connection.execute(request, pendingParamValues);
		}
	});

	// connection.prepare is asynchronous. The connection will have an invalid state until the 'prepared'
	// event is received. Before this event is received, we MUST NOT launch any connection.execute(...)
	connection.prepare(request);

	// Note : the 'error' event is only available on the connection
	// As connections are pooled, we will have to unregister this event before leaving ...
	connection.on('errorMessage', processError);

	//var requestPrepared = false;
	return devices.generic.writer(function (obj) {
		return wait(cb => {

			if (done) return;
			callback = cb;
			if ((obj === undefined) && !done) {
				// End of writing operation.
				done = true; // from now, we must not write anything more
				if (isPreparing) {
					// This case occurs when the writer has been created and closed without having written any row
					// The request is being prepared (the 'prepare' event was not fired yet).
					// The request will be unprepared as soon as the 'prepared' event is received					
					shouldUnprepare = true;
					if (tracer) tracer('should unprepare');
				} else {
					if (tracer) tracer('unpreparing the request');
					connection.unprepare(request);
				}
				connection.removeListener('errorMessage', processError);
				// Note : we must not invoke the send() method here because the connection has an invalid state
				// it's processing the unprepare. When the unprepare will be over, the callback bound to the request
				// will be invoked and then the send() method will be invoked.
				return;
			}
			try {
				const vals = Array.isArray(obj) ? obj : Object.keys(obj).map((k, i) => (obj as any)[k]);

				const paramValues = {} as Dict<any>;
				vals.forEach(function (value, index) {
					paramValues['p' + index] = value;
				});

				if (isPreparing) {
					// We can't execute the request now, the connection is not in a valid state
					// We have to enqueue the values. The request will be executed as soon as
					// the request will be prepared (see request.on('prepared') for more details)
					if (tracer) tracer("waiting for request's prepare : enqueing values " + JSON.stringify(paramValues));
					pendingParamValues = paramValues;
				} else {
					if (tracer) tracer('execute values ' + JSON.stringify(paramValues));
					connection.execute(request, paramValues);
				}
			} catch (err) {
				if (tracer) tracer('ERROR : ' + err);
				if (callback) {
					// Do not invoke callback twice
					callback(err);
					callback = null;
				}
			}
		});

	});
}
