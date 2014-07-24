#!/usr/bin/env node
var r = require('rethinkdb'),
	async = require('async'),
	program = require('commander'),
	fs = require('fs'),
	path = require('path'),
	config = require('package.json');

program
	.version(config.version)
	.description(config.description)
	.option('-h, --host [value]', 'Hostname')
	.option('-p, --port [value]', 'Port')
	.option('-d, --db, --database [value]', 'Database Name')
	.option('-a, --auth, --authentication', 'Authentication')
	.option('-f, --folder [value]', 'Folder')
	.option('-c, --cwd [value]', 'Current Working Directory')
	.option('-i, --import', 'Set functionality to Import')
	.parse(process.argv);

if(program.args.length == 0){
	program.help();
}

// defaults
program.cwd = program.cwd || process.cwd();
program.host = program.host || 'localhost';
program.port = program.port || 28015;
program.db = program.db || 'test';

var today = new Date();
program.folder = program.folder || './' + (today.getMonth()+1) + '-' + today.getDate() + '-' + today.getFullYear() + '_' + (today.getHours()+1) + '.' + today.getMinutes() + '.' + today.getSeconds();

var targetFolder = String(program.cwd + '/' + program.folder + '/').replace(/[\/\/]+/g, '/'),
	targetTables = false,
	targetLogfile = targetFolder + 'export.log',
	logFile = false;

if(program.import){
	targetLogfile = targetFolder + 'import.log';
}

var log = function(type, message){
	if(message == undefined && type){
		message = type;
		type = 'log';
	}
	if(type == 'error'){
		message = '[ERROR] '+ message;
		console.error(message);
	}else{
		message = '[INFO] '+ message;
		console.log(message);
	}
	if(logFile){
		try{
			fs.appendFileSync(targetLogfile, new Date().toISOString() + ' ' + message + "\r\n");
		}catch(err){
			console.error('Failed to write to log!')
			console.error(err);
			process.exit();
		}
	}
}

var config = {
	host: program.host,
	port: program.port,
	db: program.db
};

log('Connecting to Rethink server ['+program.host+':'+program.port+']');
if(program.auth){
	program.password('Authentication Key:', function(desc){
	  config.authKey = desc;
	  continueBackup()
	});
}else{
	continueBackup();
}

function continueBackup(){
	r.connect(config, function(err, conn){
		if(err){
			log('error', 'Failed to connect to Rethink server.')
			log('error', err);
			process.exit();
		}
		if(program.import){
			var targetTables = [],
				existingTables = [];
			async.series([
				function(callback){
					// check if we can use the folder
					fs.exists(targetFolder, function(exists){
						if(exists){
							fs.readdir(targetFolder, function(err, files){
								if(err){
									return callback({
										error: err,
										message: 'Failed to read target folder ('+targetFolder+').'
									});
								}
								for(var i = files.length - 1; i >= 0; i--){
									var ext = path.extname(files[i]);
									if(ext == '.json'){
										targetTables.push(String(files[i]).replace(ext, ''));
									}
								};
								if(targetTables.length == 0){
									return callback({
										message: 'Target folder does not contain any backups.'
									});
								}
								return callback();
							})
						}else{
							return callback({
								message: 'Could not find target folder ('+targetFolder+').'
							});
						}
					})
				},
				function(callback){
					fs.appendFile(targetLogfile, 'Rethink DB import for ' + today.toISOString() + "\r\n" + '---------------------------' + "\r\n", function (err) {
					  if(err){
					  	return callback({
					  		error: err,
					  		message: 'Failed to write to log file in target directory'
					  	});
					  }
					  logFile = true;
					  return callback();
					});
				},
				function(callback){
					r.tableList().run(conn, function(err, results){
						if(err){
							return callback({
								error: err,
								message: 'Failed to list tables from database.'
							});
						}
						existingTables = results;
						return callback();
					});
				},
				function(callback){
					var count = 1;
					async.eachSeries(targetTables, function(table, tableCallback){
						log('Importing table ' + count + '/' + targetTables.length + ' [' + table + ']');
						var doImport = function(){
							fs.readFile(targetFolder + table + '.json', {
								encoding: 'utf8'
							}, function(err, results){
								if(err){
									return tableCallback({
										error: err,
										message: 'Failed to read table json file ('+table+').'
									});
								}
								try{
									var data = JSON.parse(results);
								}catch(e){
									return tableCallback({
										error: err,
										message: 'Failed to parse table json file ('+table+').'
									});
								}
								r.table(table).insert(data, {
									upsert: true
								}).run(conn, function(err, results){
									if(err){
										return tableCallback({
											error: err,
											message: 'Failed to write to database ('+table+').'
										});
									}
									count++;
									tableCallback();
								})
							})
						};
						if(existingTables.indexOf(table) === -1){
							// create the table
							r.tableCreate(table).run(conn, function(err, results){
								if(err){
									return tableCallback({
										error: err,
										message: 'Failed to create table ('+table+') in database.'
									});
								}
								return doImport();
							})
						}else{
							return doImport();
						}
					}, function(err){
						return callback(err);
					});
				}
			], function(err){
				conn.close();
				if(err){
					log('error', err.message);
					if(err.error){
						log('error', err.error);
					}
				}else{
					log('Finished');
					process.exit();
				}
			});
		}else{
			async.series([
				function(callback){
					// check if we can use the folder
					fs.exists(targetFolder, function(exists){
						if(exists){
							fs.readdir(targetFolder, function(err, files){
								if(err){
									return callback({
										error: err,
										message: 'Failed to read target folder ('+targetFolder+').'
									});
								}
								if(files.length > 0){
									return callback({
										message: 'Target folder is not empty. Please clear folder or use another target.'
									});
								}
								return callback();
							})
						}else{
							fs.mkdir(targetFolder, function(err){
								if(err){
									return callback({
										error: err,
										message: 'Could not create target folder ('+targetFolder+').'
									});
								}
								return callback();
							});
						}
					})
				},
				function(callback){
					fs.appendFile(targetLogfile, 'Rethink DB backup for ' + today.toISOString() + "\r\n" + '---------------------------' + "\r\n", function (err){
					  if(err){
					  	return callback({
					  		error: err,
					  		message: 'Failed to write to log file in Target Directory'
					  	});
					  }
					  logFile = true;
					  return callback();
					});
				},
				function(callback){
					r.tableList().run(conn, function(err, results){
						if(err){
							return callback({
								error: err,
								message: 'Failed to list tables from database.'
							});
						}
						if(results.length > 0){
							targetTables = results;
						}
						return callback();
					});
				},
				function(callback){
					if(targetTables == false){
						return callback({
							error: err,
							message: 'No tables were found to backup.'
						});
					}
					log('Iterating over all tables (' + targetTables.length + ')');
					var count = 1;
					async.eachSeries(targetTables, function(table, tableCallback){
						log('Backing up table ' + count + '/' + targetTables.length + ' [' + table + ']');
						var currentLog = fs.createWriteStream(targetFolder + table + '.json'),
							first = true;
						currentLog.on('error', function(err){
							return tableCallback(err);
						});
						currentLog.on('close', function(err){
							count++;
							return tableCallback();
						});
						currentLog.write('[');
						r.table(table).run(conn, {
							timeFormat: 'raw'
						}, function(err, cursor){
							if(err){
								return tableCallback(err);
							}
							var fetchNext = function(err, row){
								if(err){
									return tableCallback(err);
								}
								if(first == true){
									first = false;
								}else{
									currentLog.write(',');
								}
								currentLog.write(JSON.stringify(row));
								if(cursor.hasNext()){
									cursor.next(fetchNext);
								}else{
									cursor.close();
									currentLog.write(']');
									currentLog.end();
								}
							};
							if(cursor.hasNext()){
								cursor.next(fetchNext);
							}else{
								cursor.close();
								currentLog.write(']');
								currentLog.end();
							}
						});
					}, function(err){
						callback(err);
					});
				}
			], function(err){
				conn.close();
				if(err){
					log('error', err.message);
					if(err.error){
						log('error', err.error);
					}
				}else{
					log('Finished');
					process.exit();
				}
			});
		} // end if import
	}); // end rethinkdb connect
} // end function delcaration