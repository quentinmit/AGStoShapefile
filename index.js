#!/usr/bin/env node

// @Author: Joshua Tanner
// @Date: 1/15/2018
// @Description: Easy way to convert ArcGIS Server service to GeoJSON
//               and shapefile format.  Good for backup solution.
// @services.txt format :: serviceLayerURL|layerName|throttle(ms)
// @githubURL : https://github.com/tannerjt/AGStoShapefile

// Node Modules
const fs = require('fs');
const rp = require('request-promise');
const Promise = require('bluebird');
const request = require('request');
const path = require('path');
const _ = require('lodash');
const TerraformerArcGIS = require('terraformer-arcgis-parser');
const geojsonStream = require('geojson-stream');
const JSONStream = require('JSONStream');
const CombinedStream = require('combined-stream');
const streamToPromise = require('stream-to-promise');
const queryString = require('query-string');
const merge2 = require('merge2');
const rimraf = require('rimraf');
const ogr2ogr = require('ogr2ogr');
const xmljs = require('xml-js');
// ./mixin.js
// merge user query params with default
const mixin = require('./mixin');
var program = require('commander');

program
	.version('1.0.2')
	.option('-o, --outdir [directory]', 'Output directory', './output/')
	.option('-s, --services [path to txt file]', 'Text file containing service list to extract', 'services.txt')
	.option('--server [url]', 'Server to fetch all layers from')
	.option('-P, --parallelism [concurrency]', 'Number of services to download in parallel', value => parseInt(value), Infinity)
	.option('-t, --throttle [milliseconds', 'Delay between requests for the same layer', value => parseInt(value), 0)
	.option('-S, --shapefile', 'Optional export to shapefile')
	.parse(process.argv);

var outDir = program.outdir || './output/';
// Remove trailing '/'
outDir = outDir.replace(/\/$/, '');

if (program.server) {
	let layers = listLayers(program.server);
	Promise.map(layers, layer => {
		return fetchOneService(layer.url, layer.name, layer.throttle);
	});
} else {
	fs.readFile(program.services, function (err, data) {
		if (err) {
			throw err;
		}

		Promise.map(data.toString().split('\n'), (service) => {
			var service = service.split('|');
			if(service[0].split('').length == 0) return;
			const serviceUrl = service[0].trim();
			const serviceName = service[1].trim();
			let throttle = program.throttle;
			if(service.length > 2) {
				throttle = +service[2];
			}

			return fetchOneService(serviceUrl, serviceName, throttle);
		}, {concurrency: program.parallelism})
	});
}

function listLayers(serverUrl) {
	var baseUrl = getBaseUrl(serverUrl);
	return rp({
		url: baseUrl + '?f=json',
		method: 'GET',
		json: true,
	})
		.then(meta => {
			const layers = _.mapKeys(meta.layers, 'id');

			return _.map(meta.layers, layer => {
				let name = layer.name;
				for (parent = layer.parentLayerId; parent >= 0; parent = layers[parent].parentLayerId) {
					name = layers[parent].name + '/' + name;
				}
				return {url: baseUrl+'/'+layer.id, name: name, throttle: program.throttle};
			});
		});
}

function fetchOneService(serviceUrl, serviceName, throttle) {
	var baseUrl = getBaseUrl(serviceUrl);
	var reqQS = {
		where: '1=1',
		returnIdsOnly: true,
		f: 'json'
	};
	var userQS = getUrlVars(serviceUrl);
	// mix one obj with another
	var qs = mixin(userQS, reqQS);
	qs = queryString.stringify(qs);
	var url = decodeURIComponent(getBaseUrl(baseUrl) + '/query/?' + qs);

	return Promise.join(
		rp({
			url : url,
			method : 'GET',
			json : true
		}),
		rp({
			url: baseUrl+'?f=json',
			method: 'GET',
			json: true,
		}),
		(body, meta) => requestService(serviceUrl, serviceName, body, meta, throttle))
		.catch((err) => {
			console.log("Failed to fetch service", serviceUrl, err);
		});
}

// Resquest JSON from AGS
function requestService(serviceUrl, serviceName, body, meta, throttle) {
	if (body.error) {
		return Promise.reject(body.error.message);
	}

	let objectIds = body.objectIds;
	objectIds.sort();
	const requests = Math.ceil(objectIds.length / 100);
	console.log(`Number of features for service ${serviceName}:`, objectIds.length);
	console.log(`Getting chunks of 100 features, will make ${requests} total requests`);

	const serviceNameShort = path.basename(serviceName);

	const supportsGeoJSON = meta.supportedQueryFormats.split(", ").indexOf("geoJSON") >= 0;

	const partialsDir = `${outDir}/${serviceName}/partials`;

	fs.mkdirSync(`${outDir}/${serviceName}`, {recursive: true});

	if (!fs.existsSync(partialsDir)){
		fs.mkdirSync(partialsDir);
	} else {
		rimraf.sync(partialsDir);
		fs.mkdirSync(partialsDir);
	}

	const filePrefix = `${outDir}/${serviceName}/${serviceNameShort}_${Date.now()}`;

	fs.writeFileSync(`${filePrefix}.xml`, xmljs.js2xml(convertMetadata(meta), {compact: true}));
	fs.writeFileSync(`${filePrefix}.qml`, xmljs.js2xml(generateQML(meta), {compact: true}));

	function allRequests(supportsGeoJSON) {
		let parts = [];

		for(let i = 0; i < Math.ceil(objectIds.length / 100); i++) {
			var ids = [];
			if ( ((i + 1) * 100) < objectIds.length ) {
				ids = objectIds.slice(i * 100, (i * 100) + 100);
			} else {
				ids = objectIds.slice(i * 100, objectIds[objectIds.length]);
			}

			// we need these query params
			const reqQS = {
				objectIds : ids.join(','),
				geometryType : 'esriGeometryEnvelope',
				returnGeometry : true,
				returnIdsOnly : false,
				outFields : '*',
				outSR : '4326',
				f : 'json'
			};
			// user provided query params
			const userQS = getUrlVars(serviceUrl);
			// mix one obj with another
			var qs = mixin(userQS, reqQS);

			if (supportsGeoJSON) {
				qs.f = 'geoJSON';
			}

			qs = queryString.stringify(qs);
			const url = decodeURIComponent(getBaseUrl(serviceUrl) + '/query/?' + qs);

			const options = {
				url: url,
				method: 'GET',
				json: true,
			};

			parts.push({
				options: options,
				path: `${partialsDir}/${i}.json`,
				supportsGeoJSON: supportsGeoJSON,
			});
		}

		return Promise.mapSeries(
			parts,
			oneRequest);
	}
	let p = allRequests(supportsGeoJSON);
	if (supportsGeoJSON) {
		p = p.catch(e => {
			console.log("Failed to fetch geoJSON for "+serviceName+"; falling back to ESRI JSON", e);
			return allRequests(false);
		})
	}
	return p.then(mergeFiles);

	function oneRequest(part, index) {
		return Promise.delay(index ? throttle : 0, part.options)
			.then(rp)
			.then((resp) => {
				if (resp.error) {
					throw new Error(resp.error.message);
				}
				if (part.supportsGeoJSON) {
					return resp.features;
				} else {
					return _.flatMap(resp.features, convert);
				}
			})
			.then((features) => {
				const out = geojsonStream.stringify()
				const p = streamToPromise(out
							  .pipe(fs.createWriteStream(part.path))
							 );
				features.forEach((feature) => out.write(feature));
				out.end();
				return p;
			})
			.then(() => {
				console.log(`Completed ${index+1} of ${requests} requests for ${serviceName}`);
				return part.path;
			})
			.catch(e => {
				console.log("Failed request", part.url, e);
				throw e;
			});
	}

	function convert (feature) {
		if(!feature.geometry) {
			console.log("Feature Missing Geometry and is Omitted: ", JSON.stringify(feature))
			return [];
		}
		const gj = {
			type: 'Feature',
			properties: feature.attributes,
			geometry: TerraformerArcGIS.parse(feature.geometry)
		}
		return [gj];
	}

	function mergeFiles(files) {
		console.log(`Finished extracting chunks for ${serviceName}, merging files...`)
		const finalFilePath = `${filePrefix}.geojson`
		const finalFile = fs.createWriteStream(finalFilePath);

		let streams = CombinedStream.create();
		_.each(files, (file) => {
			streams.append((next) => {
				next(
					fs.createReadStream(file)
						.pipe(JSONStream.parse('features.*'))
						.on('error', (err) => {
							console.log(err);
						})
				);
			})
		});

		return streamToPromise(
			streams
				.pipe(geojsonStream.stringify())
				.pipe(finalFile)
		).then(() => {
			rimraf(partialsDir, () => {
				console.log(`${serviceName} is complete`);
				console.log(`File Location: ${finalFilePath}`);
				if(program.shapefile) {
					makeShape(finalFilePath);
				}
			});
		});
	}

	function makeShape(geojsonPath) {
		console.log(`Generating shapefile for ${serviceName}`)
		// todo: make optional with flag
		const shpPath = `${filePrefix}.zip`;
		const shpFile = fs.createWriteStream(shpPath);
		var shapefile = ogr2ogr(geojsonPath)
		    .format('ESRI Shapefile')
		    .options(['-nln', serviceName])
		    .timeout(120000)
		    .skipfailures()
		    .stream();
		shapefile.pipe(shpFile);
	}
}

function convertMetadata(meta) {
	// Convert JSON-format metadata to FGDC XML metadata
	let out = {
		metadata: {
			idinfo: {
				citation: {
					citeinfo: {
						title: meta.name,
						geoform: "vector digital data",
					},
				},
				descript: {
					abstract: meta.name,
					supplinf: meta.description,
				},
			},
			eainfo: {
				detailed: {
					enttyp: {
						enttypl: meta.name,
						enttypd: meta.description,
					},
					attr: [
					],
				},
			},
		},
	};
	out.metadata.eainfo.detailed.attr = meta.fields.map(function(field) {
		let out = {
			attrlabl: field.name,
			attrdef: field.alias,
		};
		if (field.domain) {
			if (field.domain.type == 'codedValue') {
				out.attrdomv = {
					edom: field.domain.codedValues.map(function(value) {
						return {
							edomv: value.code,
							edomvd: value.name,
						};
					}),
				};
			}
		}
		return out;
	});
	return out;
}

function parseEsriLineStyle(style) {
	return {
		esriSLSSolid: 'solid',
		esriSLSDash: 'dash',
		esriSLSDashDot: 'dash dot',
		esriSLSDashDotDot: 'dash dot dot',
		esriSLSDot: 'dot',
		esriSLSNull: 'no',
	}[style] || 'solid';
}

function parseEsriMarkerShape(style) {
	return {
		esriSMSCircle: 'circle',
		esriSMSCross: 'cross',
		esriSMSDiamond: 'diamond',
		esriSMSSquare: 'square',
		esriSMSX: 'cross2',
		esriSMSTriangle: 'triangle',
	}[style] || 'circle';
}

function parseEsriFillStyle(style) {
	return {
		esriSFSBackwardDiagonal: 'b_diagonal',
		esriSFSCross: 'cross',
		esriSFSDiagonalCross: 'diagonal_x',
		esriSFSForwardDiagonal: 'f_diagonal',
		esriSFSHorizontal: 'horizontal',
		esriSFSNull: 'no',
		esriSFSSolid: 'solid',
		esriSFSVertical: 'vertical',
	}[style] || 'solid';
}

function parseEsriSymbol(symbol) {
	if (symbol.type == "esriSMS") {
		// marker
		const out = {
			'_attributes': {
				type: 'marker',
			},
			'layer': {
				'_attributes': {
					'class': 'SimpleMarker',
				},
				'prop': [
					{'_attributes': {k: 'color', v: symbol.color.join(',')}},
					{'_attributes': {k: 'size', v: symbol.size}},
					{'_attributes': {k: 'size_unit', v: 'Point'}},
					{'_attributes': {k: 'angle', v: -symbol.angle}},
					{'_attributes': {k: 'offset', v: [symbol.xoffset,symbol.yoffset].join(',')}},
					{'_attributes': {k: 'offset_unit', v: 'Point'}},
					{'_attributes': {k: 'name', v: parseEsriMarkerShape(symbol.style)}},
				],
			},
		};
		if (symbol.outline) {
			out.layer.prop.push(
				{'_attributes': {k: 'outline_color', v: symbol.outline.color.join(',')}},
				{'_attributes': {k: 'outline_width', v: symbol.outline.width}},
				{'_attributes': {k: 'outline_width_unit', v: 'Point'}},
				{'_attributes': {k: 'outline_style', v: parseEsriLineStyle(symbol.outline.style)}},
			);
		}
		return out;
	} else if (symbol.type == "esriSLS") {
		// line
		return {
			'_attributes': {
				type: 'line',
			},
			'layer': {
				'_attributes': {
					'class': 'SimpleLine',
				},
				'prop': [
					{'_attributes': {k: 'line_color', v: symbol.color.join(',')}},
					{'_attributes': {k: 'line_width', v: symbol.width}},
					{'_attributes': {k: 'line_width_unit', v: 'Point'}},
					{'_attributes': {k: 'line_style', v: parseEsriLineStyle(symbol.style)}},
				],
			},
		};
	} else if (symbol.type == "esriSFS") {
		// fill
		const out = {
			'_attributes': {
				type: 'fill',
			},
			'layer': {
				'_attributes': {
					'class': 'SimpleFill',
				},
				'prop': [
					{'_attributes': {k: 'color', v: symbol.color.join(',')}},
					{'_attributes': {k: 'fill_style', v: parseEsriFillStyle(symbol.style)}},
				],
			},
		};
		if (symbol.outline) {
			out.layer.prop.push(
				{'_attributes': {k: 'outline_color', v: symbol.outline.color.join(',')}},
				{'_attributes': {k: 'outline_width', v: symbol.outline.width}},
				{'_attributes': {k: 'outline_width_unit', v: 'Point'}},
				{'_attributes': {k: 'outline_style', v: parseEsriLineStyle(symbol.outline.style)}},
			);
		}
		return out;
	} else if (symbol.type == "esriPFS") {
		// picture fill
	} else if (symbol.type == "esriPMS") {
		// picture marker
		return {
			'_attributes': {
				type: 'marker',
			},
			'layer': {
				'_attributes': {
					'class': 'RasterMarker',
				},
				'prop': [
					{'_attributes': {k: 'offset', v: [symbol.xoffset,symbol.yoffset].join(',')}},
					{'_attributes': {k: 'offset_unit', v: 'Point'}},
					{'_attributes': {k: 'size', v: symbol.width}},
					{'_attributes': {k: 'size_unit', v: 'Point'}},
					{'_attributes': {k: 'angle', v: -symbol.angle}},
					{'_attributes': {k: 'imageFile', v: `base64:${symbol.imageData}`}},
				],
			},
		};
	} else if (symbol.type == "esriTS") {
		// text
	}
}

function toQGISOption(data) {
	if (typeof data === 'string') {
		return {
			'_attributes': {
				type: 'QString',
				value: data,
			},
		};
	} else if (typeof data === 'boolean') {
		return {
			'_attributes': {
				type: 'bool',
				value: data ? 'true' : 'false',
			},
		};
	} else if (typeof data === 'number') {
		return {
			'_attributes': {
				type: 'int',
				value: data,
			},
		};
	} else if (Array.isArray(data)) {
		return {
			'_attributes': {
				type: 'List',
			},
			Option: _.map(data, toQGISOption),
		};
	} else if (typeof data === 'object') {
		return {
			'_attributes': {
				type: 'Map',
			},
			Option: _.map(data, (value, key) => _.merge(toQGISOption(value), {'_attributes': {'name': key}})),
		};
	};
}

function generateQML(meta) {
	// Convert JSON-format metadata to QGIS QML style file
	let out = {
		"_doctype": "qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'",
		qgis: {
			"_attributes": {
				labelsEnabled: meta.hasLabels,
				version: "3",
			},
			"renderer-v2": {
				"_attributes": {
				}
			},
			"fieldConfiguration": {},
			"aliases": {},
		}
	};

	if (meta.drawingInfo && meta.drawingInfo.renderer) {
		const renderer = meta.drawingInfo.renderer;

		if (renderer.type == "simple") {
			out.qgis['renderer-v2'] = {
				'_attributes': {
					type: "singleSymbol",
				},
				symbols: {
					symbol: _.merge(parseEsriSymbol(renderer.symbol), {'_attributes': {name: '0'}}),
				},
			};
		} else if (renderer.type == "uniqueValue") {
			let attr = renderer.field1;
			if (renderer.field3) {
				attr = `concat("${renderer.field1}",',',"${renderer.field2}",',',"${renderer.field3}")`;
			} else if (renderer.field2) {
				attr = `concat("${renderer.field1}",',',"${renderer.field2}")`;
			}
			let symbols = [];
			let categories = [];
			_.forEach(renderer.uniqueValueInfos, category => {
				const symbol = parseEsriSymbol(category.symbol);
				if (symbol) {
					const name = `${symbols.length}`;
					symbol['_attributes'].name = name;
					categories.push({
						'_attributes': {
							value: category.value,
							label: category.label,
							symbol: name,
						},
					});
					symbols.push(symbol);
				};
			});
			out.qgis['renderer-v2'] = {
				'_attributes': {
					type: "categorizedSymbol",
					attr: attr,
				},
				symbols: {
					symbol: symbols,
				},
				categories: {
					category: categories,
				},
			};
		}
	}
	if (meta.type == "Annotation Layer") {
		out.qgis['_attributes'].labelsEnabled = '1';
		out.qgis['renderer-v2'] = {
			'_attributes': {
				type: 'nullSymbol',
			},
		};
		out.qgis.labeling = {
			'_attributes': {
				type: 'simple',
			},
			settings: {
				'text-style': {
					'_attributes': {
						fieldName: "regexp_replace(TextString, '&lt;[^>]+>','')",
						isExpression: '1',
					},
				},
				placement: {
					'_attributes': {
						placement: '1',
					},
				},
				dd_properties: {
					'Option': toQGISOption({
						name: '',
						properties: {
							Bold: {
								active: true,
								field: 'Bold',
								type: 2,
							},
							Italic: {
								active: true,
								field: 'Italic',
								type: 2,
							},
							Underline: {
								active: true,
								field: 'Underline',
								type: 2,
							},
							Color: {
								active: true,
								expression: "color_rgb(to_real(coalesce(regexp_substr( TextString, 'red=''([^'']+)'''),0)&#x9;||'.0'),to_real(coalesce(regexp_substr( TextString, 'green=''([^'']+)'''),0)||'.0'),to_real(coalesce(regexp_substr( TextString, 'blue=''([^'']+)'''),0)||'.0'))",
								type: 3,
							},
							Family: {
								active: true,
								field: 'FontName',
								type: 2,
							},
							LabelRotation: {
								active: true,
								expression: '-"Angle"',
								type: 3,
							},
							OffsetXY: {
								active: true,
								expression: 'array(XOffset,YOffset)',
								type: 3,
							},
							Size: {
								active: true,
								field: 'FontSize',
								type: 2,
							},
						},
						type: 'collection',
					}),
				},
			},
		};
	}

	out.qgis.fieldConfiguration.field = meta.fields.map(function(field) {
		let out = {
			'_attributes': {
				name: field.name,
			}
		};
		if (field.domain) {
			if (field.domain.type == 'codedValue') {
				out.editWidget = {
					'_attributes': {
						type: 'ValueMap',
					},
					config: {
						Option: toQGISOption({
							map: field.domain.codedValues.map(
								value => {
									let ret = {};
									ret[value.name] = value.code;
									return ret
								}),
						}),
					},
				};
			}
		}
		return out;
	});

	out.qgis.aliases.alias = _.map(meta.fields, (field, index) => {
		return {
			'_attributes': {
				'field': field.name,
				'name': field.alias,
				'index': index,
			},
		};
	});

	return out;
}


//http://stackoverflow.com/questions/4656843/jquery-get-querystring-from-url
function getUrlVars(url) {
    var vars = {}, hash;
    var hashes = url.slice(url.indexOf('?') + 1).split('&');
    for(var i = 0; i < hashes.length; i++)
    {
        hash = hashes[i].split('=');
        vars[hash[0].toString()] = hash[1];
    }
    return vars;
}

// get base url for query
function getBaseUrl(url) {
	// remove any query params
	var url = url.split("?")[0];
	if((/\/$/ig).test(url)) {
		url = url.substring(0, url.length - 1);
	}
	return url;
}
