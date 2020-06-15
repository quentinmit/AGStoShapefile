const _ = require('lodash');

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

module.exports = generateQML;
