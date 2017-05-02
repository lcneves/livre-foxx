/*
 * world/index.js
 * Copyright Lucas Neves <lcneves@gmail.com>
 * 
 * A set of functions to import places from GeoNames.org into ArangoDB and
 * explore the data for the Livre project
 */

'use strict';

const db = require('@arangodb').db;
const collections = [
  db._collection('adm5'),
  db._collection('adm4'),
  db._collection('adm3'),
  db._collection('adm2'),
  db._collection('adm1')
];

const traversal = require('@arangodb/graph/traversal');

const geoConfig = {
  datasource: traversal.generalGraphDatasourceFactory('world'),
  strategy: 'depthfirst',
  order: 'preorder',
  filter: traversal.visitAllFilter,
  expander: traversal.outboundExpander
}; 

const createRouter = require('@arangodb/foxx/router');
const router = createRouter();
const joi = require('joi');

module.context.use(router);

router.get('/geo/', function (req, res) {
  let lat = parseFloat(req.queryParams.lat);
  let lon = parseFloat(req.queryParams.lon);
  let radius = 20 * 1000; // 20 km

  let startVertex;

  while (!startVertex) {
    for (let i = 0, c = collections.length; i < c; ++i) {
      let nearest = collections[i].within(lat, lon, radius).limit(1).toArray();
      if (nearest[0]) {
        startVertex = nearest[0];
        break;
      } else {
        radius *= 2;
  }}}

  let result = {
    visited: {
      vertices: [ ],
      paths: [ ]
    }
  };

  let traverser = new traversal.Traverser(geoConfig);
  traverser.traverse(result, startVertex);

  res.json(result.visited.vertices);
})
.queryParam('lat', joi.number().min(-90).max(90).required(), 'Latitude')
.queryParam('lon', joi.number().min(-180).max(180).required(), 'Longitude')
.response(['application/json'], 'A list of places')
.summary('Returns nearest place to provided geolocation.')
.description('Returns nearest place to provided geolocation.');
