'use strict';

const db = require('@arangodb').db;
const graph_module = require('@arangodb/general-graph');
const vertices = [
  'world',
  'countries',
  'adm1',
  'adm2',
  'adm3',
  'adm4',
  'adm5'
];
const relations = [
  ['in',
  ['countries', 'adm1', 'adm2', 'adm3', 'adm4', 'adm5'],
  ['world', 'countries', 'adm1', 'adm2', 'adm3', 'adm4']
]];

/*
 * Let's try to load the graph 'world'. If it exists, we don't need to do
 * anything else. Otherwise, it will throw an exception, which we will
 * catch and create the graph.
 * Not the best control flow, but that will do until ArangoDB comes up with
 * a _graphExists function.
 */
try {
  var worldGraph = graph_module._graph('world'); 
} catch (e) {
  if (!worldGraph) {
    var graph = graph_module._create('world');

    vertices.forEach(function (vertexCollection) {
      graph._addVertexCollection(vertexCollection, true);
      let collection = db._collection(vertexCollection); 

      collection.ensureIndex({
        type: "hash",
        fields: [ "geonameId" ],
        unique: true
      });

      collection.ensureIndex({
        type: "fulltext",
        fields: [ "alternateNames" ],
        minLength: 1
      });

      collection.ensureIndex({
        type: "geo",
        fields: [ "geolocation.latitude", "geolocation.longitude" ]
      });
    });

    relations.forEach(function (relation) {
      let rel = graph_module._relation(...relation);
      graph._extendEdgeDefinitions(rel);
    });
  }
}


