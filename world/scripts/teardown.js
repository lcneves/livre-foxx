'use strict';

const graph_module = require('@arangodb/general-graph');

graph_module._drop('world', true); // Also drops collections

