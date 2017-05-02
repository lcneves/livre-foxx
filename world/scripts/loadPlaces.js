/*
 * This script will import data from files in the worldFiles directory.
 * All files are sourced from GeoNames.org. They are a list of places separated
 * by TAB.
 * The field indexes on each line are as follows:
 *
 * 0  geonameid         : integer id of record in geonames database
 * 1  name              : name of geographical point (utf8) varchar(200)
 * 2  asciiname         : name of geographical point in plain ascii characters, varchar(200)
 * 3  alternatenames    : alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
 * 4  latitude          : latitude in decimal degrees (wgs84)
 * 5  longitude         : longitude in decimal degrees (wgs84)
 * 6  feature class     : see http://www.geonames.org/export/codes.html, char(1)
 * 7  feature code      : see http://www.geonames.org/export/codes.html, varchar(10)
 * 8  country code      : ISO-3166 2-letter country code, 2 characters
 * 9  cc2               : alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters
 * 10 admin1 code       : fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
 * 11 admin2 code       : code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80) 
 * 12 admin3 code       : code for third level administrative division, varchar(20)
 * 13 admin4 code       : code for fourth level administrative division, varchar(20)
 * 14 population        : bigint (8 byte int) 
 * 15 elevation         : in meters, integer
 * 16 dem               : digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
 * 17 timezone          : the iana timezone id (see file timeZone.txt) varchar(40)
 * 18 modification date : date of last modification in yyyy-MM-dd format
 */

'use strict';

const db = require('@arangodb').db;
const graph_module = require('@arangodb/general-graph');
const fs = require('fs');

const path = fs.join(__dirname, '..', 'worldFiles');
const files = fs.list(path);
const logFile = fs.join(__dirname, '..', 'log.txt');

var graph = graph_module._graph('world');

// Create a 'world' vertex
graph.world.save({
  geonameId: 'world',
  name: 'World',
  alternateNames: ['World', 'world'],
  location: {latitude: 0.0, longitude: 0.0},
  population: 7500000000
});

// Load vertices (places) on the first iteration; edges on the second
for (let i = 0; i < 2; ++i) {
  files.forEach(function (file) {
    let contents = fs.read(fs.join(path, file));
    let places = contents.split("\n");
    
    places.forEach(function (place) {
      let fields = place.split("\t");
      if (fields.length != 19) // Malformed row
        return;

      let name = fields[1];
      let alternateNames = fields[3].split(',');

      // Ensure that there will be a name, provided there are alternate names
      if (!name && alternateNames[0])
        name = alternateNames[0];
      
      // Ensure that the name is part of the alternate names list
      // (which is fulltext-indexed)
      if(alternateNames.indexOf(name) === -1)
        alternateNames.push(name);

      let collection;
      let geonameId = fields[8] + '/';
      /* For some reason, the ID on the first column of the data file does not
       * match the references on columns 11 to 14. We will use the country code
       * for countries; for other administrative levels, we will use the path
       * from country to the final level.
       * Ex: a US county with reference number 12345 in Florida would get the
       * geonameId of US/FL/12345
       */
      switch (fields[7]) {
        case 'ADM1': 
          collection = 'adm1';
          geonameId += fields[10];
          break;

        case 'ADM2':
          collection = 'adm2';
          geonameId += fields[10] + '/' + fields[11];
          break;

        case 'ADM3':
          collection = 'adm3';
          geonameId += fields[10] + '/' + fields[11] + '/' + fields[12];
          break;

        case 'ADM4':
          collection = 'adm4';
          geonameId += fields[10] + '/' + fields[11] + '/' + fields[12]
            + '/' + fields[13];
          break;

        case 'ADM5':
          collection = 'adm5';
          geonameId += fields[10] + '/' + fields[11] + '/' + fields[12]
            + '/' + fields[13] + '/' + fields[0];
          break;

        case 'ADMD':
          if (fields[13] && fields[13] !== '0') {
            collection = 'adm5';
            geonameId += fields[10] + '/' + fields[11] + '/' + fields[12]
              + '/' + fields[13] + '/' + fields[0];
          }
          else if (fields[12] && fields[12] !== '0') {
            collection = 'adm4';
            geonameId += fields[10] + '/' + fields[11] + '/' + fields[12]
              + '/' + fields[0];
          }
          else if (fields[11] && fields[11] !== '0') {
            collection = 'adm3';
            geonameId += fields[10] + '/' + fields[11] + '/' + fields[0];
          }
          else if (fields[10] && fields[10] !== '0') {
            collection = 'adm2';
            geonameId += fields[10] + '/' + fields[0];
          }
          else {
            collection = 'adm1';
            geonameId += fields[0];
          }
          break;

        case 'PCL':
        case 'PCLD':
        case 'PCLF':
        case 'PCLI':
        case 'PCLIX':
        case 'PCLS':
          // We are admitting as countries political entities of
          // any status, even dependent ones.
          collection = 'countries';
          geonameId = fields[8];
          break;

        case 'TERR':
          /*
           * Since, for now, only American Samoa, Western Sahara and Svalbard
           * are territories that parent other places, let's put them in the
           * 'countries' collection for the sake of simplicity and ignore the
           * rest. In the future, we might create a 'territories' collection.
           */
          switch (fields[8]) {
            case 'AS': // American Samoa
            case 'EH': // Western Sahara
            case 'SJ': // Svalbard
              collection = 'countries';
              geonameId = fields[8];
              break;
            default:
              return; // Not interested in other territories
          }
          break;

        default:
          return; // We're not interested in places of other types.
      }

      if (i === 0) { // First pass, load vertices
        let vertex = {
          geonameId: geonameId,
          name: name,
          alternateNames: alternateNames,
          geolocation: {
            latitude: parseFloat(fields[4]),
            longitude: parseFloat(fields[5])
          },
          population: parseInt(fields[14])
        };

        try {
          graph[collection].save(vertex);
        } catch (e) {
          fs.append(logFile, 'Unable to save vertex! Reason: ' + e + "\n");
          fs.append(logFile, JSON.stringify(vertex) + "\n"); 
          console.error('Unable to save vertex! Reason: ' + e);
          console.dir(vertex);
        }

      } else { // Second pass, load edges
        let fromVertex;
        let toVertex;
        let toSearchTerms;

        switch (collection) {
          case 'adm1':
            toSearchTerms = ['countries', fields[8]];
            break;

          case 'adm2':
            toSearchTerms = ['adm1', fields[8] + '/' + fields[10]];
            break;

          case 'adm3':
            toSearchTerms = ['adm2', fields[8] + '/' + fields[10] + '/'
              + fields[11]];
            break;

          case 'adm4':
            toSearchTerms = ['adm3', fields[8] + '/' + fields[10] + '/'
              + fields[11] + '/' + fields[12]];
            break;

          case 'adm5':
            toSearchTerms = ['adm4', fields[8] + '/' + fields[10] + '/'
              + fields[11] + '/' + fields[12] + '/' + fields[13]];
            break;

          case 'countries':
            toSearchTerms = ['world', 'world'];
            break;
        }

        try {
          fromVertex = db._collection(collection).firstExample(
              { 'geonameId': geonameId })['_id'];
        } catch (e) {
          handleSearchError(collection, geonameId, e);
        }

        try {
          toVertex = searchToVertex(
              toSearchTerms[0], toSearchTerms[1], fields[9]
              )['_id'];
        } catch (e) {
          handleSearchError(toSearchTerms[0], toSearchTerms[1], e);
        }

        try {
          graph.in.save(fromVertex, toVertex, {});
        } catch (e) {
          fs.append(logFile, 'ERR: Could not save edge from ' + fromVertex
              + ' to ' + toVertex + "\n");
          fs.append(logFile, 'ERR: Error message was: ' + e + "\n");
          console.error('Could not save edge from ' + fromVertex
              + ' to ' + toVertex);
          console.error('Error message was: ' + e);
        }
      }
    });
  });
}

function handleSearchError (collection, geonameId, error) {
  fs.append(logFile, 'ERR: Could not find vertex with geonameId ' + geonameId
      + ' in collection ' + collection + '.' + "\n");
  fs.append(logFile, 'ERR: Error message was: ' + error + "\n");
  console.error('ERR: Could not find vertex with geonameId ' + geonameId
      + ' in collection ' + collection + '.');
  console.error('ERR: Error message was: ' + error);
};

function searchToVertex (collection, geonameId, alternateCountry) {
  let vertexDocument = db._collection(collection).firstExample(
      {'geonameId': geonameId}
      );
  if (!vertexDocument) { // If the administrative region is not found, let's try
                         // to attach the city to that region's parent.
    let parentDocument;
    let parentCollection;

    if (collection === 'country' && alternateCountry) {
      parentCollection = collection;
      parentDocument = alternateCountry;
    }
    else {
      parentDocument = geonameId.substring(0, geonameId.lastIndexOf('/'));
      switch (collection) {
        case 'adm1':
          parentCollection = 'countries';
          break;
        case 'adm2':
          parentCollection = 'adm1';
          break;
        case 'adm3':
          parentCollection = 'adm2';
          break;
        case 'adm4':
          parentCollection = 'adm3';
          break;
        default:
          throw new Error(
              'searchToVertex function could not identify parent collection!'
              );
          break;
      }
    }
    vertexDocument = searchToVertex(parentCollection, parentDocument);
  }
  return vertexDocument;
};
