#!/usr/bin/node

/*
 * makeWorldPlaces.js
 *
 * Copyright 2017 Lucas Neves <lcneves@gmail.com>
 *
 * This script will look for an 'allCountries.txt' file in the local directory
 * and break it into 100k line files in the worldFiles directory.
 * Since we are at it, it will also filter out unnecessary lines.
 *
 * CAUTION: This script will empty the directory 'worldFiles'!
 *
 * The reason for this breaking is that Foxx's implementation of 'fs' only reads full
 * files, and this takes up too much memory.
 *
 * the file allCountries.txt is sourced from GeoNames.org.
 * It is a list of places separated by TAB.
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

const fs = require('fs');
const path = require('path');
const readline = require('readline');
const stream = require('stream');

const pathToSourceFile = path.join(__dirname, 'allCountries.txt');
const pathToOutputDirectory = path.join(__dirname, 'worldFiles');
const LINES_PER_FILE = 100000;

// Adapted from http://stackoverflow.com/questions/18052762/remove-directory-which-is-not-empty
var deleteDirRecursive = function(path) {
  fs.readdirSync(path).forEach(function(file,index){
    var curPath = path + "/" + file;
    if(fs.lstatSync(curPath).isDirectory()) { // recurse
      deleteDirRecursive(curPath);
    } else { // delete file
      fs.unlinkSync(curPath);
    }
  });
  fs.rmdirSync(path);
};

if (!fs.existsSync(pathToSourceFile)) {
  throw new Error('File ' + pathToSourceFile + ' does not exist!');
  return 1;
}

if (fs.existsSync(pathToOutputDirectory)) {
  try {
    deleteDirRecursive(pathToOutputDirectory);
  } catch (e) {
    console.error('Unable to clear directory ' + pathToOutputDirectory);
    console.error(e);
    return 2;
  }
} 

try {
  fs.mkdirSync(pathToOutputDirectory);
} catch (e) {
  console.error('Unable to create directory ' + pathToOutputDirectory);
  return 3;
}

// If we got here, let's assume that the worldFiles directory exists and is empty

// Adapted from https://coderwall.com/p/ohjerg/read-large-text-files-in-nodejs
var instream = fs.createReadStream(pathToSourceFile);
var outstream = new stream;
var rl = readline.createInterface(instream, outstream);
var lineCounter = 0;

rl.on('line', function(line) {
  let fields = line.split("\t");
  if (fields.length < 19) // Malformed line or EOF
    return;
  if (!(
      fields[7] === 'TERR' ||
      fields[7] === 'ADM1' ||
      fields[7] === 'ADM2' ||
      fields[7] === 'ADM3' ||
      fields[7] === 'ADM4' ||
      fields[7] === 'ADM5' ||
      fields[7] === 'ADMD' ||
      (
       fields[7] !== 'PCLH' &&
       fields[7].indexOf('PCL') === 0
      )
      ))
    return;

  // Line must be valid and of interest!
  let fileNumber = parseInt(lineCounter++ / LINES_PER_FILE);
  let fileName = '000' + fileNumber;
  fileName = 'world' + fileName.slice(-4).concat('.txt');
  fileName = path.join(pathToOutputDirectory, fileName);
  fs.appendFileSync(fileName, line.concat("\n"));
});

return 0;

