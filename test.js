'use strict';
var BBPromise = require('bluebird'); // the most awesome promise lib
var github = require('octonode'); // wrapper for github api
var _ = require('lodash');
var parse = require('parse-link-header');
var mysql = require('mysql');
require('dotenv').config();

// promisify here instead of dynamically, a lot faster
BBPromise.promisifyAll(mysql);
BBPromise.promisifyAll(require('mysql/lib/Connection').prototype);
BBPromise.promisifyAll(require('mysql/lib/Pool').prototype);
BBPromise.promisifyAll(require('octonode/lib/octonode/repo').prototype, {multiArgs: true});
BBPromise.promisifyAll(require('octonode/lib/octonode/search').prototype);

var client = github.client(process.env.GITHUB_TOKEN);

function extractAllContributorsForRepo() {
  var repo = client.repo('drupal/drupal')
  return repo.contributorsAsync({page:1})
    .then(results => {
      if (results[1].link) {
        var links;
        var pages = [];
        links = parse(results[1].link);
        for (var i = 2; i <= links.last.page; i++) {pages.push({page:i})};
        return BBPromise.map(pages, page => { return repo.contributorsAsync(page); })
          .then(contributors => { return contributors[0][0].concat(results[0]); });
      } else {
        return [[results[0], null]];
      }
    })

    .map((contributors,index,stuff) => { return contributors[0]; })
    .reduce((arr1, arr2) => { return arr1.concat(arr2); }, [])
    .then(arr => { return { contributors: arr, repo: repo } })
    .then()
    .catch(e => {
      console.error(e.stack)
    });
}

extractAllContributorsForRepo();
