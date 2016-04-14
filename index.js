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
var ghSearch = client.search();

// setup connection to db
var pool = mysql.createPool({
  host     : process.env.DB_HOST,
  user     : process.env.DB_USER,
  password : process.env.DB_PASSWORD,
  port     : process.env.DB_PORT,
  database : process.env.DB_NAME,
});

// nifty wrap to clean up resources
function getSqlConnection() {
  return pool.getConnectionAsync().disposer(function(conn) {
      conn.release();
  });
}

// main promise pipeline
BBPromise.using(getSqlConnection(), getSqlConnection(), (conn1, conn2) => {
  return BBPromise.join(
    conn1.queryAsync('CREATE TABLE projects (name VARCHAR(256), description TEXT, id INTEGER UNSIGNED PRIMARY KEY, owner_id INTEGER UNSIGNED, homepage TEXT, watchers_cnt INTEGER UNSIGNED, forks_cnt INTEGER UNSIGNED, stargazers_cnt INTEGER UNSIGNED);'),
    conn2.queryAsync('CREATE TABLE project_contributors (login VARCHAR(256), id INTEGER UNSIGNED, project_id INTEGER UNSIGNED);')
  );
})
.then(() => {
  return ghSearch.reposAsync({
  q: 'drupal+in:description+language:php&per_page=100',
  per_page: 100
  })
  .then(repos => { return repos.items; })
  .map(repo => { return extractAllContributorsForRepo(repo) ;})
  .map(contributorsWithRepo => { return transformData(contributorsWithRepo); })
  .reduce((res1, res2) => {
    (res1.contributors || (res1.contributors = [])).push(res2.contributors);
    (res1.repos || (res1.repos = [])).push(res2.repo);
    return res1;
  }, {})
  .then(repos => {
    return BBPromise.join(
      insertIntoDB(_.flatten(repos.contributors), 'project_contributors', '`login`, `id`, `project_id`'),
      insertIntoDB(repos.repos, 'projects', '`name`, `description`, `id`, `owner_id`, `homepage`, `watchers_cnt`, `forks_cnt`, `stargazers_cnt`')
    );
  });
})
.finally(() => { return cleanUpAndIndexDB(); })
.catch(e => {console.log(e.stack)});

function insertIntoDB(doc, table, rows) {
  return BBPromise.using(getSqlConnection(), (conn) => {
    var escappedQuery = 'INSERT INTO ' + table + ' VALUES ' + mysql.escape(doc) + ';';
    return conn.queryAsync(escappedQuery);
  });
}

// add indexes for faster read speed, and faster joins
function cleanUpAndIndexDB() {
  return BBPromise.using(getSqlConnection(),
  getSqlConnection(), getSqlConnection(),
  getSqlConnection(), getSqlConnection(),
  (conn1, conn2, conn3, conn4, conn5) => {
    return BBPromise.join(
      conn1.queryAsync('ALTER TABLE project_contributors ADD INDEX join_pc_on_pj_idx(id, project_id);'),
      conn2.queryAsync('ALTER TABLE project_contributors ADD INDEX join_pj_on_pc_idx(project_id, id);'),
      conn3.queryAsync('ALTER TABLE project_contributors ADD INDEX project_id_idx(project_id);'),
      conn4.queryAsync('ALTER TABLE project_contributors ADD INDEX login_idx(login(256));'),
      conn5.queryAsync('ALTER TABLE projects ADD INDEX name_idx(name(256));')
    )
    .then(() =>{pool.end();})
  });
}

function extractAllContributorsForRepo(repo) {
  var repos = client.repo(repo.full_name);
  return repos.contributorsAsync({page:1})
    .then(results => {
      if (results[1].link) {
        var links;
        var pages = [];
        links = parse(results[1].link);
        for (var i = 1; i <= links.last.page; i++) {pages.push({page:i})};
        return BBPromise.map(pages, page => { return repos.contributorsAsync(page) });
      } else {
        return [[results[0], null]];
      }
    })
    .reduce((arr1, arr2) => { return arr1.concat(arr2[0]); }, [])
    .then(arr => { return { contributors: arr, repo: repo } });
}

function transformData(data) {
  return BBPromise.map(data.contributors, contributor => {
    return _.clone([ contributor.login, contributor.id, data.repo.id ]);
  })
  .then(contributors => {
    var transformedRepo = _.clone([
       data.repo.name,
       data.repo.description,
       data.repo.id,
       data.repo.owner.id,
       data.repo.homepage,
       data.repo.watchers_count,
       data.repo.forks_count,
       data.repo.stargazers_count
    ]);
    return {contributors: contributors, repo: transformedRepo};
  })
}
