'use strict';
var Promise = require('bluebird'); // the most awesome promise lib
var github = require('octonode'); // wrapper for github api
var _ = require('lodash');
var parse = require('parse-link-header');
var mysql = require('mysql');
require('dotenv').config();

var pool = Promise.promisifyAll(mysql.createPool({
	host     : process.env.DB_HOST,
	user     : process.env.DB_USER,
	password : process.env.DB_PASSWORD,
	port     : process.env.DB_PORT,
	database : process.env.DB_NAME
}));
var client = github.client(process.env.GITHUB_TOKEN);
var ghSearch =  Promise.promisifyAll(client.search());

// this is here to shut down the program,
// will need to find a better to this
var signal = 2;

var howMany = 0;
ghSearch.reposAsync({
  q: 'drupal+in:description+language:php&per_page=100',
  per_page: 100
}).then(res => {
	// spawn off workers to insert the repos into the db, continue on with contributors
	transformRepos(res.items);
	return res.items;
}).map(repo => {
	var ghRepo = Promise.promisifyAll(client.repo(repo.full_name), {multiArgs: true});
	return getAllPagesOfContributors(ghRepo, repo);
	// calling lodash flatten here, bluebird promises can very niftily wrap lodash functions
}).then(_).call('flatten').all().map(contributorArr => {
	var repoId = contributorArr.repoId
	return Promise.map(contributorArr.contributors, (contributor) => {
		var transformedContributor = _.clone({
			login: contributor.login,
			id: contributor.id,
			project_id: repoId
		});
		return insertIntoDB(transformedContributor, 'project_contributors');
	});
}).finally(()=> {
	signal--;
	// dirty, need this to terminate the program
	if(signal ==0) cleanUpAndIndexDB();
}).catch(e => {
	console.error('Error in Pipeline:', e.stack);
});

// add indexes for faster read speed, sequential index from id to project_id for joins from
// projects_contributors on projects, and an index on project_id, so you can lookup contributors
// project quickly, projects was created with a primary key on id so that should make this indexes
// work as described
function cleanUpAndIndexDB() {
	pool.getConnectionAsync().then(conn => {
		conn.query('CREATE INDEX join_index on project_contributors (id, project_id);', () => {
			conn.query('CREATE INDEX contributor_lookup_index on project_contributors (project_id);', () => {
					conn.release();
					pool.end();
			});
		});
	});
}

function getAllPagesOfContributors(ghRepo, repo) {
	return ghRepo.contributorsAsync().then(results => {
		var links;
		var pages = [1]
		if (results[1].link) {
			links = parse(results[1].link);
			for (var i = 2; i <= links.last.page; i++) {pages.push(i)};
		}
		return Promise.map(pages, i => {
			return ghRepo.contributorsAsync({page: i}).then(results => {
				return {contributors: results[0], repoId: repo.id}
			});
		});
	});
}

function transformRepos(repos) {
	return Promise.map(repos, repo => {
		var transformedRepo = _.clone({
			name: repo.name,
			description: repo.description,
			id: repo.id,
			owner_id: repo.owner.id,
			homepage: repo.homepage,
			watchers_cnt: repo.watchers_count,
			forks_cnt: repo.forks_count,
			stargazers_cnt: repo.stargazers_count
		});
			return insertIntoDB(transformedRepo, 'projects');
	}).finally(()=> {
		signal--;
		// dirty, need this to terminate the program
		if(signal ==0) cleanUpAndIndexDB();
	}).catch(e => {
		console.error('Error in Pipeline:', e.stack);
	});;
}

function insertIntoDB(doc, table) {
	return pool.getConnectionAsync()
		.then(conn => {
			// need to figure out how to do this a head of time, for later
			var con = Promise.promisifyAll(conn)
			return con.queryAsync('INSERT INTO ' + table + ' SET ?;', doc).then(() => {
				conn.release();
			});
		});
}
