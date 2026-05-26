// Market configurations — loaded from config/*.json files
// Maintains the same MARKET_CONFIGS[env] interface used by app.js
var MARKET_CONFIGS = {};

(function() {
  var ENVS = ['dev', 'staging', 'sandbox', 'prod'];

  ENVS.forEach(function(env) {
    fetch('./config/' + env + '.json')
      .then(function(res) {
        if (!res.ok) throw new Error(env + '.json: ' + res.status);
        return res.json();
      })
      .then(function(data) {
        MARKET_CONFIGS[env] = data;
      })
      .catch(function(err) {
        console.error('Failed to load config/' + env + '.json:', err.message);
        MARKET_CONFIGS[env] = [];
      });
  });
})();
