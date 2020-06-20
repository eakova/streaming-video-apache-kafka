"use strict";

const glob = require("glob");
const path = require("path");
const _ = require("lodash");

// add ping route by default for health check
const routes = [
  {
    method: "GET",
    path: "/ping",
    handler: function (request, reply) {
      return "pong";
    },
    config: {
      tags: ["api"],
      auth: false,
    },
  },
];

// add all routes from all modules to the routes array manually or write your routes inside a folder inside the server folder
// with suffix as Routes.js e.g airlinesRoutes.js
glob.sync("./routes/**/*Route.js").forEach((file) => {
  routes.push(require(path.resolve(file)));
});

// export routes
module.exports = _.flattenDeep(routes);
