// `react` resolved to preact/compat, the same substitution the Vite build makes
// via `runtime: "react-compat"`. Node has no alias mechanism, so the harness
// installs this shim under the name `react`.
module.exports = require("preact/compat");
