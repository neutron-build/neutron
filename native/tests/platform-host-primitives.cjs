// Host-only RN primitives. React hooks, scheduling and DOM lifecycle stay real.
const React = require('react');
const primitive = tag => React.forwardRef(function HostPrimitive({ children, onPress, ...props }, ref) {
  delete props.accessible; delete props.accessibilityRole; delete props.accessibilityLabel;
  return React.createElement(tag, { ...props, ref, onClick: onPress }, children);
});
module.exports = { TurboModuleRegistry: { getEnforcing: name => { if (!global.__platformHostModules?.[name]) throw new Error('Missing public RN module: ' + name); return global.__platformHostModules[name]; } }, View: primitive('div'), Text: primitive('span'), Pressable: primitive('button'),
  Animated: { Value: class Value {} }, Easing: {}, Platform: { OS: 'host' } };
