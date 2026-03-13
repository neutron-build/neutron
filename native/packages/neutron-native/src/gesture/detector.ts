/**
 * GestureDetector — backward-compatible re-export from handlers.ts.
 *
 * This file previously contained a minimal GestureDetector stub. It now
 * delegates to handlers.ts which provides:
 *   1. RNGH native gesture recognition when react-native-gesture-handler is installed
 *   2. PanResponder-based fallback when RNGH is not available
 *
 * New code should import from '@neutron/native/gesture' directly.
 */

export { GestureDetector } from './handlers.js'
