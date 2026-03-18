/**
 * Build Adapters API with Hooks
 * Inspired by Next.js 16 build adapters
 *
 * Allows adapters to modify config and process build output
 */

import type { NeutronConfig } from '../config.js';
import type { Route } from '../core/types.js';

export interface BuildContext {
  /**
   * All routes discovered during build
   */
  routes: Route[];

  /**
   * Generated assets
   */
  assets: {
    js: string[];
    css: string[];
    sourcemaps?: string[];
    other: string[];
  };

  /**
   * Output directory
   */
  outputDir: string;

  /**
   * Build start time
   */
  buildStartTime: number;

  /**
   * Build end time
   */
  buildEndTime: number;
}

export interface NeutronAdapterWithHooks {
  /**
   * Adapter name
   */
  name: string;

  /**
   * Modify Neutron config during build
   * Called before build starts
   */
  modifyConfig?: (config: NeutronConfig) => NeutronConfig | Promise<NeutronConfig>;

  /**
   * Hook called when build starts
   */
  onBuildStart?: (config: NeutronConfig) => void | Promise<void>;

  /**
   * Hook called when build completes successfully
   */
  onBuildComplete?: (context: BuildContext) => void | Promise<void>;

  /**
   * Hook called when build fails
   */
  onBuildError?: (error: Error, context: Partial<BuildContext>) => void | Promise<void>;

  /**
   * Existing deployment hook
   */
  deploy?: (context: any) => Promise<void>;
}

/**
 * Runs all modifyConfig hooks from adapters
 */
export async function runModifyConfigHooks(
  config: NeutronConfig,
  adapters: NeutronAdapterWithHooks[]
): Promise<NeutronConfig> {
  let modifiedConfig = config;

  for (const adapter of adapters) {
    if (adapter.modifyConfig) {
      modifiedConfig = await adapter.modifyConfig(modifiedConfig);
    }
  }

  return modifiedConfig;
}

/**
 * Runs all onBuildStart hooks from adapters
 */
export async function runBuildStartHooks(
  config: NeutronConfig,
  adapters: NeutronAdapterWithHooks[]
): Promise<void> {
  await Promise.all(
    adapters.map(adapter =>
      adapter.onBuildStart ? adapter.onBuildStart(config) : Promise.resolve()
    )
  );
}

/**
 * Runs all onBuildComplete hooks from adapters
 */
export async function runBuildCompleteHooks(
  context: BuildContext,
  adapters: NeutronAdapterWithHooks[]
): Promise<void> {
  await Promise.all(
    adapters.map(adapter =>
      adapter.onBuildComplete ? adapter.onBuildComplete(context) : Promise.resolve()
    )
  );
}

/**
 * Runs all onBuildError hooks from adapters
 */
export async function runBuildErrorHooks(
  error: Error,
  context: Partial<BuildContext>,
  adapters: NeutronAdapterWithHooks[]
): Promise<void> {
  await Promise.all(
    adapters.map(adapter =>
      adapter.onBuildError ? adapter.onBuildError(error, context) : Promise.resolve()
    )
  );
}
