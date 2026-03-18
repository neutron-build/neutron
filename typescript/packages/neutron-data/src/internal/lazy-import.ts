type DynamicImporter = (specifier: string) => Promise<unknown>;

const dynamicImport = new Function(
  "specifier",
  "return import(specifier);"
) as DynamicImporter;

export async function lazyImport<TModule>(
  specifier: string,
  installHint: string
): Promise<TModule> {
  try {
    return (await dynamicImport(specifier)) as TModule;
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    throw new Error(
      `Missing optional dependency "${specifier}". ${installHint}. Original error: ${reason}`
    );
  }
}

