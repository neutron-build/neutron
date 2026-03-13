import { access } from 'node:fs/promises'

/** Returns true if the path exists on disk. */
export async function pathExists(filePath: string): Promise<boolean> {
  try {
    await access(filePath)
    return true
  } catch {
    return false
  }
}
