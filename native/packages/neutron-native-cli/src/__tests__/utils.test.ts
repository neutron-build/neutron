/**
 * Tests for CLI utility functions.
 */

import { pathExists } from '../utils'
import * as fs from 'node:fs/promises'
import * as path from 'node:path'
import * as os from 'node:os'

describe('pathExists', () => {
  let tmpDir: string

  beforeEach(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'neutron-cli-test-'))
  })

  afterEach(async () => {
    await fs.rm(tmpDir, { recursive: true, force: true })
  })

  it('returns true for an existing file', async () => {
    const filePath = path.join(tmpDir, 'exists.txt')
    await fs.writeFile(filePath, 'hello')
    expect(await pathExists(filePath)).toBe(true)
  })

  it('returns true for an existing directory', async () => {
    expect(await pathExists(tmpDir)).toBe(true)
  })

  it('returns false for a non-existent path', async () => {
    const missing = path.join(tmpDir, 'does-not-exist.txt')
    expect(await pathExists(missing)).toBe(false)
  })

  it('returns false for a path inside a non-existent directory', async () => {
    const deepPath = path.join(tmpDir, 'nonexistent', 'dir', 'file.txt')
    expect(await pathExists(deepPath)).toBe(false)
  })
})
