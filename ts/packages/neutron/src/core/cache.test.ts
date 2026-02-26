import { describe, it, expect, vi, beforeEach } from 'vitest';
import { cache, clearCache, clearCacheByPrefix, resetRequestCache, revalidateTag, revalidateTags, getCacheTags, getCacheKeysByTag } from './cache.js';

describe('cache()', () => {
  beforeEach(() => {
    clearCache();
  });

  it('deduplicates identical calls', async () => {
    let callCount = 0;
    const fn = cache(async (id: string) => {
      callCount++;
      return `user-${id}`;
    }, { keyPrefix: 'user' });

    const result1 = await fn('123');
    const result2 = await fn('123');
    const result3 = await fn('123');

    expect(result1).toBe('user-123');
    expect(result2).toBe('user-123');
    expect(result3).toBe('user-123');
    expect(callCount).toBe(1); // Only called once!
  });

  it('treats different arguments as separate cache keys', async () => {
    let callCount = 0;
    const fn = cache(async (id: string) => {
      callCount++;
      return `user-${id}`;
    }, { keyPrefix: 'user' });

    const result1 = await fn('123');
    const result2 = await fn('456');

    expect(result1).toBe('user-123');
    expect(result2).toBe('user-456');
    expect(callCount).toBe(2); // Called twice for different args
  });

  it('returns same promise for concurrent calls', async () => {
    let callCount = 0;
    const fn = cache(async (id: string) => {
      callCount++;
      await new Promise(resolve => setTimeout(resolve, 10));
      return `user-${id}`;
    }, { keyPrefix: 'user' });

    // Fire 3 concurrent calls
    const [result1, result2, result3] = await Promise.all([
      fn('123'),
      fn('123'),
      fn('123'),
    ]);

    expect(result1).toBe('user-123');
    expect(result2).toBe('user-123');
    expect(result3).toBe('user-123');
    expect(callCount).toBe(1); // Only called once even with concurrent calls!
  });

  it('respects TTL and expires cache entries', async () => {
    vi.useFakeTimers();

    let callCount = 0;
    const fn = cache(async (id: string) => {
      callCount++;
      return `user-${id}`;
    }, { keyPrefix: 'user', ttl: 100 });

    await fn('123');
    expect(callCount).toBe(1);

    // Before TTL expires
    vi.advanceTimersByTime(50);
    await fn('123');
    expect(callCount).toBe(1); // Still cached

    // After TTL expires
    vi.advanceTimersByTime(60);
    await fn('123');
    expect(callCount).toBe(2); // Cache expired, called again

    vi.useRealTimers();
  });

  it('clears all cache entries', async () => {
    let callCount = 0;
    const fn = cache(async (id: string) => {
      callCount++;
      return `user-${id}`;
    }, { keyPrefix: 'user' });

    await fn('123');
    expect(callCount).toBe(1);

    clearCache();

    await fn('123');
    expect(callCount).toBe(2); // Cache cleared, called again
  });

  it('clears cache by prefix', async () => {
    let userCalls = 0;
    let postCalls = 0;

    const getUser = cache(async (id: string) => {
      userCalls++;
      return `user-${id}`;
    }, { keyPrefix: 'user' });

    const getPost = cache(async (id: string) => {
      postCalls++;
      return `post-${id}`;
    }, { keyPrefix: 'post' });

    await getUser('1');
    await getPost('1');
    expect(userCalls).toBe(1);
    expect(postCalls).toBe(1);

    // Clear only user cache
    clearCacheByPrefix('user');

    await getUser('1');  // Calls function again
    await getPost('1');  // Still cached

    expect(userCalls).toBe(2);
    expect(postCalls).toBe(1); // Unchanged
  });

  it('resets request cache', async () => {
    let callCount = 0;
    const fn = cache(async (id: string) => {
      callCount++;
      return `user-${id}`;
    }, { keyPrefix: 'user' });

    await fn('123');
    expect(callCount).toBe(1);

    resetRequestCache();

    await fn('123');
    expect(callCount).toBe(2); // Cache reset, called again
  });

  it('handles cache with complex argument types', async () => {
    let callCount = 0;
    const fn = cache(async (options: { id: string; type: string }) => {
      callCount++;
      return `${options.type}-${options.id}`;
    }, { keyPrefix: 'complex' });

    const result1 = await fn({ id: '123', type: 'user' });
    const result2 = await fn({ id: '123', type: 'user' });

    expect(result1).toBe('user-123');
    expect(result2).toBe('user-123');
    expect(callCount).toBe(1);
  });

  it('caches errors (deduplication includes failures)', async () => {
    let callCount = 0;
    const fn = cache(async (shouldFail: boolean) => {
      callCount++;
      if (shouldFail) {
        throw new Error('Test error');
      }
      return 'success';
    }, { keyPrefix: 'error-test' });

    // First call fails
    await expect(fn(true)).rejects.toThrow('Test error');
    expect(callCount).toBe(1);

    // Second call with same args returns cached error (same promise)
    await expect(fn(true)).rejects.toThrow('Test error');
    expect(callCount).toBe(1); // Not called again - error is cached

    // Different args - success call works
    const result = await fn(false);
    expect(result).toBe('success');
    expect(callCount).toBe(2);
  });
});

describe('Tag-based cache invalidation', () => {
  beforeEach(() => {
    clearCache();
  });

  it('associates cache entries with tags', async () => {
    const fn = cache(async (id: string) => {
      return `user-${id}`;
    }, {
      keyPrefix: 'user',
      tags: (id) => [`user:${id}`, 'users'],
    });

    await fn('123');

    const tags = getCacheTags();
    expect(tags).toContain('user:123');
    expect(tags).toContain('users');
  });

  it('invalidates cache entries by tag', async () => {
    let callCount = 0;
    const fn = cache(async (id: string) => {
      callCount++;
      return `user-${id}`;
    }, {
      keyPrefix: 'user',
      tags: (id) => [`user:${id}`, 'users'],
    });

    await fn('123');
    expect(callCount).toBe(1);

    // Cached
    await fn('123');
    expect(callCount).toBe(1);

    // Invalidate by tag
    revalidateTag('user:123');

    // Should call function again
    await fn('123');
    expect(callCount).toBe(2);
  });

  it('invalidates multiple cache entries with same tag', async () => {
    let userCallCount = 0;
    let postsCallCount = 0;

    const getUser = cache(async (id: string) => {
      userCallCount++;
      return `user-${id}`;
    }, {
      keyPrefix: 'user',
      tags: (id) => [`user:${id}`],
    });

    const getUserPosts = cache(async (userId: string) => {
      postsCallCount++;
      return `posts-${userId}`;
    }, {
      keyPrefix: 'posts',
      tags: (userId) => [`user:${userId}`, 'posts'],
    });

    await getUser('123');
    await getUserPosts('123');
    expect(userCallCount).toBe(1);
    expect(postsCallCount).toBe(1);

    // Cached
    await getUser('123');
    await getUserPosts('123');
    expect(userCallCount).toBe(1);
    expect(postsCallCount).toBe(1);

    // Invalidate both by common tag
    revalidateTag('user:123');

    // Both should call functions again
    await getUser('123');
    await getUserPosts('123');
    expect(userCallCount).toBe(2);
    expect(postsCallCount).toBe(2);
  });

  it('invalidates multiple tags at once', async () => {
    let callCount = 0;
    const fn = cache(async (id: string) => {
      callCount++;
      return `user-${id}`;
    }, {
      keyPrefix: 'user',
      tags: (id) => [`user:${id}`, `tenant:${id.slice(0, 1)}`, 'users'],
    });

    await fn('123');
    await fn('456');
    expect(callCount).toBe(2);

    // Invalidate multiple tags
    revalidateTags(['user:123', 'user:456']);

    // Both should be invalidated
    await fn('123');
    await fn('456');
    expect(callCount).toBe(4);
  });

  it('does not invalidate unrelated cache entries', async () => {
    let user1Calls = 0;
    let user2Calls = 0;

    const getUser1 = cache(async () => {
      user1Calls++;
      return 'user1';
    }, {
      keyPrefix: 'user1',
      tags: () => ['user:1'],
    });

    const getUser2 = cache(async () => {
      user2Calls++;
      return 'user2';
    }, {
      keyPrefix: 'user2',
      tags: () => ['user:2'],
    });

    await getUser1();
    await getUser2();
    expect(user1Calls).toBe(1);
    expect(user2Calls).toBe(1);

    // Invalidate only user:1
    revalidateTag('user:1');

    // user1 should be invalidated, user2 should still be cached
    await getUser1();
    await getUser2();
    expect(user1Calls).toBe(2);
    expect(user2Calls).toBe(1); // Still cached
  });

  it('getCacheKeysByTag returns keys for a tag', async () => {
    const fn = cache(async (id: string) => {
      return `user-${id}`;
    }, {
      keyPrefix: 'user',
      tags: (id) => [`user:${id}`, 'users'],
    });

    await fn('123');
    await fn('456');

    const usersKeys = getCacheKeysByTag('users');
    expect(usersKeys).toHaveLength(2);
    expect(usersKeys.some(k => k.includes('123'))).toBe(true);
    expect(usersKeys.some(k => k.includes('456'))).toBe(true);

    const user123Keys = getCacheKeysByTag('user:123');
    expect(user123Keys).toHaveLength(1);
    expect(user123Keys[0]).toContain('123');
  });

  it('works without tags (backward compatible)', async () => {
    let callCount = 0;
    const fn = cache(async (id: string) => {
      callCount++;
      return `user-${id}`;
    }, { keyPrefix: 'user', ttl: 60000 }); // No tags, long TTL

    await fn('123');
    expect(callCount).toBe(1);

    // Cached
    await fn('123');
    expect(callCount).toBe(1);

    // revalidateTag does nothing (no tags exist)
    revalidateTag('nonexistent-tag');

    // Still cached (revalidateTag with non-existent tag does nothing)
    await fn('123');
    expect(callCount).toBe(1);

    // clearCacheByPrefix still works
    clearCacheByPrefix('user');
    await fn('123');
    expect(callCount).toBe(2);
  });

  it('cleans up tag references when cache expires', async () => {
    vi.useFakeTimers();

    const fn = cache(async (id: string) => {
      return `user-${id}`;
    }, {
      keyPrefix: 'user',
      ttl: 100,
      tags: (id) => [`user:${id}`],
    });

    await fn('123');
    expect(getCacheTags()).toContain('user:123');

    // After TTL expires
    vi.advanceTimersByTime(110);

    // Tag should be cleaned up
    const tags = getCacheTags();
    expect(tags).not.toContain('user:123');

    vi.useRealTimers();
  });
});
