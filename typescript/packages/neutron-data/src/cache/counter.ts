/** Omission alone means no expiry; every supplied counter TTL is validated. */
export function counterTTL(ttlSec: number | undefined): number | undefined {
  if (ttlSec !== undefined && (!Number.isSafeInteger(ttlSec) || ttlSec < 1 || ttlSec > 2_147_483_647)) {
    throw new Error("counter TTL must be an integer between 1 and 2147483647 seconds, or omitted");
  }
  return ttlSec;
}

/** CacheClient numbers are exact safe integers, stored as canonical decimal text. */
export function nextCounter(raw: string): number {
  const value = Number(raw);
  if (!/^(0|-?[1-9]\d*)$/.test(raw) || !Number.isSafeInteger(value) || value >= Number.MAX_SAFE_INTEGER) {
    throw new Error("counter value must be a canonical safe integer with a safe successor");
  }
  return value + 1;
}

// Validate the old value within the same server command, before INCR or EXPIRE.
export const incrementCounterScript = `local raw = redis.call('GET', KEYS[1]);
if raw then
  local n = tonumber(raw);
  if (raw ~= '0' and not string.match(raw, '^%-?[1-9]%d*$')) or not n or n < -9007199254740991 or n >= 9007199254740991 then
    return redis.error_reply('counter value must be a canonical safe integer with a safe successor');
  end;
end;
local n = redis.call('INCR', KEYS[1]);
if ARGV[1] ~= '' and redis.call('TTL', KEYS[1]) == -1 then redis.call('EXPIRE', KEYS[1], ARGV[1]); end;
return n`;
