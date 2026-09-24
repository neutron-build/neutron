// Cron evaluation for the InMemory and Postgres queue drivers.
//
// Deliberately in-house: cron-parser pulls in luxon, which alone added ~52 KB gz
// to every self-contained server bundle that imports a queue driver. This
// covers the documented schedule() contract — five or six fields (a leading
// seconds field), `*`, `?`, lists, ranges, steps, month/weekday names and the
// @yearly/@monthly/@weekly/@daily/@hourly aliases — evaluated in local time.
// When both day-of-month and day-of-week are restricted, a day matching
// either fires (Vixie cron semantics, as cron-parser does).

export interface CronSchedule {
  /** First fire time strictly after `from`, on a whole second. */
  next(from: Date): Date;
}

const ALIASES: Record<string, string> = {
  "@yearly": "0 0 1 1 *",
  "@annually": "0 0 1 1 *",
  "@monthly": "0 0 1 * *",
  "@weekly": "0 0 * * 0",
  "@daily": "0 0 * * *",
  "@hourly": "0 * * * *",
};

const MONTHS = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];
const DAYS = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];

interface FieldSpec {
  name: string;
  min: number;
  max: number;
  names?: string[];
  namesFrom?: number;
}

const FIELDS: FieldSpec[] = [
  { name: "second", min: 0, max: 59 },
  { name: "minute", min: 0, max: 59 },
  { name: "hour", min: 0, max: 23 },
  { name: "day of month", min: 1, max: 31 },
  { name: "month", min: 1, max: 12, names: MONTHS, namesFrom: 1 },
  // 7 is accepted as Sunday and folded onto 0.
  { name: "day of week", min: 0, max: 7, names: DAYS, namesFrom: 0 },
];

const SECOND_MS = 1000;
const MINUTE_MS = 60 * SECOND_MS;
const HOUR_MS = 60 * MINUTE_MS;

// Eight years covers the longest gap between matches of any satisfiable
// pattern (Feb 29 across a skipped century leap year); longer never fires.
const SEARCH_YEARS = 8;

function invalid(pattern: string, reason: string): Error {
  return new Error(`Invalid cron pattern "${pattern}": ${reason}`);
}

function parseValue(token: string, spec: FieldSpec, pattern: string): number {
  if (/^\d+$/.test(token)) {
    return Number(token);
  }
  const index = spec.names?.indexOf(token.toLowerCase()) ?? -1;
  if (index === -1) {
    throw invalid(pattern, `unsupported ${spec.name} value "${token}"`);
  }
  return index + (spec.namesFrom ?? 0);
}

function parseField(field: string, spec: FieldSpec, pattern: string): boolean[] {
  const allowed = new Array<boolean>(spec.max + 1).fill(false);
  for (const part of field.split(",")) {
    const [rangePart, stepPart, extra] = part.split("/");
    if (extra !== undefined || rangePart === "") {
      throw invalid(pattern, `malformed ${spec.name} "${part}"`);
    }
    let step = 1;
    if (stepPart !== undefined) {
      if (!/^\d+$/.test(stepPart) || Number(stepPart) === 0) {
        throw invalid(pattern, `bad step in ${spec.name} "${part}"`);
      }
      step = Number(stepPart);
    }
    let low: number;
    let high: number;
    if (rangePart === "*" || rangePart === "?") {
      low = spec.min;
      high = spec.max;
    } else if (rangePart.includes("-")) {
      const bounds = rangePart.split("-");
      if (bounds.length !== 2) {
        throw invalid(pattern, `malformed ${spec.name} range "${rangePart}"`);
      }
      low = parseValue(bounds[0], spec, pattern);
      high = parseValue(bounds[1], spec, pattern);
    } else {
      low = parseValue(rangePart, spec, pattern);
      // `a/n` means from a to the field maximum, every n.
      high = stepPart === undefined ? low : spec.max;
    }
    if (low < spec.min || high > spec.max || low > high) {
      throw invalid(pattern, `${spec.name} "${part}" is outside ${spec.min}-${spec.max}`);
    }
    for (let value = low; value <= high; value += step) {
      allowed[value] = true;
    }
  }
  return allowed;
}

function values(allowed: boolean[]): number[] {
  const out: number[] = [];
  allowed.forEach((isAllowed, value) => {
    if (isAllowed) out.push(value);
  });
  return out;
}

function isFull(allowed: boolean[], min: number, max: number): boolean {
  for (let value = min; value <= max; value += 1) {
    if (!allowed[value]) {
      return false;
    }
  }
  return true;
}

export function parseCron(pattern: string): CronSchedule {
  const trimmed = pattern.trim();
  const expanded = ALIASES[trimmed.toLowerCase()] ?? trimmed;
  const fields = expanded.split(/\s+/);
  if (fields.length === 5) {
    fields.unshift("0");
  }
  if (fields.length !== 6) {
    throw invalid(pattern, "expected five or six fields");
  }
  const [seconds, minutes, hours, days, months, weekdays] = fields.map((field, i) =>
    parseField(field, FIELDS[i], pattern)
  );
  if (weekdays[7]) {
    weekdays[0] = true;
  }
  const anyDay = isFull(days, 1, 31);
  const anyWeekday = isFull(weekdays, 0, 6);

  const anyHour = isFull(hours, 0, 23);
  const hourList = values(hours);
  const minuteList = values(minutes);
  const secondList = values(seconds);

  const dayMatches = (date: Date): boolean => {
    if (!months[date.getMonth() + 1]) {
      return false;
    }
    const dom = days[date.getDate()];
    const dow = weekdays[date.getDay()];
    if (!anyDay && !anyWeekday) {
      return dom || dow;
    }
    return dom && dow;
  };

  // Every-hour patterns step through elapsed time, so a DST fall-back's
  // repeated hour fires again, like any other hour.
  const nextByElapsedTime = (after: number): Date => {
    const date = new Date(after + SECOND_MS);
    const limitYear = date.getFullYear() + SEARCH_YEARS;
    while (date.getFullYear() <= limitYear) {
      if (!dayMatches(date)) {
        date.setDate(date.getDate() + 1);
        date.setHours(0, 0, 0);
        continue;
      }
      if (!minutes[date.getMinutes()]) {
        date.setTime(date.getTime() - date.getSeconds() * SECOND_MS + MINUTE_MS);
        continue;
      }
      if (!seconds[date.getSeconds()]) {
        date.setTime(date.getTime() + SECOND_MS);
        continue;
      }
      return date;
    }
    throw invalid(pattern, "never matches a real date");
  };

  // Patterns naming specific hours are matched on the wall clock: a time a
  // DST fall-back repeats fires once, and a time a spring-forward skips fires
  // as the clock jumps (Vixie cron semantics).
  const nextByWallClock = (after: number): Date => {
    const start = new Date(after);
    const day = new Date(start.getFullYear(), start.getMonth(), start.getDate(), 12);
    const limitYear = day.getFullYear() + SEARCH_YEARS;
    for (let first = true; day.getFullYear() <= limitYear; first = false) {
      if (dayMatches(day)) {
        for (const hour of hourList) {
          // A spring-forward gap is at most two hours; earlier hours of the
          // starting day cannot map past `after`.
          if (first && hour + 2 < start.getHours()) continue;
          for (const minute of minuteList) {
            for (const second of secondList) {
              const candidate = new Date(
                day.getFullYear(),
                day.getMonth(),
                day.getDate(),
                hour,
                minute,
                second
              );
              if (candidate.getTime() > after) {
                return candidate;
              }
            }
          }
        }
      }
      day.setDate(day.getDate() + 1);
    }
    throw invalid(pattern, "never matches a real date");
  };

  return {
    next(from: Date): Date {
      const after = Math.floor(from.getTime() / SECOND_MS) * SECOND_MS;
      return anyHour ? nextByElapsedTime(after) : nextByWallClock(after);
    },
  };
}
