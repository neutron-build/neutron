export interface ReadingTimeResult {
  text: string;
  minutes: number;
  words: number;
}

export function calculateReadingTime(text: string, wordsPerMinute = 200): ReadingTimeResult {
  const stripped = text.replace(/<[^>]*>/g, "");
  const words = stripped.split(/\s+/).filter((w) => w.length > 0).length;
  const minutes = Math.max(1, Math.ceil(words / wordsPerMinute));

  return {
    text: `${minutes} min read`,
    minutes,
    words,
  };
}
