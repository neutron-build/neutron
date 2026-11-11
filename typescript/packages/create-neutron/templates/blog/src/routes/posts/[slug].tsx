import { notFound, type LoaderArgs } from "@neutron-build/core";
import { renderMarkdown } from "@neutron-build/cms";
import cms from "../../cms";

export const config = { mode: "app", hydrate: false };

interface Data {
  title: string;
  description: string;
  date: string;
  html: string;
}

export function head({ data }: { data?: Data }) {
  return data ? { title: data.title, description: data.description || undefined } : {};
}

export async function loader({ params }: LoaderArgs) {
  const post = cms.post(params.slug ?? "");
  if (!post) return notFound();
  return {
    title: post.title,
    description: post.description,
    date: post.published_at!.slice(0, 10),
    html: renderMarkdown(post.body),
  };
}

export default function Post(props: { data?: Data }) {
  if (!props.data) return null;
  const { title, date, html } = props.data;
  return (
    <article>
      <h1>{title}</h1>
      <time className="muted" dateTime={date}>{date}</time>
      <div className="body" dangerouslySetInnerHTML={{ __html: html }} />
    </article>
  );
}
