import { Link } from "neutron/client";
import type { ComponentChildren } from "preact";

export interface CardProps {
  title: string;
  description?: string;
  href?: string;
  icon?: ComponentChildren;
}

export default function Card(props: CardProps) {
  const content = (
    <>
      {props.icon && <div class="card-icon">{props.icon}</div>}
      <span class="card-title">{props.title}</span>
      {props.description && (
        <span class="card-description">{props.description}</span>
      )}
    </>
  );

  if (props.href) {
    return (
      <Link to={props.href} class="card card--link">
        {content}
      </Link>
    );
  }

  return <div class="card">{content}</div>;
}
