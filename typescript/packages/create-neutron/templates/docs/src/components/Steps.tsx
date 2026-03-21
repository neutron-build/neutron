import type { ComponentChildren } from "preact";

export interface StepsProps {
  children: ComponentChildren;
}

export default function Steps(props: StepsProps) {
  return <ol class="steps">{props.children}</ol>;
}
