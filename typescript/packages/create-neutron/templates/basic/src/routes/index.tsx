export const config = { mode: "static" };

export async function loader() {
  return {
    title: "__PROJECT_NAME__",
    generatedAt: new Date().toISOString(),
  };
}

export default function Home(props: { data?: { title: string; generatedAt: string } }) {
  return (
    <section>
      <h2>{props.data?.title}</h2>
      <p>
        Static route generated at <strong>{props.data?.generatedAt}</strong>.
      </p>
    </section>
  );
}
