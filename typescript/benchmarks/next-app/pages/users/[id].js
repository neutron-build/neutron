const users = {
  "1": { id: "1", name: "Alice", email: "alice@example.com" },
  "2": { id: "2", name: "Bob", email: "bob@example.com" },
  "3": { id: "3", name: "Charlie", email: "charlie@example.com" },
};

export async function getServerSideProps({ params }) {
  const user = users[params.id] || {
    id: params.id,
    name: "Unknown",
    email: "unknown@example.com",
  };
  return { props: { user } };
}

export default function UserPage({ user }) {
  return (
    <main>
      <h1>User: {user.name}</h1>
      <p>ID: {user.id}</p>
      <p>Email: {user.email}</p>
    </main>
  );
}
