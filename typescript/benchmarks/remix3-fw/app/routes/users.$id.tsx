import { useParams } from "react-router";

const users = {
  "1": { id: "1", name: "Alice", email: "alice@example.com" },
  "2": { id: "2", name: "Bob", email: "bob@example.com" },
  "3": { id: "3", name: "Charlie", email: "charlie@example.com" },
};

export default function UserPage() {
  const params = useParams();
  const id = params.id || "";
  const user = users[id] || { id, name: "Unknown", email: "unknown@example.com" };
  return (
    <main>
      <h1>User: {user.name}</h1>
      <p>ID: {user.id}</p>
      <p>Email: {user.email}</p>
    </main>
  );
}
