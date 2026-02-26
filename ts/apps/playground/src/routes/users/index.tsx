export const config = { mode: "app", cache: { maxAge: 30 } };

export function headers() {
  return {
    "Cache-Control": "public, max-age=30",
    Vary: "Accept",
  };
}

export default function UsersIndex() {
  const users = [
    { id: "1", name: "Alice" },
    { id: "2", name: "Bob" },
    { id: "3", name: "Charlie" },
  ];

  return (
    <div>
      <h1>Users</h1>
      <ul>
        {users.map((user) => (
          <li key={user.id}>
            <a href={`/users/${user.id}`}>{user.name}</a>
          </li>
        ))}
      </ul>
    </div>
  );
}
