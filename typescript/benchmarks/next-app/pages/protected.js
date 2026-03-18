export async function getServerSideProps({ req, res }) {
  const authorized = req.headers.authorization === "Bearer valid-token";
  if (!authorized) {
    res.statusCode = 401;
  }
  return {
    props: {
      authorized,
    },
  };
}

export default function ProtectedPage({ authorized }) {
  return (
    <main>
      <h1>bench-protected</h1>
      <p>{authorized ? "authorized" : "unauthorized"}</p>
    </main>
  );
}
