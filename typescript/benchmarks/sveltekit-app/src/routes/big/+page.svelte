<script lang="ts">
  let { data } = $props();
</script>

<svelte:head>
  <title>Big Table</title>
</svelte:head>

<main>
  <h1>Big Table ({data.rows.length} rows)</h1>
  <table>
    <thead>
      <tr>
        <th>ID</th>
        <th>Name</th>
        <th>Value</th>
        <th>Active</th>
      </tr>
    </thead>
    <tbody>
      {#each data.rows as row (row.id)}
        <tr>
          <td>{row.id}</td>
          <td>{row.name}</td>
          <td>{row.value}</td>
          <td>{row.active ? "Yes" : "No"}</td>
        </tr>
      {/each}
    </tbody>
  </table>
</main>
