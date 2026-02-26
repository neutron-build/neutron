<script lang="ts">
  let { data } = $props();
</script>

<svelte:head>
  <title>User {data.name}</title>
</svelte:head>

<main>
  <h1>User: {data.name}</h1>
  <p>ID: {data.id}</p>
</main>
