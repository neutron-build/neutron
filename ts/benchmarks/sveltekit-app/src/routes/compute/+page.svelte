<script lang="ts">
  let { data } = $props();
</script>

<svelte:head>
  <title>Compute</title>
</svelte:head>

<main>
  <h1>Compute Result</h1>
  <p>LCG value: {data.result}</p>
</main>
