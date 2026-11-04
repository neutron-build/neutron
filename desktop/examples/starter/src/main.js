async function callApi() {
  try {
    const res = await fetch('neutron://localhost/api/hello');
    const data = await res.json();
    document.getElementById('result').textContent = JSON.stringify(data, null, 2);
  } catch (e) {
    document.getElementById('result').textContent = 'Error: ' + e.message;
  }
}

document.getElementById('call').addEventListener('click', callApi);
