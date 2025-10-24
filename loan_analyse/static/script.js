async function sendPrompt() {
  const prompt = document.getElementById('prompt').value;
  const output = document.getElementById('output');
  output.textContent = "Processing your request...";

  try {
    const res = await fetch('/api/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ prompt })
    });

    const text = await res.text();
    console.log("Raw response:", text);

    let data;
    try {
      data = JSON.parse(text);
    } catch (e) {
      output.textContent = "❌ Server returned non-JSON:\n" + text;
      return;
    }

    if (data.error) {
      output.textContent = "❌ Error: " + data.error;
    } else {
      output.textContent =
        `✅ SQL Query:\n${data.sql_query}\n\n📊 Result:\n${JSON.stringify(data.result, null, 2)}`;
    }
  } catch (err) {
    output.textContent = "❌ Request failed: " + err;
  }
}
