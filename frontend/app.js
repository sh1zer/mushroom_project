const form = document.getElementById("form");
let schema;

function label(feature, code) {
  return schema.categorical[feature].options[code] || (code && code !== "missing" ? code : "brak");
}

function decisionCell(decision) {
  return decision === "NIE_JESC" ? '<td class="bad">nie jeść</td>' : '<td class="ok">prawdopodobnie jadalny</td>';
}

async function refresh() {
  const { totals, latest } = await (await fetch("/api/predictions")).json();
  document.getElementById("events").textContent = totals.events || 0;
  document.getElementById("flagged").textContent = totals.NIE_JESC || 0;
  document.getElementById("edible").textContent = totals.PRAWDOPODOBNIE_JADALNY || 0;
  document.getElementById("missed").textContent = totals.missed_poisonous || 0;
  document.getElementById("rows").innerHTML = latest.slice(0, 20).map(p => `<tr>
    <td>${p.cap_diameter}</td><td>${label("cap_color", p.cap_color)}</td><td>${label("habitat", p.habitat)}</td>
    <td>${p.p_poison}</td>${decisionCell(p.decision)}<td>${{ p: "trujący", e: "jadalny" }[p.true_class] || "-"}</td></tr>`).join("");
  return latest;
}

function buildForm() {
  const numeric = Object.entries(schema.numeric).map(([name, text]) =>
    `<label>${text}<input type="number" step="0.1" min="0" name="${name}" required></label>`);
  const categorical = Object.entries(schema.categorical).map(([name, f]) =>
    `<label>${f.label}<select name="${name}"><option value="">brak danych</option>${
      Object.entries(f.options).map(([code, text]) => `<option value="${code}">${text}</option>`).join("")}</select></label>`);
  form.innerHTML = numeric.concat(categorical).join("") + '<button type="submit">Wyślij do Kafki</button>';
}

form.addEventListener("submit", async (e) => {
  e.preventDefault();
  const body = Object.fromEntries([...new FormData(form)].map(([k, v]) => [k, v === "" ? null : v]));
  const result = document.getElementById("result");
  const res = await fetch("/api/observations", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  if (!res.ok) { result.textContent = "Błąd: Kafka niedostępna."; return; }
  const { event_id } = await res.json();
  result.textContent = "Wysłano, czekam na wynik ze Sparka...";
  for (let i = 0; i < 15; i++) {
    await new Promise(r => setTimeout(r, 2000));
    const p = (await refresh()).find(x => x.event_id === event_id);
    if (p) {
      result.textContent = `P(trujący) = ${p.p_poison}: ${p.decision === "NIE_JESC" ? "NIE JEŚĆ" : "prawdopodobnie jadalny (zawsze skonsultuj z grzyboznawcą)"}`;
      return;
    }
  }
  result.textContent = "Brak odpowiedzi. Czy zadanie strumieniowe działa?";
});

(async () => {
  schema = await (await fetch("/api/schema")).json();
  buildForm();
  refresh();
  setInterval(refresh, 2000);
})();
