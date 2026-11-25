const form = document.getElementById("form");
let schema;

function label(feature, code) {
  return schema.categorical[feature].options[code] || (code && code !== "missing" ? code : "brak");
}

function decisionBadge(decision) {
  return decision === "NIE_JESC"
    ? '<span class="badge bg-danger">nie jeść</span>'
    : '<span class="badge bg-success">prawdopodobnie jadalny</span>';
}

async function refresh() {
  const { totals, latest } = await (await fetch("/api/predictions")).json();
  document.getElementById("events").textContent = totals.events || 0;
  document.getElementById("flagged").textContent = totals.NIE_JESC || 0;
  document.getElementById("edible").textContent = totals.PRAWDOPODOBNIE_JADALNY || 0;
  document.getElementById("missed").textContent = totals.missed_poisonous || 0;
  document.getElementById("rows").innerHTML = latest.slice(0, 20).map(p => `<tr>
    <td>${p.cap_diameter}</td><td>${label("cap_color", p.cap_color)}</td><td>${label("habitat", p.habitat)}</td>
    <td>${p.p_poison}</td><td>${decisionBadge(p.decision)}</td><td>${{ p: "trujący", e: "jadalny" }[p.true_class] || "-"}</td></tr>`).join("");
  return latest;
}

function buildForm() {
  const numeric = Object.entries(schema.numeric).map(([name, text]) =>
    `<div class="col-md-4"><label class="form-label">${text}</label><input class="form-control" type="number" step="0.1" min="0" name="${name}" required></div>`);
  const categorical = Object.entries(schema.categorical).map(([name, f]) =>
    `<div class="col-md-3"><label class="form-label">${f.label}</label><select class="form-select" name="${name}"><option value="">brak danych</option>${
      Object.entries(f.options).map(([code, text]) => `<option value="${code}">${text}</option>`).join("")}</select></div>`);
  form.innerHTML = numeric.concat(categorical).join("") + '<div class="col-12"><button type="submit" class="btn btn-primary">Wyślij</button></div>';
}

function showResult(text, kind) {
  const result = document.getElementById("result");
  result.className = `alert alert-${kind}`;
  result.textContent = text;
}

form.addEventListener("submit", async (e) => {
  e.preventDefault();
  const body = Object.fromEntries([...new FormData(form)].map(([k, v]) => [k, v === "" ? null : v]));
  const res = await fetch("/api/observations", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  if (!res.ok) { showResult("Błąd: Kafka niedostępna.", "warning"); return; }
  const { event_id } = await res.json();
  showResult("Wysłano, czekam na wynik...", "secondary");
  for (let i = 0; i < 15; i++) {
    await new Promise(r => setTimeout(r, 2000));
    const p = (await refresh()).find(x => x.event_id === event_id);
    if (p) {
      if (p.decision === "NIE_JESC") showResult(`P(trujący) = ${p.p_poison}: nie jeść`, "danger");
      else showResult(`P(trujący) = ${p.p_poison}: prawdopodobnie jadalny`, "success");
      return;
    }
  }
  showResult("Brak odpowiedzi. Czy zadanie strumieniowe działa?", "warning");
});

(async () => {
  schema = await (await fetch("/api/schema")).json();
  buildForm();
  refresh();
  setInterval(refresh, 2000);
})();
