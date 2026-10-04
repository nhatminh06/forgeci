(() => {
  const root = document.querySelector("#captured-evidence");
  if (!root) return;

  const addField = (list, label, value, className = "") => {
    const row = document.createElement("div");
    const term = document.createElement("dt");
    const detail = document.createElement("dd");
    term.textContent = label;
    detail.textContent = value;
    if (className) detail.className = className;
    row.append(term, detail);
    list.append(row);
  };

  fetch("data/self-hosting.json")
    .then((response) => {
      if (!response.ok) throw new Error(`evidence request failed: ${response.status}`);
      return response.json();
    })
    .then((capture) => {
      root.replaceChildren();

      const register = document.createElement("dl");
      register.className = "capture-register";
      addField(register, "status", capture.status, "state-pass");
      addField(register, "commit", capture.forgeci_commit);
      addField(register, "run ID", capture.run_id);
      addField(register, "source SHA-256", capture.source_digest);
      addField(register, "runners", capture.runners.join(" + "));
      root.append(register);

      const tableWrap = document.createElement("div");
      tableWrap.className = "capture-table-wrap";
      tableWrap.tabIndex = 0;
      const table = document.createElement("table");
      table.innerHTML = "<thead><tr><th>Job</th><th>State</th><th>Runner</th></tr></thead>";
      const body = document.createElement("tbody");
      for (const job of capture.jobs) {
        const row = document.createElement("tr");
        for (const value of [job.name, job.state, job.runner]) {
          const cell = document.createElement("td");
          cell.textContent = value;
          row.append(cell);
        }
        body.append(row);
      }
      table.append(body);
      tableWrap.append(table);
      root.append(tableWrap);

      const assertions = document.createElement("ul");
      assertions.className = "capture-assertions";
      const values = [
        ["Artifact", `${capture.artifact.name} downloaded and verified`, capture.artifact.verified],
        ["Cache", `${capture.cache.key} reused across unchanged source`, capture.cache.reused],
        ["Logs", `${capture.logs.job} stdout and stderr markers retrieved`, capture.logs.verified],
        ["Failure", `${capture.failure.independent_pass} / ${capture.failure.intentional_fail} / ${capture.failure.blocked_dependent}`, false],
      ];
      for (const [label, value, verified] of values) {
        const item = document.createElement("li");
        const key = document.createElement("span");
        const text = document.createElement("strong");
        key.textContent = label;
        text.textContent = value;
        if (verified) text.className = "state-pass";
        item.append(key, text);
        assertions.append(item);
      }
      root.append(assertions);
    })
    .catch(() => {
      root.textContent = "Captured evidence could not be loaded. Open the committed evidence directory below.";
    });
})();
