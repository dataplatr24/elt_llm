const API_BASE = "/api";

export async function fetchFinanceData() {
  const res = await fetch(`${API_BASE}/finance-data`);
  return res.json();
}